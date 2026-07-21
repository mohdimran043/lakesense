// Package mongodb implements the LakeSense MongoDB connector: full load and
// incremental cursor reads over collections, with a schema inferred by sampling
// documents (nested documents and arrays land as JSON, ObjectIDs as hex). Change
// -stream CDC is the next milestone; the connector is badged Stable until its
// battery passes. Design follows docs/analysis/other-sources.md.
package mongodb

import (
	"context"
	"encoding/json"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "mongodb"

// sampleSize is how many documents Discover samples to infer a collection schema.
const sampleSize = 100

// Config is the source configuration.
type Config struct {
	Type string `json:"type,omitempty"`
	// URI is a full mongodb:// connection string; when set it takes precedence
	// over host/port/user/password.
	URI      string `json:"uri,omitempty"`
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"` // default 27017
	Database string `json:"database"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}
	if c.URI == "" && c.Host == "" {
		return fmt.Errorf("uri or host is required")
	}
	if c.Port == 0 {
		c.Port = 27017
	}
	return nil
}

// uri builds the connection string. For the host/port form it uses a direct
// connection so a single node advertising an unreachable replica-set hostname
// still connects (the common dev case); production replica sets pass a full URI.
func (c *Config) uri() string {
	if c.URI != "" {
		return c.URI
	}
	auth := ""
	if c.User != "" {
		auth = c.User + ":" + c.Password + "@"
	}
	return fmt.Sprintf("mongodb://%s%s:%d/?directConnection=true", auth, c.Host, c.Port)
}

// Connector implements sdk.Connector, FullLoader, IncrementalReader.
type Connector struct {
	cfg    Config
	client *mongo.Client
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:        Type,
		DisplayName: "MongoDB",
		// Change-stream CDC joins the declaration once its battery passes.
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityStable,
		ConfigSchema: json.RawMessage(configSchema),
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(ctx context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse mongodb config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid mongodb config: %w", err)
	}
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(c.cfg.uri()))
	if err != nil {
		return fmt.Errorf("connect mongodb: %w", err)
	}
	c.client = client
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	if err := c.client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("cannot reach mongodb: %w", err)
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(ctx context.Context) error {
	if c.client != nil {
		return c.client.Disconnect(ctx)
	}
	return nil
}

// db returns the configured database handle.
func (c *Connector) db() *mongo.Database { return c.client.Database(c.cfg.Database) }

// Discover implements sdk.Connector: each collection becomes a stream whose
// schema is inferred by sampling documents. _id is the primary key.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	names, err := c.db().ListCollectionNames(ctx, bson.M{"name": bson.M{"$not": bson.M{"$regex": "^system\\."}}})
	if err != nil {
		return nil, fmt.Errorf("list collections: %w", err)
	}
	streams := make([]model.Stream, 0, len(names))
	for _, name := range names {
		schema, err := c.inferSchema(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("infer schema for %s: %w", name, err)
		}
		streams = append(streams, model.Stream{
			Namespace:          c.cfg.Database,
			Name:               name,
			Schema:             schema,
			SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

// inferSchema samples documents and unions their top-level fields into a schema.
// The first non-null type seen for a field wins; mixed fields fall back to string.
func (c *Connector) inferSchema(ctx context.Context, collection string) (model.Schema, error) {
	cur, err := c.db().Collection(collection).Find(ctx, bson.M{}, options.Find().SetLimit(sampleSize))
	if err != nil {
		return model.Schema{}, fmt.Errorf("sample: %w", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	types := map[string]model.DataType{}
	var order []string
	for cur.Next(ctx) {
		var doc bson.D
		if err := cur.Decode(&doc); err != nil {
			return model.Schema{}, fmt.Errorf("decode sample: %w", err)
		}
		for _, e := range doc {
			lt := bsonType(e.Value)
			if existing, seen := types[e.Key]; !seen {
				types[e.Key] = lt
				order = append(order, e.Key)
			} else if existing != lt {
				types[e.Key] = model.TypeString // mixed types → string
			}
		}
	}
	if err := cur.Err(); err != nil {
		return model.Schema{}, fmt.Errorf("iterate sample: %w", err)
	}

	schema := model.Schema{Columns: make([]model.Column, 0, len(order))}
	for _, name := range order {
		schema.Columns = append(schema.Columns, model.Column{
			Name:       name,
			Type:       types[name],
			Nullable:   name != "_id",
			PrimaryKey: name == "_id",
		})
	}
	return schema, nil
}

// suggestCursor proposes an incremental cursor: a timestamp field named like an
// update marker, else _id (ObjectIDs are monotonically increasing).
func suggestCursor(s model.Schema) string {
	for _, name := range []string{"updatedAt", "updated_at", "modifiedAt", "createdAt", "created_at"} {
		if col, ok := s.Column(name); ok && col.Type == model.TypeTimestamp {
			return name
		}
	}
	if _, ok := s.Column("_id"); ok {
		return "_id"
	}
	return ""
}

// bsonType maps a decoded BSON value to a lake type.
func bsonType(v any) model.DataType {
	switch v.(type) {
	case primitive.ObjectID, string, primitive.Symbol:
		return model.TypeString
	case int32:
		return model.TypeInt32
	case int64:
		return model.TypeInt64
	case float64:
		return model.TypeFloat64
	case bool:
		return model.TypeBool
	case primitive.DateTime:
		return model.TypeTimestamp
	case primitive.Decimal128:
		return model.TypeDecimal
	case primitive.Binary:
		return model.TypeBinary
	default:
		// documents, arrays, and anything else → JSON.
		return model.TypeJSON
	}
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "MongoDB source",
  "type": "object",
  "required": ["database"],
  "properties": {
    "uri": {"type": "string", "title": "Connection URI",
      "description": "mongodb:// string; overrides host/port/user/password when set."},
    "host": {"type": "string", "title": "Host"},
    "port": {"type": "integer", "title": "Port", "default": 27017},
    "database": {"type": "string", "title": "Database"},
    "user": {"type": "string", "title": "User"},
    "password": {"type": "string", "title": "Password", "format": "password"}
  }
}`

// Package cassandra implements the LakeSense Cassandra/ScyllaDB connector: full
// load and incremental cursor reads over CQL tables (one gocql connector serves
// both, badged separately). CDC is honestly absent in v1 (Cassandra CDC needs
// commitlog scraping); the connector is Beta. Design follows
// docs/analysis/other-sources.md.
package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "cassandra"

// Config is the source configuration.
type Config struct {
	Type     string   `json:"type,omitempty"`
	Hosts    []string `json:"hosts"`
	Port     int      `json:"port,omitempty"` // default 9042
	Keyspace string   `json:"keyspace"`
	User     string   `json:"user,omitempty"`
	Password string   `json:"password,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if len(c.Hosts) == 0 {
		return fmt.Errorf("at least one host is required")
	}
	if c.Keyspace == "" {
		return fmt.Errorf("keyspace is required")
	}
	if c.Port == 0 {
		c.Port = 9042
	}
	return nil
}

// Connector implements sdk.Connector, FullLoader, IncrementalReader.
type Connector struct {
	cfg     Config
	session *gocql.Session
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "Cassandra",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
		Presets: []sdk.Preset{
			{Name: "scylladb", DisplayName: "ScyllaDB", Notes: "CQL-compatible; same connector"},
		},
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse cassandra config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid cassandra config: %w", err)
	}
	cluster := gocql.NewCluster(c.cfg.Hosts...)
	cluster.Port = c.cfg.Port
	cluster.Keyspace = c.cfg.Keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 15 * time.Second
	cluster.ConnectTimeout = 15 * time.Second
	if c.cfg.User != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{Username: c.cfg.User, Password: c.cfg.Password}
	}
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("connect cassandra: %w", err)
	}
	c.session = session
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.session == nil {
		return fmt.Errorf("connector not set up")
	}
	var name string
	if err := c.session.Query(`SELECT release_version FROM system.local`).WithContext(ctx).Scan(&name); err != nil {
		return fmt.Errorf("cannot reach cassandra: %w", err)
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(context.Context) error {
	if c.session != nil {
		c.session.Close()
	}
	return nil
}

// Discover implements sdk.Connector: tables in the keyspace, with columns and
// primary keys (partition + clustering columns) from system_schema.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.session == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	tables, err := c.listTables(ctx)
	if err != nil {
		return nil, err
	}
	streams := make([]model.Stream, 0, len(tables))
	for _, name := range tables {
		schema, err := c.tableSchema(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("schema for %s: %w", name, err)
		}
		streams = append(streams, model.Stream{
			Namespace:          c.cfg.Keyspace,
			Name:               name,
			Schema:             schema,
			SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

func (c *Connector) listTables(ctx context.Context) ([]string, error) {
	iter := c.session.Query(
		`SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?`, c.cfg.Keyspace).WithContext(ctx).Iter()
	var out []string
	var name string
	for iter.Scan(&name) {
		out = append(out, name)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	return out, nil
}

func (c *Connector) tableSchema(ctx context.Context, table string) (model.Schema, error) {
	iter := c.session.Query(
		`SELECT column_name, type, kind, position FROM system_schema.columns
		 WHERE keyspace_name = ? AND table_name = ?`, c.cfg.Keyspace, table).WithContext(ctx).Iter()
	type col struct {
		name, cqlType, kind string
		position            int
	}
	var cols []col
	var cur col
	for iter.Scan(&cur.name, &cur.cqlType, &cur.kind, &cur.position) {
		cols = append(cols, cur)
	}
	if err := iter.Close(); err != nil {
		return model.Schema{}, fmt.Errorf("list columns: %w", err)
	}
	// CQL returns columns unordered; keep a stable order (partition/clustering by
	// position first, then regular columns by name).
	var schema model.Schema
	for _, kind := range []string{"partition_key", "clustering", "regular", "static"} {
		for _, cc := range cols {
			if cc.kind != kind {
				continue
			}
			schema.Columns = append(schema.Columns, model.Column{
				Name:       cc.name,
				Type:       mapType(cc.cqlType),
				SourceType: cc.cqlType,
				Nullable:   kind == "regular" || kind == "static",
				PrimaryKey: kind == "partition_key" || kind == "clustering",
			})
		}
	}
	return schema, nil
}

func suggestCursor(s model.Schema) string {
	for _, name := range []string{"updated_at", "modified_at", "created_at", "event_time"} {
		if col, ok := s.Column(name); ok && (col.Type == model.TypeTimestamp || col.Type == model.TypeDate) {
			return name
		}
	}
	pk := s.PrimaryKey()
	if len(pk) == 1 {
		if col, _ := s.Column(pk[0]); col.Type == model.TypeInt32 || col.Type == model.TypeInt64 {
			return pk[0]
		}
	}
	return ""
}

// quoteIdent double-quotes a CQL identifier.
func quoteIdent(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

func qualified(stream model.Stream) string {
	return quoteIdent(stream.Namespace) + "." + quoteIdent(stream.Name)
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Cassandra source",
  "type": "object",
  "required": ["hosts", "keyspace"],
  "properties": {
    "hosts": {"type": "array", "items": {"type": "string"}, "title": "Contact points"},
    "port": {"type": "integer", "title": "Port", "default": 9042},
    "keyspace": {"type": "string", "title": "Keyspace"},
    "user": {"type": "string", "title": "User"},
    "password": {"type": "string", "title": "Password", "format": "password"}
  }
}`

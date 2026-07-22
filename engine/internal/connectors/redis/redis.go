// Package redis implements the LakeSense Redis connector: a full-load keyspace
// snapshot. Redis is a schemaless key/value store, so a single stream exposes a
// fixed shape — key, type, value (JSON per value type), ttl — over every key
// matching a pattern in one logical database. There is no incremental cursor
// (keys carry no modification time) and no honest CDC (keyspace notifications
// are ephemeral pub/sub, not replayable), so the connector is full-load only and
// Beta. SCAN is not a point-in-time snapshot: keys mutated mid-scan may be missed
// or seen twice — inherent to Redis. Design follows docs/analysis/other-sources.md.
package redis

import (
	"context"
	"encoding/json"
	"fmt"

	goredis "github.com/redis/go-redis/v9"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "redis"

// Column names of the fixed keyspace schema.
const (
	colKey   = "key"
	colType  = "type"
	colValue = "value"
	colTTL   = "ttl"
)

// Config is the source configuration.
type Config struct {
	Type     string `json:"type,omitempty"`
	Address  string `json:"address"` // host:port, e.g. localhost:6379
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	DB       int    `json:"db,omitempty"` // logical database (default 0)
	// Pattern is the SCAN MATCH glob selecting keys (default "*").
	Pattern string `json:"pattern,omitempty"`
	// ScanCount is the SCAN COUNT hint per round trip (default 500).
	ScanCount int64 `json:"scan_count,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if c.Address == "" {
		return fmt.Errorf("address is required")
	}
	if c.DB < 0 {
		return fmt.Errorf("db must be >= 0")
	}
	if c.Pattern == "" {
		c.Pattern = "*"
	}
	if c.ScanCount <= 0 {
		c.ScanCount = 500
	}
	return nil
}

// Connector implements sdk.Connector and sdk.FullLoader.
type Connector struct {
	cfg    Config
	client *goredis.Client
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "Redis",
		Capabilities: []sdk.Capability{sdk.CapFullLoad},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
		Presets: []sdk.Preset{
			{Name: "valkey", DisplayName: "Valkey", Notes: "Redis-protocol compatible; same connector"},
		},
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse redis config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid redis config: %w", err)
	}
	c.client = goredis.NewClient(&goredis.Options{
		Addr:     c.cfg.Address,
		Username: c.cfg.Username,
		Password: c.cfg.Password,
		DB:       c.cfg.DB,
	})
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.client == nil {
		return fmt.Errorf("connector not set up")
	}
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("cannot reach redis: %w", err)
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(context.Context) error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Discover implements sdk.Connector: one stream with the fixed keyspace schema.
// The stream is named for the logical database; Redis has no per-key schema to
// derive, so columns are fixed.
func (c *Connector) Discover(_ context.Context) ([]model.Stream, error) {
	if c.client == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	schema := model.Schema{Columns: []model.Column{
		{Name: colKey, Type: model.TypeString, Nullable: false, PrimaryKey: true},
		{Name: colType, Type: model.TypeString, Nullable: false},
		{Name: colValue, Type: model.TypeJSON, Nullable: true},
		{Name: colTTL, Type: model.TypeInt64, Nullable: false, SourceType: "seconds"},
	}}
	return []model.Stream{{
		Namespace:          fmt.Sprintf("db%d", c.cfg.DB),
		Name:               "keys",
		Schema:             schema,
		SupportedSyncModes: []model.SyncMode{model.ModeFullLoad},
	}}, nil
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Redis source",
  "type": "object",
  "required": ["address"],
  "properties": {
    "address": {"type": "string", "title": "Address", "description": "host:port, e.g. localhost:6379"},
    "username": {"type": "string", "title": "Username"},
    "password": {"type": "string", "title": "Password", "format": "password"},
    "db": {"type": "integer", "title": "Database", "default": 0},
    "pattern": {"type": "string", "title": "Key pattern", "default": "*", "description": "SCAN MATCH glob"},
    "scan_count": {"type": "integer", "title": "Scan count", "default": 500}
  }
}`

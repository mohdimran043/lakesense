// Package clickhouse implements the LakeSense ClickHouse connector: full load
// and incremental cursor reads over an analytical column store. ClickHouse has
// no enforced unique key and no logical-replication CDC, so CDC is honestly
// absent; the connector is Beta. Design follows docs/analysis/other-sources.md.
package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2" // registers the "clickhouse" driver

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "clickhouse"

// Config is the source configuration.
type Config struct {
	Type     string `json:"type,omitempty"`
	Host     string `json:"host"`
	Port     int    `json:"port,omitempty"` // native protocol, default 9000
	Database string `json:"database"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}
	if c.Port == 0 {
		c.Port = 9000
	}
	if c.User == "" {
		c.User = "default"
	}
	return nil
}

func (c *Config) dsn() string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s", c.User, c.Password, c.Host, c.Port, c.Database)
}

// Connector implements sdk.Connector, FullLoader, IncrementalReader.
type Connector struct {
	cfg Config
	db  *sql.DB
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "ClickHouse",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse clickhouse config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid clickhouse config: %w", err)
	}
	db, err := sql.Open("clickhouse", c.cfg.dsn())
	if err != nil {
		return fmt.Errorf("open clickhouse: %w", err)
	}
	db.SetMaxOpenConns(4)
	c.db = db
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("connector not set up")
	}
	if err := c.db.PingContext(ctx); err != nil {
		return fmt.Errorf("cannot reach %s:%d/%s: %w", c.cfg.Host, c.cfg.Port, c.cfg.Database, err)
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(context.Context) error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Discover implements sdk.Connector: tables in the configured database (excluding
// views), with columns and a suggested cursor. Sorting-key columns are marked as
// the primary key for record identity (ClickHouse keys are not unique, but they
// are the closest identity signal).
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.db == nil {
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
			Namespace:          c.cfg.Database,
			Name:               name,
			Schema:             schema,
			SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

func (c *Connector) listTables(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT name FROM system.tables WHERE database = ? AND engine NOT LIKE '%View%' ORDER BY name`, c.cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func (c *Connector) tableSchema(ctx context.Context, table string) (model.Schema, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT name, type, is_in_primary_key FROM system.columns
		 WHERE database = ? AND table = ? ORDER BY position`, c.cfg.Database, table)
	if err != nil {
		return model.Schema{}, fmt.Errorf("list columns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var schema model.Schema
	for rows.Next() {
		var name, chType string
		var inPK uint8
		if err := rows.Scan(&name, &chType, &inPK); err != nil {
			return model.Schema{}, fmt.Errorf("scan column: %w", err)
		}
		schema.Columns = append(schema.Columns, model.Column{
			Name:       name,
			Type:       mapType(chType),
			SourceType: chType,
			Nullable:   strings.HasPrefix(chType, "Nullable("),
			PrimaryKey: inPK == 1,
		})
	}
	return schema, rows.Err()
}

func suggestCursor(s model.Schema) string {
	for _, name := range []string{"updated_at", "modified_at", "event_time", "created_at", "timestamp"} {
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

// quoteIdent backtick-quotes a ClickHouse identifier.
func quoteIdent(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

func qualified(stream model.Stream) string {
	return quoteIdent(stream.Namespace) + "." + quoteIdent(stream.Name)
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ClickHouse source",
  "type": "object",
  "required": ["host", "database"],
  "properties": {
    "host": {"type": "string", "title": "Host"},
    "port": {"type": "integer", "title": "Native port", "default": 9000},
    "database": {"type": "string", "title": "Database"},
    "user": {"type": "string", "title": "User", "default": "default"},
    "password": {"type": "string", "title": "Password", "format": "password"}
  }
}`

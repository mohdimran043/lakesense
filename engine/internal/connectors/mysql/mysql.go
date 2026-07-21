// Package mysql implements the LakeSense MySQL connector: parallel-resumable
// full load (keyset chunking over a single-column integer PK) and incremental
// cursor reads, verified against a real MySQL. It serves the MySQL wire family —
// MariaDB, Aurora-MySQL, Percona, TiDB, Vitess — through connection presets,
// each badged by the battery it passes. Row-based binlog CDC is the next
// milestone and joins the capability declaration once its battery passes (the
// connector is badged Stable until then). Design mirrors the Postgres exemplar
// (docs/analysis/mysql-connector.md).
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-sql-driver/mysql"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "mysql"

// Config is the source configuration.
type Config struct {
	Type     string `json:"type,omitempty"` // must equal "mysql" when present
	Host     string `json:"host"`
	Port     int    `json:"port,omitempty"` // default 3306
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	// TLS selects the go-sql-driver tls mode: "false" (default), "true",
	// "skip-verify", or "preferred".
	TLS string `json:"tls,omitempty"`
	// Preset names a wire-compatible variant (mariadb, percona, tidb, …).
	Preset string `json:"preset,omitempty"`
	// ChunkRows is the PK span per full-load chunk (default 50k).
	ChunkRows int64 `json:"chunk_rows,omitempty"`
	// ServerID is the replica id used for binlog CDC (default 1001). Must be
	// unique among replicas connected to the primary.
	ServerID uint32 `json:"server_id,omitempty"`
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
	if c.User == "" {
		return fmt.Errorf("user is required")
	}
	if c.Port == 0 {
		c.Port = 3306
	}
	if c.ChunkRows <= 0 {
		c.ChunkRows = 50_000
	}
	if c.ServerID == 0 {
		c.ServerID = 1001
	}
	if c.TLS == "" {
		c.TLS = "false"
	}
	return nil
}

// dsn builds a go-sql-driver DSN. parseTime keeps DATE/DATETIME as time.Time.
func (c *Config) dsn() string {
	cfg := mysql.NewConfig()
	cfg.User = c.User
	cfg.Passwd = c.Password
	cfg.Net = "tcp"
	cfg.Addr = fmt.Sprintf("%s:%d", c.Host, c.Port)
	cfg.DBName = c.Database
	cfg.ParseTime = true
	cfg.TLSConfig = c.TLS
	return cfg.FormatDSN()
}

// Connector implements sdk.Connector, FullLoader, IncrementalReader, ChangeStreamer.
type Connector struct {
	cfg Config
	db  *sql.DB
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:        Type,
		DisplayName: "MySQL",
		// CDC (binlog) joins the declaration once its battery passes (below).
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityStable,
		ConfigSchema: json.RawMessage(configSchema),
		Presets: []sdk.Preset{
			{Name: "mariadb", DisplayName: "MariaDB"},
			{Name: "aurora-mysql", DisplayName: "Amazon Aurora (MySQL)"},
			{Name: "percona", DisplayName: "Percona Server"},
			{Name: "tidb", DisplayName: "TiDB", Maturity: sdk.MaturityStable,
				Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
				Notes:        "MySQL binlog semantics differ; use incremental (TiCDC is the native CDC path)"},
			{Name: "vitess", DisplayName: "Vitess / PlanetScale", Maturity: sdk.MaturityStable,
				Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
				Notes:        "sharded; binlog access varies, use incremental (VStream is the native CDC path)"},
		},
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(ctx context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse mysql config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid mysql config: %w", err)
	}
	db, err := sql.Open("mysql", c.cfg.dsn())
	if err != nil {
		return fmt.Errorf("open mysql: %w", err)
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
	var version string
	if err := c.db.QueryRowContext(ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("query server version: %w", err)
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

// Discover implements sdk.Connector: base tables in the configured database,
// with columns, primary keys, and a suggested cursor.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	names, err := c.listTables(ctx)
	if err != nil {
		return nil, err
	}

	streams := make([]model.Stream, 0, len(names))
	for _, name := range names {
		schema, err := c.tableSchema(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("schema for %s: %w", name, err)
		}
		modes := []model.SyncMode{model.ModeFullLoad, model.ModeIncremental}
		streams = append(streams, model.Stream{
			Namespace:          c.cfg.Database,
			Name:               name,
			Schema:             schema,
			SupportedSyncModes: modes,
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

// listTables returns the base-table names in the configured database.
func (c *Connector) listTables(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT table_name FROM information_schema.tables
		 WHERE table_schema = ? AND table_type = 'BASE TABLE'
		 ORDER BY table_name`, c.cfg.Database)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("scan table name: %w", err)
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tables: %w", err)
	}
	return names, nil
}

// tableSchema reads a table's columns, types, nullability, and PK membership.
func (c *Connector) tableSchema(ctx context.Context, table string) (model.Schema, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT column_name, data_type, is_nullable, column_key
		 FROM information_schema.columns
		 WHERE table_schema = ? AND table_name = ?
		 ORDER BY ordinal_position`, c.cfg.Database, table)
	if err != nil {
		return model.Schema{}, fmt.Errorf("list columns: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var schema model.Schema
	for rows.Next() {
		var (
			name, dataType, isNullable, columnKey string
		)
		if err := rows.Scan(&name, &dataType, &isNullable, &columnKey); err != nil {
			return model.Schema{}, fmt.Errorf("scan column: %w", err)
		}
		schema.Columns = append(schema.Columns, model.Column{
			Name:       name,
			Type:       mapType(dataType),
			SourceType: dataType,
			Nullable:   isNullable == "YES",
			PrimaryKey: columnKey == "PRI",
		})
	}
	if err := rows.Err(); err != nil {
		return model.Schema{}, fmt.Errorf("iterate columns: %w", err)
	}
	return schema, nil
}

// suggestCursor proposes an incremental cursor: an update-marker timestamp
// column, else a single integer PK.
func suggestCursor(s model.Schema) string {
	for _, name := range []string{"updated_at", "modified_at", "last_modified", "created_at"} {
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

// quoteIdent backtick-quotes a MySQL identifier, doubling embedded backticks.
func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "MySQL source",
  "type": "object",
  "required": ["host", "database", "user", "password"],
  "properties": {
    "host": {"type": "string", "title": "Host"},
    "port": {"type": "integer", "title": "Port", "default": 3306},
    "database": {"type": "string", "title": "Database"},
    "user": {"type": "string", "title": "User"},
    "password": {"type": "string", "title": "Password", "format": "password"},
    "tls": {"type": "string", "title": "TLS", "default": "false",
      "enum": ["false", "true", "skip-verify", "preferred"]},
    "preset": {"type": "string", "title": "Variant preset",
      "enum": ["", "mariadb", "aurora-mysql", "percona", "tidb", "vitess"]},
    "chunk_rows": {"type": "integer", "title": "Rows per full-load chunk", "default": 50000},
    "server_id": {"type": "integer", "title": "Replica server id (CDC)", "default": 1001}
  }
}`

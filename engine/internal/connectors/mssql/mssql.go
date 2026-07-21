// Package mssql implements the LakeSense SQL Server connector: keyset-chunked
// full load over an integer primary key and incremental cursor reads. CDC via
// change-table polling is the next milestone; the connector is badged Stable
// until its battery passes. Design follows docs/analysis/other-sources.md.
package mssql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/microsoft/go-mssqldb" // registers the "sqlserver" driver

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "sqlserver"

// Config is the source configuration.
type Config struct {
	Type      string `json:"type,omitempty"`
	Host      string `json:"host"`
	Port      int    `json:"port,omitempty"` // default 1433
	Database  string `json:"database"`
	User      string `json:"user"`
	Password  string `json:"password"`
	Encrypt   string `json:"encrypt,omitempty"` // "disable" (default), "true", "false"
	ChunkRows int64  `json:"chunk_rows,omitempty"`
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
		c.Port = 1433
	}
	if c.ChunkRows <= 0 {
		c.ChunkRows = 50_000
	}
	if c.Encrypt == "" {
		c.Encrypt = "disable"
	}
	return nil
}

// dsn builds a sqlserver:// connection URL.
func (c *Config) dsn() string {
	q := url.Values{}
	q.Set("database", c.Database)
	q.Set("encrypt", c.Encrypt)
	u := url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(c.User, c.Password),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		RawQuery: q.Encode(),
	}
	return u.String()
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
		DisplayName:  "SQL Server",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityStable,
		ConfigSchema: json.RawMessage(configSchema),
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse sqlserver config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid sqlserver config: %w", err)
	}
	db, err := sql.Open("sqlserver", c.cfg.dsn())
	if err != nil {
		return fmt.Errorf("open sqlserver: %w", err)
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
	var v string
	if err := c.db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&v); err != nil {
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

// Discover implements sdk.Connector: user tables (excluding system schemas),
// with columns, primary keys, and a suggested cursor.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	tables, err := c.listTables(ctx)
	if err != nil {
		return nil, err
	}
	streams := make([]model.Stream, 0, len(tables))
	for _, t := range tables {
		schema, err := c.tableSchema(ctx, t.schema, t.name)
		if err != nil {
			return nil, fmt.Errorf("schema for %s.%s: %w", t.schema, t.name, err)
		}
		streams = append(streams, model.Stream{
			Namespace:          t.schema,
			Name:               t.name,
			Schema:             schema,
			SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

type tableRef struct{ schema, name string }

func (c *Connector) listTables(ctx context.Context) ([]tableRef, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT s.name, t.name FROM sys.tables t
		 JOIN sys.schemas s ON s.schema_id = t.schema_id
		 WHERE t.is_ms_shipped = 0 ORDER BY s.name, t.name`)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []tableRef
	for rows.Next() {
		var t tableRef
		if err := rows.Scan(&t.schema, &t.name); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// tableSchema reads a table's columns plus its primary-key membership.
func (c *Connector) tableSchema(ctx context.Context, schemaName, table string) (model.Schema, error) {
	pk, err := c.primaryKey(ctx, schemaName, table)
	if err != nil {
		return model.Schema{}, err
	}
	rows, err := c.db.QueryContext(ctx,
		`SELECT column_name, data_type, is_nullable
		 FROM information_schema.columns
		 WHERE table_schema = @p1 AND table_name = @p2
		 ORDER BY ordinal_position`, schemaName, table)
	if err != nil {
		return model.Schema{}, fmt.Errorf("list columns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var schema model.Schema
	for rows.Next() {
		var name, dataType, isNullable string
		if err := rows.Scan(&name, &dataType, &isNullable); err != nil {
			return model.Schema{}, fmt.Errorf("scan column: %w", err)
		}
		schema.Columns = append(schema.Columns, model.Column{
			Name:       name,
			Type:       mapType(dataType),
			SourceType: dataType,
			Nullable:   isNullable == "YES",
			PrimaryKey: pk[name],
		})
	}
	return schema, rows.Err()
}

// primaryKey returns the set of PK column names for a table.
func (c *Connector) primaryKey(ctx context.Context, schemaName, table string) (map[string]bool, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT kcu.column_name
		 FROM information_schema.table_constraints tc
		 JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		 WHERE tc.table_schema = @p1 AND tc.table_name = @p2 AND tc.constraint_type = 'PRIMARY KEY'`,
		schemaName, table)
	if err != nil {
		return nil, fmt.Errorf("primary key of %s.%s: %w", schemaName, table, err)
	}
	defer func() { _ = rows.Close() }()
	pk := map[string]bool{}
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan pk column: %w", err)
		}
		pk[col] = true
	}
	return pk, rows.Err()
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

// quoteIdent bracket-quotes a T-SQL identifier, doubling embedded ']'.
func quoteIdent(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}

// qualified returns the bracket-quoted [schema].[table] identifier.
func qualified(stream model.Stream) string {
	return quoteIdent(stream.Namespace) + "." + quoteIdent(stream.Name)
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "SQL Server source",
  "type": "object",
  "required": ["host", "database", "user", "password"],
  "properties": {
    "host": {"type": "string", "title": "Host"},
    "port": {"type": "integer", "title": "Port", "default": 1433},
    "database": {"type": "string", "title": "Database"},
    "user": {"type": "string", "title": "User"},
    "password": {"type": "string", "title": "Password", "format": "password"},
    "encrypt": {"type": "string", "title": "Encrypt", "default": "disable", "enum": ["disable", "true", "false"]},
    "chunk_rows": {"type": "integer", "title": "Rows per full-load chunk", "default": 50000}
  }
}`

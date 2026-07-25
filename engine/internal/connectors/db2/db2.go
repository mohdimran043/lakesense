//go:build db2

// Package db2 implements the LakeSense IBM Db2 connector: keyset-chunked full
// load over a single integer primary key plus incremental cursor reads. It uses
// github.com/ibmdb/go_ibm_db, which requires IBM's native CLI driver (clidriver)
// and CGO_ENABLED=1 — incompatible with the engine's default static/CGo-free
// distroless build. So the whole connector lives behind the `db2` build tag and
// is only compiled into the separate CGo engine image (engine/Dockerfile.db2).
// CDC is out of scope (Db2 change capture needs SQL replication / db2ReadLog);
// the connector is badged Beta. Design follows engine/internal/connectors/mssql
// and engine/internal/connectors/oracle.
package db2

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/ibmdb/go_ibm_db" // registers the "go_ibm_db" driver

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "db2"

// Config is the source configuration.
type Config struct {
	Type      string `json:"type,omitempty"`
	Host      string `json:"host"`
	Port      int    `json:"port,omitempty"` // default 50000
	Database  string `json:"database"`
	User      string `json:"user"`
	Password  string `json:"password"`
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
		c.Port = 50000
	}
	if c.ChunkRows <= 0 {
		c.ChunkRows = 50_000
	}
	return nil
}

// dsn builds the go_ibm_db keyword connection string.
func (c *Config) dsn() string {
	return fmt.Sprintf("HOSTNAME=%s;DATABASE=%s;PORT=%d;UID=%s;PWD=%s",
		c.Host, c.Database, c.Port, c.User, c.Password)
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
		DisplayName:  "IBM Db2",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse db2 config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid db2 config: %w", err)
	}
	db, err := sql.Open("go_ibm_db", c.cfg.dsn())
	if err != nil {
		return fmt.Errorf("open db2: %w", err)
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
	var v int
	if err := c.db.QueryRowContext(ctx, "SELECT 1 FROM SYSIBM.SYSDUMMY1").Scan(&v); err != nil {
		return fmt.Errorf("query SYSIBM.SYSDUMMY1: %w", err)
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

// systemSchemas are Db2 catalog/tooling schemas excluded from discovery.
var systemSchemas = []string{"SYSCAT", "SYSIBM", "SYSSTAT", "SYSTOOLS", "SYSPUBLIC", "NULLID"}

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

// listTables lists user tables (TYPE='T') outside the system schemas.
func (c *Connector) listTables(ctx context.Context) ([]tableRef, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT TRIM(TABSCHEMA), TABNAME FROM SYSCAT.TABLES
		 WHERE TYPE = 'T'
		   AND TABSCHEMA NOT IN ('SYSCAT','SYSIBM','SYSSTAT','SYSTOOLS','SYSPUBLIC','NULLID')
		   AND TABSCHEMA NOT LIKE 'SYS%'
		 ORDER BY TABSCHEMA, TABNAME`)
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
		`SELECT COLNAME, TYPENAME, NULLS FROM SYSCAT.COLUMNS
		 WHERE TABSCHEMA = ? AND TABNAME = ?
		 ORDER BY COLNO`, schemaName, table)
	if err != nil {
		return model.Schema{}, fmt.Errorf("list columns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var schema model.Schema
	for rows.Next() {
		var name, typeName, nulls string
		if err := rows.Scan(&name, &typeName, &nulls); err != nil {
			return model.Schema{}, fmt.Errorf("scan column: %w", err)
		}
		typeName = strings.TrimSpace(typeName)
		schema.Columns = append(schema.Columns, model.Column{
			Name:       name,
			Type:       mapType(typeName),
			SourceType: typeName,
			Nullable:   strings.TrimSpace(nulls) == "Y",
			PrimaryKey: pk[name],
		})
	}
	return schema, rows.Err()
}

// primaryKey returns the set of PK column names for a table.
func (c *Connector) primaryKey(ctx context.Context, schemaName, table string) (map[string]bool, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT k.COLNAME
		 FROM SYSCAT.TABCONST t
		 JOIN SYSCAT.KEYCOLUSE k
		   ON t.CONSTNAME = k.CONSTNAME AND t.TABSCHEMA = k.TABSCHEMA AND t.TABNAME = k.TABNAME
		 WHERE t.TYPE = 'P' AND t.TABSCHEMA = ? AND t.TABNAME = ?
		 ORDER BY k.COLSEQ`, schemaName, table)
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
// column, else a single integer PK. Db2 folds unquoted identifiers UPPERCASE.
func suggestCursor(s model.Schema) string {
	for _, name := range []string{"UPDATED_AT", "MODIFIED_AT", "LAST_MODIFIED", "CREATED_AT"} {
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

// quoteIdent double-quotes a Db2 identifier, doubling embedded '"'.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// qualified returns the double-quoted "schema"."table" identifier.
func qualified(stream model.Stream) string {
	return quoteIdent(stream.Namespace) + "." + quoteIdent(stream.Name)
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "IBM Db2 source",
  "type": "object",
  "required": ["host", "database", "user", "password"],
  "properties": {
    "host": {"type": "string", "title": "Host"},
    "port": {"type": "integer", "title": "Port", "default": 50000},
    "database": {"type": "string", "title": "Database"},
    "user": {"type": "string", "title": "User"},
    "password": {"type": "string", "title": "Password", "format": "password"},
    "chunk_rows": {"type": "integer", "title": "Rows per full-load chunk", "default": 50000}
  }
}`

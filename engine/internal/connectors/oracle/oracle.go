// Package oracle implements the LakeSense Oracle Database connector: keyset-
// chunked full load over a numeric primary key and incremental cursor reads.
// It uses the pure-Go driver github.com/sijms/go-ora/v2 (no Instant Client, no
// CGo) so the engine keeps building with CGO_ENABLED=0. CDC is out of scope for
// v1 (Oracle change capture needs LogMiner/XStream); the connector is badged
// Beta. Design follows engine/internal/connectors/mssql and
// docs/analysis/other-sources.md.
package oracle

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	go_ora "github.com/sijms/go-ora/v2" // registers the "oracle" driver

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "oracle"

// Config is the source configuration.
type Config struct {
	Type      string `json:"type,omitempty"`
	Host      string `json:"host"`
	Port      int    `json:"port,omitempty"` // default 1521
	Service   string `json:"service"`        // service name, e.g. FREEPDB1
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
	if c.Service == "" {
		return fmt.Errorf("service is required")
	}
	if c.User == "" {
		return fmt.Errorf("user is required")
	}
	if c.Port == 0 {
		c.Port = 1521
	}
	if c.ChunkRows <= 0 {
		c.ChunkRows = 50_000
	}
	return nil
}

// dsn builds an oracle:// connection URL via go-ora's helper.
func (c *Config) dsn() string {
	return go_ora.BuildUrl(c.Host, c.Port, c.Service, c.User, c.Password, nil)
}

// Connector implements sdk.Connector, FullLoader, IncrementalReader.
type Connector struct {
	cfg    Config
	schema string // connecting schema (owner), UPPERCASE — the discovery namespace
	db     *sql.DB
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "Oracle",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(_ context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse oracle config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid oracle config: %w", err)
	}
	db, err := sql.Open("oracle", c.cfg.dsn())
	if err != nil {
		return fmt.Errorf("open oracle: %w", err)
	}
	db.SetMaxOpenConns(4)
	c.db = db
	// Oracle folds unquoted identifiers to uppercase; the connecting user owns
	// the objects USER_TABLES exposes, so its uppercased name is the namespace.
	c.schema = strings.ToUpper(c.cfg.User)
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("connector not set up")
	}
	if err := c.db.PingContext(ctx); err != nil {
		return fmt.Errorf("cannot reach %s:%d/%s: %w", c.cfg.Host, c.cfg.Port, c.cfg.Service, err)
	}
	var v int
	if err := c.db.QueryRowContext(ctx, "SELECT 1 FROM DUAL").Scan(&v); err != nil {
		return fmt.Errorf("query DUAL: %w", err)
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

// Discover implements sdk.Connector: tables owned by the connecting schema,
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
		schema, err := c.tableSchema(ctx, t)
		if err != nil {
			return nil, fmt.Errorf("schema for %s.%s: %w", c.schema, t, err)
		}
		streams = append(streams, model.Stream{
			Namespace:          c.schema,
			Name:               t,
			Schema:             schema,
			SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

// listTables lists the connecting schema's own tables, skipping dropped tables
// held in the recycle bin.
func (c *Connector) listTables(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT table_name FROM user_tables
		 WHERE table_name NOT LIKE 'BIN$%' ORDER BY table_name`)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, fmt.Errorf("scan table: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// tableSchema reads a table's columns plus its primary-key membership.
func (c *Connector) tableSchema(ctx context.Context, table string) (model.Schema, error) {
	pk, err := c.primaryKey(ctx, table)
	if err != nil {
		return model.Schema{}, err
	}
	rows, err := c.db.QueryContext(ctx,
		`SELECT column_name, data_type, data_precision, data_scale, nullable
		 FROM user_tab_columns
		 WHERE table_name = :1
		 ORDER BY column_id`, table)
	if err != nil {
		return model.Schema{}, fmt.Errorf("list columns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var schema model.Schema
	for rows.Next() {
		var (
			name, dataType, nullable string
			precision, scale         sql.NullInt64
		)
		if err := rows.Scan(&name, &dataType, &precision, &scale, &nullable); err != nil {
			return model.Schema{}, fmt.Errorf("scan column: %w", err)
		}
		schema.Columns = append(schema.Columns, model.Column{
			Name:       name,
			Type:       mapType(dataType, precision, scale),
			SourceType: dataType,
			Nullable:   nullable == "Y",
			PrimaryKey: pk[name],
		})
	}
	return schema, rows.Err()
}

// primaryKey returns the set of PK column names for a table.
func (c *Connector) primaryKey(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT ucc.column_name
		 FROM user_constraints uc
		 JOIN user_cons_columns ucc ON uc.constraint_name = ucc.constraint_name
		 WHERE uc.constraint_type = 'P' AND uc.table_name = :1
		 ORDER BY ucc.position`, table)
	if err != nil {
		return nil, fmt.Errorf("primary key of %s: %w", table, err)
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
// column, else a single numeric PK. Oracle identifiers are UPPERCASE.
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

// quoteIdent double-quotes an Oracle identifier, doubling embedded '"'.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// qualified returns the double-quoted "schema"."table" identifier.
func qualified(stream model.Stream) string {
	return quoteIdent(stream.Namespace) + "." + quoteIdent(stream.Name)
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Oracle source",
  "type": "object",
  "required": ["host", "service", "user", "password"],
  "properties": {
    "host": {"type": "string", "title": "Host"},
    "port": {"type": "integer", "title": "Port", "default": 1521},
    "service": {"type": "string", "title": "Service name"},
    "user": {"type": "string", "title": "User"},
    "password": {"type": "string", "title": "Password", "format": "password"},
    "chunk_rows": {"type": "integer", "title": "Rows per full-load chunk", "default": 50000}
  }
}`

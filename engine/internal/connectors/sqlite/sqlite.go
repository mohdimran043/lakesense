// Package sqlite implements the LakeSense SQLite connector: a self-contained,
// server-less source (full load + incremental, no CDC). SQLite has no logical
// replication, so CDC is honestly absent from the capability declaration and
// the connector ships as Beta. Being file-based and dependency-light (pure-Go
// modernc.org/sqlite, no CGo), it is the engine's demo/canary source — the full
// engine→events→platform path runs with zero external services.
//
// Design mirrors the Postgres exemplar's contract (docs/analysis/engine-protocol.md
// §3): the connector implements only the narrow leaf operations; the syncrun
// orchestrator owns chunking, cursors, and resume.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	_ "modernc.org/sqlite" // registers the CGo-free "sqlite" driver

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "sqlite"

// defaultChunkRows is the rowid span per full-load chunk when unset.
const defaultChunkRows = 50_000

// Config is the source configuration.
type Config struct {
	Type string `json:"type,omitempty"` // must equal "sqlite" when present
	// Path is the SQLite database file. Opened read-only.
	Path string `json:"path"`
	// ChunkRows is the rowid span per full-load chunk (default 50k).
	ChunkRows int64 `json:"chunk_rows,omitempty"`
}

func (c *Config) validate() error {
	if c.Type != "" && c.Type != Type {
		return fmt.Errorf("config type %q is not %q", c.Type, Type)
	}
	if c.Path == "" {
		return fmt.Errorf("path is required")
	}
	if c.ChunkRows == 0 {
		c.ChunkRows = defaultChunkRows
	}
	if c.ChunkRows < 1 {
		return fmt.Errorf("chunk_rows must be positive, got %d", c.ChunkRows)
	}
	return nil
}

// dsn builds a read-only file URI so a sync can never mutate the source.
func (c *Config) dsn() string {
	q := url.Values{}
	q.Set("mode", "ro")
	q.Set("_pragma", "busy_timeout(5000)")
	return "file:" + c.Path + "?" + q.Encode()
}

// Connector implements sdk.Connector, sdk.FullLoader, sdk.IncrementalReader.
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
		DisplayName:  "SQLite",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
		Maturity:     sdk.MaturityBeta,
		ConfigSchema: json.RawMessage(configSchema),
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(ctx context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse sqlite config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid sqlite config: %w", err)
	}
	db, err := sql.Open("sqlite", c.cfg.dsn())
	if err != nil {
		return fmt.Errorf("open sqlite %s: %w", c.cfg.Path, err)
	}
	// A single connection keeps read-only snapshots simple and avoids
	// modernc's per-conn file handles multiplying against a read-only file.
	db.SetMaxOpenConns(1)
	c.db = db
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("connector not set up")
	}
	if err := c.db.PingContext(ctx); err != nil {
		return fmt.Errorf("cannot open %s: %w", c.cfg.Path, err)
	}
	var n int
	if err := c.db.QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE type='table'`).Scan(&n); err != nil {
		return fmt.Errorf("read schema of %s: %w", c.cfg.Path, err)
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

// Discover implements sdk.Connector: user tables (excluding sqlite internal
// and virtual-table shadow tables), each with columns, PK, and a suggested
// cursor. SQLite has a single namespace ("main").
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
		streams = append(streams, model.Stream{
			Namespace:          "main",
			Name:               name,
			Schema:             schema,
			SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

// listTables returns user table names (excluding SQLite internal tables).
func (c *Connector) listTables(ctx context.Context) ([]string, error) {
	rows, err := c.db.QueryContext(ctx,
		`SELECT name FROM sqlite_master
		 WHERE type='table' AND name NOT LIKE 'sqlite_%'
		 ORDER BY name`)
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

// tableSchema reads columns via PRAGMA table_info, mapping declared types to
// lake types by SQLite's type-affinity rules.
func (c *Connector) tableSchema(ctx context.Context, table string) (model.Schema, error) {
	// PRAGMA does not accept bound parameters; the identifier is quoted.
	q := fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(table))
	rows, err := c.db.QueryContext(ctx, q)
	if err != nil {
		return model.Schema{}, fmt.Errorf("table_info: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var schema model.Schema
	for rows.Next() {
		var (
			cid        int
			name       string
			declType   sql.NullString
			notNull    int
			dfltValue  sql.NullString
			pkPosition int
		)
		if err := rows.Scan(&cid, &name, &declType, &notNull, &dfltValue, &pkPosition); err != nil {
			return model.Schema{}, fmt.Errorf("scan column: %w", err)
		}
		schema.Columns = append(schema.Columns, model.Column{
			Name:       name,
			Type:       mapType(declType.String),
			SourceType: strings.ToUpper(strings.TrimSpace(declType.String)),
			Nullable:   notNull == 0 && pkPosition == 0,
			PrimaryKey: pkPosition > 0,
		})
	}
	if err := rows.Err(); err != nil {
		return model.Schema{}, fmt.Errorf("iterate columns: %w", err)
	}
	return schema, nil
}

// suggestCursor proposes an incremental cursor: a timestamp column named like
// an update marker, else a single integer PK.
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

// quoteIdent quotes a SQLite identifier by doubling embedded quotes.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "SQLite source",
  "type": "object",
  "required": ["path"],
  "properties": {
    "path": {"type": "string", "title": "Database file path",
      "description": "Path to the .db/.sqlite file. Opened read-only."},
    "chunk_rows": {"type": "integer", "title": "Rows per full-load chunk", "default": 50000}
  }
}`

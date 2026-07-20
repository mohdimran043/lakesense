// Package postgres implements the LakeSense Tier A PostgreSQL connector:
// full load (CTID or keyset chunking) and incremental reads. CDC via pgoutput
// is the next milestone and joins the capability declaration only when it
// passes its battery. Design follows docs/analysis/postgres-connector.md.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Type is the connector's registry name.
const Type = "postgres"

// Config is the source configuration.
type Config struct {
	Type     string `json:"type,omitempty"` // must equal "postgres" when present
	Host     string `json:"host"`
	Port     int    `json:"port,omitempty"` // default 5432
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	SSLMode  string `json:"sslmode,omitempty"` // default "prefer"
	// Preset names a wire-compatible variant (cockroachdb, timescaledb, …);
	// it tunes quirk handling and capability reporting, never the protocol.
	Preset string `json:"preset,omitempty"`
	// MaxConnections bounds the pool (default 4).
	MaxConnections int `json:"max_connections,omitempty"`
	// ChunkTargetMB is the approximate source data volume per full-load
	// chunk (default 512 MiB).
	ChunkTargetMB int `json:"chunk_target_mb,omitempty"`
	// ChunkStrategy selects "ctid" (default) or "keyset" (single-column
	// integer PK required) full-load chunking.
	ChunkStrategy string `json:"chunk_strategy,omitempty"`

	// CDCSlotName overrides the logical replication slot name
	// (default "lakesense_<database>").
	CDCSlotName string `json:"cdc_slot_name,omitempty"`
	// CDCPublicationName overrides the pgoutput publication name
	// (default "lakesense_pub").
	CDCPublicationName string `json:"cdc_publication_name,omitempty"`
	// CDCAutoCreate, when unset or true, lets the connector create the slot
	// and publication and add selected tables. Set false to require an
	// operator-managed slot/publication (nil means the default, true).
	CDCAutoCreate *bool `json:"cdc_auto_create,omitempty"`
	// CDCIdleTimeoutSeconds aborts a bounded CDC micro-batch if no traffic
	// arrives before the target LSN is reached (default 30).
	CDCIdleTimeoutSeconds int `json:"cdc_idle_timeout_seconds,omitempty"`
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
	switch c.ChunkStrategy {
	case "", "ctid", "keyset":
	default:
		return fmt.Errorf("chunk_strategy must be \"ctid\" or \"keyset\", got %q", c.ChunkStrategy)
	}
	if c.Port == 0 {
		c.Port = 5432
	}
	if c.SSLMode == "" {
		c.SSLMode = "prefer"
	}
	if c.MaxConnections <= 0 {
		c.MaxConnections = 4
	}
	if c.ChunkTargetMB <= 0 {
		c.ChunkTargetMB = 512
	}
	return nil
}

// dsn builds a connection URL with credentials escaped.
func (c *Config) dsn() string {
	u := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(c.User, c.Password),
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:   "/" + c.Database,
	}
	q := url.Values{}
	q.Set("sslmode", c.SSLMode)
	u.RawQuery = q.Encode()
	return u.String()
}

// Connector implements sdk.Connector, sdk.FullLoader, sdk.IncrementalReader.
type Connector struct {
	cfg  Config
	pool *pgxpool.Pool
}

// New returns an unconfigured connector (sdk.Factory).
func New() sdk.Connector { return &Connector{} }

// Spec implements sdk.Connector.
func (c *Connector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         Type,
		DisplayName:  "PostgreSQL",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental, sdk.CapCDC},
		Maturity:     sdk.MaturityCertified,
		ConfigSchema: json.RawMessage(configSchema),
		Presets: []sdk.Preset{
			{Name: "aurora-postgres", DisplayName: "Amazon Aurora (PostgreSQL)"},
			{Name: "alloydb", DisplayName: "Google AlloyDB"},
			{Name: "timescaledb", DisplayName: "TimescaleDB"},
			{Name: "cockroachdb", DisplayName: "CockroachDB", Maturity: sdk.MaturityStable,
				Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
				Notes:        "no pgoutput logical replication; CDC unavailable, incremental instead; CTID unsupported so keyset chunking is forced"},
			{Name: "yugabytedb", DisplayName: "YugabyteDB", Maturity: sdk.MaturityStable,
				Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental},
				Notes:        "CTID unsupported; keyset chunking forced"},
		},
	}
}

// Setup implements sdk.Connector.
func (c *Connector) Setup(ctx context.Context, rawConfig json.RawMessage) error {
	if err := json.Unmarshal(rawConfig, &c.cfg); err != nil {
		return fmt.Errorf("parse postgres config: %w", err)
	}
	if err := c.cfg.validate(); err != nil {
		return fmt.Errorf("invalid postgres config: %w", err)
	}
	poolCfg, err := pgxpool.ParseConfig(c.cfg.dsn())
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	poolCfg.MaxConns = int32(c.cfg.MaxConnections)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return fmt.Errorf("create pool: %w", err)
	}
	c.pool = pool
	return nil
}

// Check implements sdk.Connector.
func (c *Connector) Check(ctx context.Context) error {
	if c.pool == nil {
		return fmt.Errorf("connector not set up")
	}
	if err := c.pool.Ping(ctx); err != nil {
		return fmt.Errorf("cannot reach %s:%d/%s: %w", c.cfg.Host, c.cfg.Port, c.cfg.Database, err)
	}
	var version string
	if err := c.pool.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		return fmt.Errorf("query server version: %w", err)
	}
	return nil
}

// Close implements sdk.Connector.
func (c *Connector) Close(context.Context) error {
	if c.pool != nil {
		c.pool.Close()
	}
	return nil
}

// Discover implements sdk.Connector: ordinary, partitioned, materialized and
// foreign tables outside system schemas, with columns, PKs, and a suggested
// cursor column.
func (c *Connector) Discover(ctx context.Context) ([]model.Stream, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	const tablesSQL = `
SELECT n.nspname, c.relname, c.relkind
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p', 'm', 'f')
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg\_%'
  AND NOT c.relispartition
ORDER BY 1, 2`
	rows, err := c.pool.Query(ctx, tablesSQL)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	type table struct{ ns, name, kind string }
	var tables []table
	for rows.Next() {
		var t table
		if err := rows.Scan(&t.ns, &t.name, &t.kind); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan table row: %w", err)
		}
		tables = append(tables, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tables: %w", err)
	}

	var streams []model.Stream
	for _, t := range tables {
		schema, err := c.tableSchema(ctx, t.ns, t.name)
		if err != nil {
			return nil, fmt.Errorf("schema for %s.%s: %w", t.ns, t.name, err)
		}
		modes := []model.SyncMode{model.ModeFullLoad, model.ModeIncremental}
		streams = append(streams, model.Stream{
			Namespace:          t.ns,
			Name:               t.name,
			Schema:             schema,
			SupportedSyncModes: modes,
			DefaultCursorField: suggestCursor(schema),
		})
	}
	return streams, nil
}

func (c *Connector) tableSchema(ctx context.Context, ns, name string) (model.Schema, error) {
	const colsSQL = `
SELECT a.attname,
       pg_catalog.format_type(a.atttypid, a.atttypmod),
       NOT a.attnotnull,
       COALESCE((
           SELECT true FROM pg_catalog.pg_index i
           WHERE i.indrelid = a.attrelid AND i.indisprimary
             AND a.attnum = ANY (i.indkey)
       ), false)
FROM pg_catalog.pg_attribute a
WHERE a.attrelid = ($1 || '.' || $2)::regclass::oid
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`
	quotedNS := pgx.Identifier{ns}.Sanitize()
	quotedName := pgx.Identifier{name}.Sanitize()
	rows, err := c.pool.Query(ctx, colsSQL, quotedNS, quotedName)
	if err != nil {
		return model.Schema{}, fmt.Errorf("list columns: %w", err)
	}
	defer rows.Close()

	var schema model.Schema
	for rows.Next() {
		var col model.Column
		var sourceType string
		if err := rows.Scan(&col.Name, &sourceType, &col.Nullable, &col.PrimaryKey); err != nil {
			return model.Schema{}, fmt.Errorf("scan column: %w", err)
		}
		col.SourceType = sourceType
		col.Type = MapType(sourceType)
		schema.Columns = append(schema.Columns, col)
	}
	if err := rows.Err(); err != nil {
		return model.Schema{}, fmt.Errorf("iterate columns: %w", err)
	}
	return schema, nil
}

// suggestCursor proposes an incremental cursor column: a timestamp column
// named like an update marker, else a single integer PK.
func suggestCursor(s model.Schema) string {
	for _, name := range []string{"updated_at", "modified_at", "last_modified", "created_at"} {
		if col, ok := s.Column(name); ok && col.Type == model.TypeTimestamp {
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

// qualifiedTable returns the safely quoted schema.table identifier.
func qualifiedTable(stream model.Stream) string {
	return pgx.Identifier{stream.Namespace, stream.Name}.Sanitize()
}

const configSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "PostgreSQL source",
  "type": "object",
  "required": ["host", "database", "user", "password"],
  "properties": {
    "host": {"type": "string", "title": "Host"},
    "port": {"type": "integer", "title": "Port", "default": 5432},
    "database": {"type": "string", "title": "Database"},
    "user": {"type": "string", "title": "User"},
    "password": {"type": "string", "title": "Password", "format": "password"},
    "sslmode": {"type": "string", "title": "SSL mode", "default": "prefer",
      "enum": ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]},
    "preset": {"type": "string", "title": "Variant preset",
      "enum": ["", "aurora-postgres", "alloydb", "timescaledb", "cockroachdb", "yugabytedb"]},
    "max_connections": {"type": "integer", "title": "Max connections", "default": 4},
    "chunk_target_mb": {"type": "integer", "title": "Chunk target (MiB)", "default": 512},
    "chunk_strategy": {"type": "string", "enum": ["ctid", "keyset"], "default": "ctid"},
    "cdc_slot_name": {"type": "string", "title": "CDC replication slot",
      "description": "Logical replication slot name; defaults to lakesense_<database>."},
    "cdc_publication_name": {"type": "string", "title": "CDC publication", "default": "lakesense_pub"},
    "cdc_auto_create": {"type": "boolean", "title": "Auto-create slot & publication", "default": true,
      "description": "When true, LakeSense creates the slot/publication and adds selected tables. Set false to require operator-managed replication objects."},
    "cdc_idle_timeout_seconds": {"type": "integer", "title": "CDC idle timeout (s)", "default": 30}
  }
}`

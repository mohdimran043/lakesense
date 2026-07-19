// Package model defines the engine's source-agnostic data model: lake types,
// streams, schemas, and the two-layer catalog (what exists vs what the user
// selected). Shapes follow docs/analysis/engine-protocol.md §2.
package model

import "fmt"

// DataType is the engine's internal ("lake") type system. Connectors map
// source types into these; writers map these onto Parquet/Iceberg types.
type DataType string

const (
	TypeBool    DataType = "bool"
	TypeInt32   DataType = "int32"
	TypeInt64   DataType = "int64"
	TypeFloat32 DataType = "float32"
	TypeFloat64 DataType = "float64"
	// TypeDecimal preserves exact precision (backed by decimal128 in Parquet).
	// The reference project degrades decimals to float64; we deliberately do not.
	TypeDecimal   DataType = "decimal"
	TypeString    DataType = "string"
	TypeBinary    DataType = "binary"
	TypeDate      DataType = "date"
	TypeTimestamp DataType = "timestamp" // microsecond precision, UTC
	TypeJSON      DataType = "json"
	TypeArray     DataType = "array"
)

// SyncMode is how a stream is read.
type SyncMode string

const (
	ModeFullLoad    SyncMode = "full_load"
	ModeIncremental SyncMode = "incremental"
	ModeCDC         SyncMode = "cdc"
)

// Engine-owned metadata columns injected into every stream.
const (
	ColRecordID     = "_ls_id"            // hash of PK values; idempotency key for merges
	ColIngestedAt   = "_ls_ingested_at"   // engine ingestion timestamp
	ColOpType       = "_ls_op"            // r|c|i|u|d (read/create/insert-upsert/update/delete)
	ColCDCTimestamp = "_ls_cdc_timestamp" // source-side change timestamp (CDC streams)
)

// Column is one field of a stream schema.
type Column struct {
	Name       string   `json:"name"`
	Type       DataType `json:"type"`
	SourceType string   `json:"source_type,omitempty"` // native type name, e.g. "jsonb"
	Nullable   bool     `json:"nullable"`
	PrimaryKey bool     `json:"primary_key,omitempty"`
}

// Schema is an ordered set of columns.
type Schema struct {
	Columns []Column `json:"columns"`
}

// Column returns the column with the given name, if present.
func (s Schema) Column(name string) (Column, bool) {
	for _, c := range s.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// PrimaryKey returns the names of primary-key columns in schema order.
func (s Schema) PrimaryKey() []string {
	var pk []string
	for _, c := range s.Columns {
		if c.PrimaryKey {
			pk = append(pk, c.Name)
		}
	}
	return pk
}

// Stream is a discovered source table/collection/topic.
type Stream struct {
	Namespace          string     `json:"namespace"` // schema/database/prefix
	Name               string     `json:"name"`
	Schema             Schema     `json:"schema"`
	SupportedSyncModes []SyncMode `json:"supported_sync_modes"`
	// DefaultCursorField is the connector's suggested incremental cursor
	// (e.g. an updated-at column), empty when none is detectable.
	DefaultCursorField string `json:"default_cursor_field,omitempty"`
}

// ID returns the stream identity "namespace.name" used in catalogs and state.
func (s Stream) ID() string { return StreamID(s.Namespace, s.Name) }

// StreamID builds the canonical stream identity.
func StreamID(namespace, name string) string {
	return fmt.Sprintf("%s.%s", namespace, name)
}

// SelectedStream is a user's sync choice for one stream (catalog layer 2).
type SelectedStream struct {
	Namespace   string   `json:"namespace"`
	Name        string   `json:"name"`
	Mode        SyncMode `json:"mode"`
	CursorField string   `json:"cursor_field,omitempty"` // required for incremental
	// Columns restricts synced columns; empty means all. New columns follow
	// SyncNewColumns.
	Columns        []string `json:"columns,omitempty"`
	SyncNewColumns bool     `json:"sync_new_columns,omitempty"`
	// DestinationTable overrides the default destination table name.
	DestinationTable string `json:"destination_table,omitempty"`
}

// ID returns the stream identity this selection refers to.
func (s SelectedStream) ID() string { return StreamID(s.Namespace, s.Name) }

// Catalog is the discover output plus user selections: layer 1 records what
// exists at the source; layer 2 what the user chose to sync and how.
type Catalog struct {
	Streams  []Stream         `json:"streams"`
	Selected []SelectedStream `json:"selected_streams,omitempty"`
}

// Stream returns the discovered stream with the given ID, if present.
func (c Catalog) Stream(id string) (Stream, bool) {
	for _, s := range c.Streams {
		if s.ID() == id {
			return s, true
		}
	}
	return Stream{}, false
}

// Validate checks selections against discovered streams: every selected
// stream must exist, its mode must be supported, and incremental selections
// need a cursor field present in the schema.
func (c Catalog) Validate() error {
	for _, sel := range c.Selected {
		stream, ok := c.Stream(sel.ID())
		if !ok {
			return fmt.Errorf("selected stream %s not found in catalog", sel.ID())
		}
		supported := false
		for _, m := range stream.SupportedSyncModes {
			if m == sel.Mode {
				supported = true
				break
			}
		}
		if !supported {
			return fmt.Errorf("stream %s does not support sync mode %q", sel.ID(), sel.Mode)
		}
		if sel.Mode == ModeIncremental {
			if sel.CursorField == "" {
				return fmt.Errorf("stream %s: incremental mode requires cursor_field", sel.ID())
			}
			if _, ok := stream.Schema.Column(sel.CursorField); !ok {
				return fmt.Errorf("stream %s: cursor_field %q not in schema", sel.ID(), sel.CursorField)
			}
		}
	}
	return nil
}

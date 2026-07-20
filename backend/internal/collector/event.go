// Package collector ingests the engine's JSONL event stream into the control
// plane: it lands every raw event in `events` and fans out the derived rows
// (metrics, diff_runs, …) the rest of the platform reads.
//
// The event envelope here is the CONSUMER-SIDE view of the engine↔platform
// contract (engine/internal/events). The backend and engine are separate Go
// modules; rather than couple them, the collector re-declares the wire shape it
// depends on. The schema is versioned (`V`), so additive engine changes never
// break ingestion.
package collector

import (
	"encoding/json"
	"time"
)

// Event is the JSONL envelope emitted by lsengine (one object per line).
type Event struct {
	V        int             `json:"v"`
	TS       time.Time       `json:"ts"`
	Kind     string          `json:"event"`
	SyncID   string          `json:"sync_id"`
	Pipeline string          `json:"pipeline_id"`
	Stream   string          `json:"stream"`
	Payload  json.RawMessage `json:"payload"`
}

// Event kinds the collector reacts to (others are still stored raw).
const (
	KindSyncStarted      = "sync_started"
	KindSyncFinished     = "sync_finished"
	KindSyncFailed       = "sync_failed"
	KindStreamFinished   = "stream_finished"
	KindChecksumComputed = "checksum_computed"
	KindColumnMapping    = "column_mapping"
	KindSchemaChanged    = "schema_changed"
	KindAnomalyDetected  = "anomaly_detected"
)

// SyncFinished payload.
type SyncFinished struct {
	RowsRead        int64   `json:"rows_read"`
	RowsWritten     int64   `json:"rows_written"`
	BytesWritten    int64   `json:"bytes_written"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// StreamFinished payload.
type StreamFinished struct {
	Mode         string `json:"mode"`
	RowsRead     int64  `json:"rows_read"`
	RowsWritten  int64  `json:"rows_written"`
	BytesWritten int64  `json:"bytes_written"`
}

// Checksum payload (emitted once per side per stream).
type Checksum struct {
	Side     string   `json:"side"` // source | destination
	Rows     int64    `json:"rows"`
	Checksum string   `json:"checksum"`
	Columns  []string `json:"columns"`
}

// ColumnMapping payload (feeds lineage).
type ColumnMapping struct {
	SourceColumn string `json:"source_column"`
	SourceType   string `json:"source_type"`
	DestColumn   string `json:"dest_column"`
	DestType     string `json:"dest_type"`
}

// Error payload (rides on sync_failed).
type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

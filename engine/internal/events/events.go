// Package events defines the LakeSense engine event protocol: the JSONL stream
// lsengine emits on stdout, consumed verbatim by the control-plane collector.
//
// This schema is the contract between engine and platform. It is versioned via
// the envelope's "v" field; additive changes (new kinds, new payload fields)
// keep v=1, breaking changes bump it. Design decisions are recorded in
// docs/analysis/engine-protocol.md §4.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

// SchemaVersion is the current envelope version emitted in every event.
const SchemaVersion = 1

// Kind identifies the event type. Payload shape is fixed per kind.
type Kind string

const (
	// Lifecycle.
	KindEngineInfo   Kind = "engine_info"   // EngineInfo: emitted once at process start
	KindSyncStarted  Kind = "sync_started"  // SyncStarted
	KindSyncFinished Kind = "sync_finished" // SyncFinished
	KindSyncFailed   Kind = "sync_failed"   // Error
	// Per-stream progress.
	KindStreamStarted  Kind = "stream_started"  // StreamStarted
	KindStreamProgress Kind = "stream_progress" // StreamProgress: periodic
	KindStreamFinished Kind = "stream_finished" // StreamFinished
	KindChunkCompleted Kind = "chunk_completed" // ChunkCompleted
	// State.
	KindStateAdvanced Kind = "state_advanced" // StateAdvanced
	// Schema.
	KindSchemaDiscovered Kind = "schema_discovered" // SchemaDiscovered
	KindSchemaChanged    Kind = "schema_changed"    // SchemaChanged
	KindColumnMapping    Kind = "column_mapping"    // ColumnMapping: feeds lineage
	// Correctness (feeds data-diff).
	KindChecksumComputed Kind = "checksum_computed" // Checksum
	KindVerifyResult     Kind = "verify_result"     // VerifyResult
	// Diagnostics.
	KindWarning Kind = "warning" // Warning
	KindError   Kind = "error"   // Error: non-fatal; fatal errors use sync_failed
)

// Event is the JSONL envelope. One object per line on stdout.
type Event struct {
	V        int       `json:"v"`
	TS       time.Time `json:"ts"`
	Kind     Kind      `json:"event"`
	SyncID   string    `json:"sync_id,omitempty"`
	Pipeline string    `json:"pipeline_id,omitempty"`
	// Stream is "namespace.name" for stream-scoped events, empty otherwise.
	Stream  string `json:"stream,omitempty"`
	Payload any    `json:"payload,omitempty"`
}

// EngineInfo announces the binary and connector capabilities at process start.
type EngineInfo struct {
	Version      string   `json:"version"`
	Connector    string   `json:"connector,omitempty"`
	Command      string   `json:"command"`
	Capabilities []string `json:"capabilities,omitempty"` // e.g. "full_load", "incremental", "cdc"
}

// SyncStarted marks the beginning of a sync run.
type SyncStarted struct {
	Connector   string   `json:"connector"`
	Destination string   `json:"destination"`
	Streams     []string `json:"streams"`
	Resumed     bool     `json:"resumed"`
}

// SyncFinished summarizes a completed run.
type SyncFinished struct {
	RowsRead        int64   `json:"rows_read"`
	RowsWritten     int64   `json:"rows_written"`
	BytesWritten    int64   `json:"bytes_written"`
	DurationSeconds float64 `json:"duration_seconds"`
}

// StreamStarted marks a stream entering a read phase.
type StreamStarted struct {
	Mode        string `json:"mode"` // full_load | incremental | cdc | backfill | verify
	TotalChunks int    `json:"total_chunks,omitempty"`
}

// StreamProgress is a periodic per-stream progress snapshot.
type StreamProgress struct {
	Mode           string  `json:"mode"`
	RowsRead       int64   `json:"rows_read"`
	RowsWritten    int64   `json:"rows_written"`
	BytesWritten   int64   `json:"bytes_written"`
	RowsPerSecond  float64 `json:"rows_per_second"`
	ChunksDone     int     `json:"chunks_done,omitempty"`
	ChunksTotal    int     `json:"chunks_total,omitempty"`
	LagDescription string  `json:"lag,omitempty"` // e.g. CDC lag "lsn_bytes=1024"
}

// StreamFinished carries final per-stream counts; the diff badge starts here.
type StreamFinished struct {
	Mode         string `json:"mode"`
	RowsRead     int64  `json:"rows_read"`
	RowsWritten  int64  `json:"rows_written"`
	BytesWritten int64  `json:"bytes_written"`
}

// ChunkCompleted records one durable unit of full-load/backfill progress.
type ChunkCompleted struct {
	ChunkMin   string `json:"chunk_min,omitempty"`
	ChunkMax   string `json:"chunk_max,omitempty"`
	Rows       int64  `json:"rows"`
	Remaining  int    `json:"remaining_chunks"`
	DurationMS int64  `json:"duration_ms"`
}

// StateAdvanced reports a durable state mutation, so the control plane can
// track progress without reading state files.
type StateAdvanced struct {
	Scope  string `json:"scope"`  // chunk | cursor | cdc_position | backfill
	Detail string `json:"detail"` // human-readable summary, e.g. "lsn=0/1A2B3C4D"
}

// ColumnSchema describes one column in a discovered or changed schema.
type ColumnSchema struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primary_key,omitempty"`
}

// SchemaDiscovered carries a stream's full schema at discovery time.
type SchemaDiscovered struct {
	Columns            []ColumnSchema `json:"columns"`
	SupportedSyncModes []string       `json:"supported_sync_modes"`
}

// SchemaChanged reports drift detected during a sync.
type SchemaChanged struct {
	Added   []ColumnSchema  `json:"added,omitempty"`
	Dropped []string        `json:"dropped,omitempty"`
	Retyped []RetypedColumn `json:"retyped,omitempty"`
}

// RetypedColumn is a column whose type changed between runs.
type RetypedColumn struct {
	Name string `json:"name"`
	From string `json:"from"`
	To   string `json:"to"`
}

// ColumnMapping records one source→destination column edge (lineage input).
type ColumnMapping struct {
	SourceColumn string `json:"source_column"`
	SourceType   string `json:"source_type"`
	DestColumn   string `json:"dest_column"`
	DestType     string `json:"dest_type"`
}

// Checksum reports an order-independent aggregate over rows on one side of a
// sync. Matching source and destination checksums back the "verified" badge.
type Checksum struct {
	Side     string   `json:"side"` // source | destination
	Rows     int64    `json:"rows"`
	Checksum string   `json:"checksum"` // hex, order-independent aggregate of row hashes
	Columns  []string `json:"columns"`  // columns included in the row hash
}

// VerifyResult is the outcome of `lsengine verify` for one stream.
type VerifyResult struct {
	Match            bool     `json:"match"`
	SourceRows       int64    `json:"source_rows"`
	DestinationRows  int64    `json:"destination_rows"`
	MismatchedRanges []Range  `json:"mismatched_ranges,omitempty"`
	SamplePKs        []string `json:"sample_pks,omitempty"`
}

// Range is a PK range isolated by verify's bisection drill-down.
type Range struct {
	Min string `json:"min"`
	Max string `json:"max"`
}

// Warning is a non-fatal diagnostic with a stable machine-readable code.
type Warning struct {
	Code    string `json:"code"` // e.g. stats_missing, replica_identity_partial, cdc_fallback
	Message string `json:"message"`
	Hint    string `json:"hint,omitempty"` // actionable fix, e.g. "run ANALYZE tablename"
}

// Error is a structured failure. Fatal errors ride on sync_failed.
type Error struct {
	Code      string `json:"code"` // e.g. cdc_position_lost, connection_refused
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// Emitter writes events as JSONL. Safe for concurrent use.
type Emitter struct {
	mu       sync.Mutex
	enc      *json.Encoder
	syncID   string
	pipeline string
	now      func() time.Time
}

// NewEmitter creates an Emitter writing to w (normally os.Stdout).
// pipelineID may be empty when the engine runs outside the control plane.
func NewEmitter(w io.Writer, syncID, pipelineID string) *Emitter {
	return &Emitter{
		enc:      json.NewEncoder(w),
		syncID:   syncID,
		pipeline: pipelineID,
		now:      func() time.Time { return time.Now().UTC() },
	}
}

// Emit writes one event line. Stream may be empty for run-scoped events.
func (e *Emitter) Emit(kind Kind, stream string, payload any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	err := e.enc.Encode(Event{
		V:        SchemaVersion,
		TS:       e.now(),
		Kind:     kind,
		SyncID:   e.syncID,
		Pipeline: e.pipeline,
		Stream:   stream,
		Payload:  payload,
	})
	if err != nil {
		return fmt.Errorf("emit %s event: %w", kind, err)
	}
	return nil
}

// SyncID returns the run identifier this emitter stamps on every event.
func (e *Emitter) SyncID() string { return e.syncID }

// NewSyncID returns a sortable unique run ID: millisecond timestamp + random
// suffix, hex-encoded. Sortable by creation time like a ULID, stdlib-only.
func NewSyncID() string {
	var r [6]byte
	if _, err := rand.Read(r[:]); err != nil {
		// crypto/rand failure is effectively unreachable; degrade to time-only.
		return fmt.Sprintf("%012x-%012x", time.Now().UnixMilli(), 0)
	}
	return fmt.Sprintf("%012x-%s", time.Now().UnixMilli(), hex.EncodeToString(r[:]))
}

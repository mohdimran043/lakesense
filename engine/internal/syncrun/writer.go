package syncrun

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Writer is the destination abstraction, defined here on the consumer side per
// the stack rules. The orchestrator opens one StreamWriter per selected stream
// and hands it fully-formed rows (source data plus engine _ls_ metadata
// columns already injected). Concrete writers — NDJSON now, Parquet/Iceberg in
// Phase 2.5 — implement this same surface, so the orchestrator never changes
// when a destination is added.
type Writer interface {
	// Open returns a StreamWriter for one stream. destTable overrides the
	// default destination name when the selection specifies one. truncate
	// requests a fresh destination (a first full-refresh with no durable
	// progress); when false the writer appends, so a resumed run preserves
	// chunks already committed. At-least-once re-reads of an interrupted
	// chunk are deduplicated by record ID once a merge-capable destination
	// (Parquet/Iceberg, Phase 2.5) lands; the interim NDJSON writer appends.
	Open(ctx context.Context, stream model.Stream, destTable string, truncate bool) (StreamWriter, error)
	// Close releases any writer-wide resources after all streams finish.
	Close(ctx context.Context) error
}

// StreamWriter accepts rows for one stream and reports what it durably wrote.
type StreamWriter interface {
	// WriteRow persists one row. The row already carries _ls_ metadata; the
	// writer independently checksums the data (non-metadata) columns so its
	// reported digest is genuine proof of what reached the destination, not a
	// copy of the source's count.
	WriteRow(ctx context.Context, row sdk.Row) error
	// Flush makes every row written so far durable. The orchestrator calls it
	// at each state-commit boundary (chunk done, cursor advance, CDC position
	// advance) BEFORE recording that progress in state — the write-ahead
	// discipline that keeps a crash from losing rows a completed-chunk marker
	// claims are present. Safe to call repeatedly.
	Flush(ctx context.Context) error
	// Close flushes and returns the durable result. It must be safe to call
	// exactly once.
	Close(ctx context.Context) (WriteResult, error)
}

// WriteResult is a stream writer's self-reported accounting.
type WriteResult struct {
	Rows     int64
	Bytes    int64
	Checksum string // hex digest over data columns of the rows written
}

// DestinationConfig selects and configures a destination. Shape follows the
// {type, ...} pattern from docs/analysis/engine-protocol.md §4.
type DestinationConfig struct {
	Type string `json:"type"` // "ndjson" (v0.1); "parquet"/"iceberg" in 2.5
	// Path is the output directory for file destinations.
	Path string `json:"path,omitempty"`
}

// OpenWriter constructs the writer named by cfg.Type.
func OpenWriter(cfg DestinationConfig) (Writer, error) {
	switch cfg.Type {
	case "", "ndjson":
		if cfg.Path == "" {
			return nil, fmt.Errorf("ndjson destination requires a path")
		}
		return newNDJSONWriter(cfg.Path)
	default:
		return nil, fmt.Errorf("unknown destination type %q (v0.1 supports \"ndjson\"; parquet/iceberg land in Phase 2.5)", cfg.Type)
	}
}

// LoadDestinationConfig decodes a destination config document.
func LoadDestinationConfig(raw json.RawMessage) (DestinationConfig, error) {
	var cfg DestinationConfig
	if len(raw) == 0 {
		return cfg, fmt.Errorf("destination config is empty")
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse destination config: %w", err)
	}
	return cfg, nil
}

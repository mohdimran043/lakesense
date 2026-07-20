package syncrun

import (
	"context"
	"fmt"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// PKRange is a half-open [Min, Max) filter over a stream's record id; empty
// bounds are unbounded on that side.
type PKRange struct {
	Min string
	Max string
}

// Reader is the read-back side of a destination. verify and backfill use it to
// materialize the destination's current logical state.
type Reader interface {
	OpenRead(ctx context.Context, stream model.Stream, destTable string) (StreamReader, error)
	Close(ctx context.Context) error
}

// StreamReader yields the destination's current state for one stream: the
// latest row per _ls_id (max _ls_ingested_at), deletes omitted. bounds, when
// non-nil, restricts to record ids in [Min, Max).
type StreamReader interface {
	Read(ctx context.Context, bounds *PKRange, emit sdk.RowFunc) error
	Close(ctx context.Context) error
}

// OpenReader constructs the reader matching a destination config.
func OpenReader(cfg DestinationConfig) (Reader, error) {
	switch cfg.Type {
	case "", "ndjson":
		return newNDJSONReader(cfg.Path)
	case "parquet":
		return newParquetReader(cfg.Path)
	default:
		return nil, fmt.Errorf("no reader for destination type %q", cfg.Type)
	}
}

// resolveCurrentState collapses an append log into current state: for each
// _ls_id keep the row with the greatest _ls_ingested_at; drop ids whose latest
// op is delete. Ties break on later slice position (stable last-writer-wins).
func resolveCurrentState(rows []sdk.Row) []sdk.Row {
	type entry struct {
		row sdk.Row
		ts  string
	}
	latest := map[string]entry{}
	for _, row := range rows {
		id, _ := row[model.ColRecordID].(string)
		ts, _ := row[model.ColIngestedAt].(string)
		cur, ok := latest[id]
		if !ok || ts >= cur.ts {
			latest[id] = entry{row: row, ts: ts}
		}
	}
	out := make([]sdk.Row, 0, len(latest))
	for _, e := range latest {
		if op, _ := e.row[model.ColOpType].(string); op == "d" {
			continue
		}
		out = append(out, e.row)
	}
	return out
}

// inRange reports whether a row's record id falls inside bounds (nil = all).
func inRange(row sdk.Row, bounds *PKRange) bool {
	if bounds == nil {
		return true
	}
	id, _ := row[model.ColRecordID].(string)
	if bounds.Min != "" && id < bounds.Min {
		return false
	}
	if bounds.Max != "" && id >= bounds.Max {
		return false
	}
	return true
}

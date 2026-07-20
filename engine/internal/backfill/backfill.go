// Package backfill re-syncs a bounded slice of one stream and merges it into
// the destination without a full reload, never touching CDC/cursor/chunk state.
// It appends corrected rows (merge-on-read via _ls_id + a fresh _ls_ingested_at)
// and closes by verifying the stream, so the diff badge goes green again.
//
// State-safety is structural: Options carries no *state.Document, so a backfill
// cannot advance a CDC position, complete a chunk, or move a cursor.
package backfill

import (
	"context"
	"fmt"
	"time"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/lakesense/lakesense/engine/internal/syncrun"
	"github.com/lakesense/lakesense/engine/internal/verify"
)

// Options are the fully-resolved inputs to a backfill.
type Options struct {
	Connector  sdk.Connector
	Writer     syncrun.Writer
	Reader     syncrun.Reader
	Stream     model.Stream
	Selection  model.SelectedStream
	Emitter    *events.Emitter
	PKMin      string // inclusive rowid/PK lower bound (PK-range mode)
	PKMax      string // exclusive rowid/PK upper bound (PK-range mode)
	SinceField string // cursor field (changed-since mode)
	SinceValue string // cursor value (changed-since mode)
	Now        func() time.Time
}

// Run performs the backfill and returns the closing verify result for the
// stream. The returned result's Match reports whether the merge reconciled.
func Run(ctx context.Context, opts Options) (events.VerifyResult, error) {
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if err := opts.Emitter.Emit(events.KindStreamStarted, opts.Stream.ID(), events.StreamStarted{Mode: "backfill"}); err != nil {
		return events.VerifyResult{}, err
	}

	// Append, never truncate — the backfill merges over existing part-files.
	sw, err := opts.Writer.Open(ctx, opts.Stream, opts.Selection.DestinationTable, false)
	if err != nil {
		return events.VerifyResult{}, err
	}
	cols := syncrun.DataColumns(opts.Stream)
	pk := opts.Stream.Schema.PrimaryKey()
	src := &syncrun.AggregateDigest{}

	writeRow := func(_ context.Context, row sdk.Row) error {
		h, err := syncrun.HashDataColumns(row, cols)
		if err != nil {
			return err
		}
		src.Add(h)
		syncrun.InjectMetadata(row, pk, "u", opts.Now()) // op "u": merge correction
		return sw.WriteRow(ctx, row)
	}

	if err := opts.readSlice(ctx, writeRow); err != nil {
		return events.VerifyResult{}, err
	}
	if err := sw.Flush(ctx); err != nil {
		return events.VerifyResult{}, err
	}
	res, err := sw.Close(ctx)
	if err != nil {
		return events.VerifyResult{}, err
	}

	_ = opts.Emitter.Emit(events.KindStateAdvanced, opts.Stream.ID(), events.StateAdvanced{
		Scope: "backfill", Detail: fmt.Sprintf("%s wrote %d rows", opts.Stream.ID(), res.Rows),
	})
	_ = opts.Emitter.Emit(events.KindChecksumComputed, opts.Stream.ID(), events.Checksum{
		Side: "source", Rows: src.Rows(), Checksum: src.Hex(), Columns: cols,
	})
	_ = opts.Emitter.Emit(events.KindChecksumComputed, opts.Stream.ID(), events.Checksum{
		Side: "destination", Rows: res.Rows, Checksum: res.Checksum, Columns: cols,
	})

	// Closing verify: the merged destination must now reconcile with the source.
	fl, ok := opts.Connector.(sdk.FullLoader)
	if !ok {
		return events.VerifyResult{}, fmt.Errorf("connector cannot full-load; backfill needs it for the closing verify")
	}
	sr, err := opts.Reader.OpenRead(ctx, opts.Stream, opts.Selection.DestinationTable)
	if err != nil {
		return events.VerifyResult{}, err
	}
	defer func() { _ = sr.Close(ctx) }()
	vr, err := verify.VerifyStream(ctx, verify.StreamInput{Stream: opts.Stream, Source: fl, DestReader: sr})
	if err != nil {
		return events.VerifyResult{}, err
	}
	_ = opts.Emitter.Emit(events.KindVerifyResult, opts.Stream.ID(), vr)
	return vr, nil
}

// readSlice reads the bounded source slice: a changed-since window when
// SinceField is set, otherwise a PK/rowid range.
func (opts Options) readSlice(ctx context.Context, emit sdk.RowFunc) error {
	if opts.SinceField != "" {
		ir, ok := opts.Connector.(sdk.IncrementalReader)
		if !ok {
			return fmt.Errorf("connector does not support incremental read for a --since backfill")
		}
		if _, err := ir.ReadIncrement(ctx, opts.Stream, opts.SinceField, opts.SinceValue, emit); err != nil {
			return fmt.Errorf("backfill since %s=%s: %w", opts.SinceField, opts.SinceValue, err)
		}
		return nil
	}
	fl, ok := opts.Connector.(sdk.FullLoader)
	if !ok {
		return fmt.Errorf("connector does not support full load for a PK-range backfill")
	}
	chunk := state.Chunk{Min: opts.PKMin, Max: opts.PKMax}
	if err := fl.ReadChunk(ctx, opts.Stream, chunk, emit); err != nil {
		return fmt.Errorf("backfill range [%s,%s): %w", opts.PKMin, opts.PKMax, err)
	}
	return nil
}

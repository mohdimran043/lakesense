// Package syncrun is the engine's sync orchestrator: the source-agnostic layer
// that drives a connector through full-load, incremental, and CDC phases,
// owning chunk resume, cursor bookkeeping, CDC phasing, engine-metadata
// injection, and the JSONL event stream — while connectors implement only the
// narrow SDK leaf operations. Layering follows docs/analysis/engine-protocol.md
// §3 and the state/CDC semantics in docs/analysis/state-and-recovery.md.
package syncrun

import (
	"context"
	"fmt"
	"time"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// Options are the fully-resolved inputs to a sync run. The CLI layer loads
// files and builds these; tests construct them directly with fakes.
type Options struct {
	Connector       sdk.Connector
	Writer          Writer
	Catalog         model.Catalog
	State           *state.Document
	Emitter         *events.Emitter
	ConnectorType   string // stamped on events
	DestinationType string // stamped on events
	// Now supplies wall-clock time (injectable for deterministic tests).
	Now func() time.Time
}

// runner carries per-run state so the phase methods stay readable.
type runner struct {
	opts       Options
	now        func() time.Time
	totalRead  int64
	totalWrite int64
	totalBytes int64
}

// Run executes a full sync per the catalog's stream selections. It is the
// single entry point behind `lsengine sync`.
func Run(ctx context.Context, opts Options) error {
	if opts.Connector == nil || opts.Writer == nil || opts.Emitter == nil || opts.State == nil {
		return fmt.Errorf("syncrun: Connector, Writer, Emitter, and State are required")
	}
	if opts.Now == nil {
		opts.Now = func() time.Time { return time.Now().UTC() }
	}
	if err := opts.Catalog.Validate(); err != nil {
		return fmt.Errorf("catalog: %w", err)
	}
	r := &runner{opts: opts, now: opts.Now}
	return r.run(ctx)
}

func (r *runner) run(ctx context.Context) error {
	start := r.now()
	selected := r.opts.Catalog.Selected
	if len(selected) == 0 {
		return fmt.Errorf("no streams selected for sync")
	}

	ids := make([]string, len(selected))
	resumed := false
	var cdc []model.SelectedStream
	for i, sel := range selected {
		ids[i] = sel.ID()
		if r.streamHasProgress(sel) {
			resumed = true
		}
		if sel.Mode == model.ModeCDC {
			cdc = append(cdc, sel)
		}
	}

	if err := r.emit(events.KindSyncStarted, "", events.SyncStarted{
		Connector:   r.opts.ConnectorType,
		Destination: r.opts.DestinationType,
		Streams:     ids,
		Resumed:     resumed,
	}); err != nil {
		return err
	}

	for _, sel := range selected {
		if sel.Mode == model.ModeCDC {
			continue // CDC streams run together as a group below
		}
		if err := r.runStream(ctx, sel); err != nil {
			r.fail(sel.ID(), err)
			return err
		}
	}

	if len(cdc) > 0 {
		if err := r.runCDCGroup(ctx, cdc); err != nil {
			r.fail("", err)
			return err
		}
	}

	return r.emit(events.KindSyncFinished, "", events.SyncFinished{
		RowsRead:        r.totalRead,
		RowsWritten:     r.totalWrite,
		BytesWritten:    r.totalBytes,
		DurationSeconds: r.now().Sub(start).Seconds(),
	})
}

// runStream dispatches one non-CDC stream to its phase.
func (r *runner) runStream(ctx context.Context, sel model.SelectedStream) error {
	stream, ok := r.opts.Catalog.Stream(sel.ID())
	if !ok {
		return fmt.Errorf("selected stream %s missing from catalog", sel.ID())
	}
	switch sel.Mode {
	case model.ModeFullLoad:
		return r.runFullLoad(ctx, stream, sel)
	case model.ModeIncremental:
		return r.runIncremental(ctx, stream, sel)
	default:
		return fmt.Errorf("stream %s: unsupported sync mode %q", sel.ID(), sel.Mode)
	}
}

// runFullLoad runs (or resumes) chunked full-load for one stream.
func (r *runner) runFullLoad(ctx context.Context, stream model.Stream, sel model.SelectedStream) error {
	fl, ok := r.opts.Connector.(sdk.FullLoader)
	if !ok {
		return fmt.Errorf("connector does not support full load for %s", stream.ID())
	}
	ns, name := stream.Namespace, stream.Name

	remaining, done := r.opts.State.RemainingChunks(ns, name)
	fresh := remaining == nil && !done
	if fresh {
		chunks, err := fl.SplitChunks(ctx, stream)
		if err != nil {
			return fmt.Errorf("split %s: %w", stream.ID(), err)
		}
		if err := r.opts.State.SetChunks(ns, name, chunks); err != nil {
			return err
		}
		remaining = chunks
	}

	sw, err := r.opts.Writer.Open(ctx, stream, sel.DestinationTable, fresh)
	if err != nil {
		return err
	}
	if err := r.emit(events.KindStreamStarted, stream.ID(), events.StreamStarted{
		Mode: string(model.ModeFullLoad), TotalChunks: len(remaining),
	}); err != nil {
		return err
	}

	src := &digest{}
	cols := dataColumns(stream)
	var streamRead int64
	for _, chunk := range remaining {
		chunkStart := r.now()
		var chunkRows int64
		err := fl.ReadChunk(ctx, stream, chunk, func(ctx context.Context, row sdk.Row) error {
			return r.handleRow(ctx, sw, stream, cols, src, row, "r", time.Time{}, &chunkRows, &streamRead)
		})
		if err != nil {
			return fmt.Errorf("read chunk [%s,%s) of %s: %w", chunk.Min, chunk.Max, stream.ID(), err)
		}
		// Write-ahead: the chunk's rows must be durable before state records
		// the chunk as done, or a crash here would lose rows the completed
		// marker claims are present.
		if err := sw.Flush(ctx); err != nil {
			return err
		}
		left, err := r.opts.State.CompleteChunk(ns, name, chunk)
		if err != nil {
			return err
		}
		if err := r.emit(events.KindChunkCompleted, stream.ID(), events.ChunkCompleted{
			ChunkMin: chunk.Min, ChunkMax: chunk.Max, Rows: chunkRows,
			Remaining: left, DurationMS: r.now().Sub(chunkStart).Milliseconds(),
		}); err != nil {
			return err
		}
		if err := r.emitStateAdvanced("chunk", fmt.Sprintf("%s [%s,%s) done, %d remaining", stream.ID(), chunk.Min, chunk.Max, left)); err != nil {
			return err
		}
	}

	return r.finishStream(ctx, stream, sw, model.ModeFullLoad, src, streamRead)
}

// runIncremental reads rows past the stored cursor (all rows on first run) and
// advances the cursor watermark in state.
func (r *runner) runIncremental(ctx context.Context, stream model.Stream, sel model.SelectedStream) error {
	ir, ok := r.opts.Connector.(sdk.IncrementalReader)
	if !ok {
		return fmt.Errorf("connector does not support incremental read for %s", stream.ID())
	}
	ns, name := stream.Namespace, stream.Name
	since, hasCursor := r.opts.State.Cursor(ns, name, sel.CursorField)

	sw, err := r.opts.Writer.Open(ctx, stream, sel.DestinationTable, !hasCursor)
	if err != nil {
		return err
	}
	if err := r.emit(events.KindStreamStarted, stream.ID(), events.StreamStarted{Mode: string(model.ModeIncremental)}); err != nil {
		return err
	}

	src := &digest{}
	cols := dataColumns(stream)
	var streamRead, unused int64
	newCursor, err := ir.ReadIncrement(ctx, stream, sel.CursorField, since, func(ctx context.Context, row sdk.Row) error {
		return r.handleRow(ctx, sw, stream, cols, src, row, "r", time.Time{}, &unused, &streamRead)
	})
	if err != nil {
		return fmt.Errorf("incremental read of %s: %w", stream.ID(), err)
	}
	if newCursor != "" && newCursor != since {
		// Durable before the watermark advances, so a resume never skips rows
		// that were counted but not yet written.
		if err := sw.Flush(ctx); err != nil {
			return err
		}
		if err := r.opts.State.SetCursor(ns, name, sel.CursorField, newCursor); err != nil {
			return err
		}
		if err := r.emitStateAdvanced("cursor", fmt.Sprintf("%s %s=%s", stream.ID(), sel.CursorField, newCursor)); err != nil {
			return err
		}
	}

	return r.finishStream(ctx, stream, sw, model.ModeIncremental, src, streamRead)
}

// runCDCGroup runs the sequential-log CDC flow for one or more streams sharing
// a global position (Postgres WAL): anchor before backfill, backfill streams
// not yet done under the anchor, then replay changes to the bounded target.
func (r *runner) runCDCGroup(ctx context.Context, sels []model.SelectedStream) error {
	cs, ok := r.opts.Connector.(sdk.ChangeStreamer)
	if !ok {
		return fmt.Errorf("connector does not support CDC")
	}
	streams := make([]model.Stream, 0, len(sels))
	for _, sel := range sels {
		s, ok := r.opts.Catalog.Stream(sel.ID())
		if !ok {
			return fmt.Errorf("CDC stream %s missing from catalog", sel.ID())
		}
		streams = append(streams, s)
	}

	// Anchor: capture (or reuse) the replication position BEFORE backfill so
	// nothing falls in a gap between snapshot and stream.
	pos, ok := r.opts.State.GlobalPosition()
	if !ok {
		p, err := cs.PrepareCDC(ctx, streams)
		if err != nil {
			return fmt.Errorf("prepare CDC: %w", err)
		}
		if err := r.opts.State.SetGlobalPosition(p); err != nil {
			return err
		}
		if err := r.emitStateAdvanced("cdc_position", fmt.Sprintf("anchored at %v", p)); err != nil {
			return err
		}
		pos = p
	}

	// Per-stream initial backfill for streams not yet backfilled under this
	// anchor. Writers stay open through streaming so CDC changes append.
	writers := make(map[string]StreamWriter, len(streams))
	digests := make(map[string]*digest, len(streams))
	defer func() {
		for _, sw := range writers {
			_, _ = sw.Close(context.WithoutCancel(ctx))
		}
	}()
	for i, s := range streams {
		sel := sels[i]
		backfilled := r.opts.State.BackfilledGlobally(s.ID())
		sw, err := r.opts.Writer.Open(ctx, s, sel.DestinationTable, !backfilled)
		if err != nil {
			return err
		}
		writers[s.ID()] = sw
		digests[s.ID()] = &digest{}

		if !backfilled {
			if err := r.cdcBackfill(ctx, s, sw, digests[s.ID()]); err != nil {
				return err
			}
			// Snapshot durable before the stream is marked backfilled under
			// the anchor.
			if err := sw.Flush(ctx); err != nil {
				return err
			}
			if err := r.opts.State.SetGlobalPosition(nil, s.ID()); err != nil {
				return err
			}
		}
	}

	// Stream changes to the bounded target captured at call time.
	if err := r.emit(events.KindStreamStarted, "", events.StreamStarted{Mode: string(model.ModeCDC)}); err != nil {
		return err
	}
	final, err := cs.StreamChanges(ctx, streams, pos, func(ctx context.Context, ch sdk.Change) error {
		sw, ok := writers[ch.StreamID]
		if !ok {
			return nil // change for a stream not in this run
		}
		s, _ := r.opts.Catalog.Stream(ch.StreamID)
		op := cdcOp(ch.Kind)
		return r.handleRow(ctx, sw, s, dataColumns(s), digests[ch.StreamID], ch.Data, op, ch.Timestamp, new(int64), &r.totalRead)
	})
	if err != nil {
		return fmt.Errorf("stream changes: %w", err)
	}
	// Ack-before-state: every replayed change must be durable before the CDC
	// position advances, so a crash re-reads from the last durable position
	// rather than skipping changes.
	for _, sw := range writers {
		if err := sw.Flush(ctx); err != nil {
			return err
		}
	}
	if err := r.opts.State.SetGlobalPosition(final); err != nil {
		return err
	}
	if err := r.emitStateAdvanced("cdc_position", fmt.Sprintf("advanced to %v", final)); err != nil {
		return err
	}

	// Close writers and emit per-stream results + checksums.
	for _, s := range streams {
		sw := writers[s.ID()]
		delete(writers, s.ID())
		if err := r.closeAndReport(ctx, s, sw, model.ModeCDC, digests[s.ID()]); err != nil {
			return err
		}
	}
	return nil
}

// cdcBackfill does a full-load snapshot of one stream during the CDC overlap
// window (op "r"); duplicate rows converge with CDC replay via record ID.
func (r *runner) cdcBackfill(ctx context.Context, stream model.Stream, sw StreamWriter, src *digest) error {
	fl, ok := r.opts.Connector.(sdk.FullLoader)
	if !ok {
		return fmt.Errorf("connector supports CDC but not the full-load backfill needed for %s", stream.ID())
	}
	chunks, err := fl.SplitChunks(ctx, stream)
	if err != nil {
		return fmt.Errorf("split %s for CDC backfill: %w", stream.ID(), err)
	}
	cols := dataColumns(stream)
	var read int64
	for _, chunk := range chunks {
		if err := fl.ReadChunk(ctx, stream, chunk, func(ctx context.Context, row sdk.Row) error {
			return r.handleRow(ctx, sw, stream, cols, src, row, "r", time.Time{}, new(int64), &read)
		}); err != nil {
			return fmt.Errorf("CDC backfill chunk of %s: %w", stream.ID(), err)
		}
	}
	return nil
}

// handleRow is the shared write path: source-side checksum over data columns,
// engine-metadata injection, write, and counter bookkeeping.
func (r *runner) handleRow(ctx context.Context, sw StreamWriter, stream model.Stream, cols []string, src *digest, row sdk.Row, op string, cdcTS time.Time, streamCount, runCount *int64) error {
	h, err := hashRow(row, cols)
	if err != nil {
		return err
	}
	src.add(h)
	r.injectMetadata(row, stream, op, cdcTS)
	if err := sw.WriteRow(ctx, row); err != nil {
		return err
	}
	*streamCount++
	*runCount++
	r.totalWrite++
	return nil
}

// injectMetadata stamps the engine-owned _ls_ columns. recordID is computed
// from the primary key before other metadata is added, so it stays stable
// across a full-load read and a later CDC replay of the same row.
func (r *runner) injectMetadata(row sdk.Row, stream model.Stream, op string, cdcTS time.Time) {
	row[model.ColRecordID] = recordID(row, stream.Schema.PrimaryKey())
	row[model.ColIngestedAt] = r.now().UTC().Format(time.RFC3339Nano)
	row[model.ColOpType] = op
	if !cdcTS.IsZero() {
		row[model.ColCDCTimestamp] = cdcTS.UTC().Format(time.RFC3339Nano)
	}
}

// finishStream closes a stream writer and emits its results and checksums.
func (r *runner) finishStream(ctx context.Context, stream model.Stream, sw StreamWriter, mode model.SyncMode, src *digest, read int64) error {
	if err := r.closeAndReport(ctx, stream, sw, mode, src); err != nil {
		return err
	}
	_ = read
	return nil
}

// closeAndReport closes the writer and emits checksum_computed for both sides
// plus stream_finished — the events that seed the data-diff badge.
func (r *runner) closeAndReport(ctx context.Context, stream model.Stream, sw StreamWriter, mode model.SyncMode, src *digest) error {
	res, err := sw.Close(ctx)
	if err != nil {
		return err
	}
	r.totalBytes += res.Bytes

	cols := dataColumns(stream)
	if err := r.emit(events.KindChecksumComputed, stream.ID(), events.Checksum{
		Side: "source", Rows: src.Rows(), Checksum: src.Hex(), Columns: cols,
	}); err != nil {
		return err
	}
	if err := r.emit(events.KindChecksumComputed, stream.ID(), events.Checksum{
		Side: "destination", Rows: res.Rows, Checksum: res.Checksum, Columns: cols,
	}); err != nil {
		return err
	}
	return r.emit(events.KindStreamFinished, stream.ID(), events.StreamFinished{
		Mode: string(mode), RowsRead: src.Rows(), RowsWritten: res.Rows, BytesWritten: res.Bytes,
	})
}

func (r *runner) streamHasProgress(sel model.SelectedStream) bool {
	stream, ok := r.opts.Catalog.Stream(sel.ID())
	if !ok {
		return false
	}
	if _, done := r.opts.State.RemainingChunks(stream.Namespace, stream.Name); done {
		return true
	}
	if remaining, _ := r.opts.State.RemainingChunks(stream.Namespace, stream.Name); remaining != nil {
		return true
	}
	if sel.CursorField != "" {
		if _, ok := r.opts.State.Cursor(stream.Namespace, stream.Name, sel.CursorField); ok {
			return true
		}
	}
	if sel.Mode == model.ModeCDC {
		if _, ok := r.opts.State.GlobalPosition(); ok {
			return true
		}
	}
	return false
}

func cdcOp(kind sdk.ChangeKind) string {
	switch kind {
	case sdk.ChangeInsert:
		return "i"
	case sdk.ChangeUpdate:
		return "u"
	case sdk.ChangeDelete:
		return "d"
	default:
		return "i"
	}
}

func (r *runner) emit(kind events.Kind, stream string, payload any) error {
	return r.opts.Emitter.Emit(kind, stream, payload)
}

func (r *runner) emitStateAdvanced(scope, detail string) error {
	return r.emit(events.KindStateAdvanced, "", events.StateAdvanced{Scope: scope, Detail: detail})
}

// fail emits a structured sync_failed event; the returned error still
// propagates to the caller for the process exit code.
func (r *runner) fail(stream string, err error) {
	_ = r.emit(events.KindSyncFailed, stream, events.Error{
		Code: "sync_failed", Message: err.Error(), Retryable: false,
	})
}

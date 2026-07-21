package collector

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Sink is the collector's consumer-side persistence interface. The pg
// implementation lives alongside; tests use a fake. Keeping it narrow means the
// ingestion logic (the valuable part) is testable without a database.
type Sink interface {
	// InsertEvent lands one raw event.
	InsertEvent(ctx context.Context, pipelineID int64, e Event) error
	// RecordMetric stores a per-run metric row (from sync_finished).
	RecordMetric(ctx context.Context, pipelineID int64, syncID string, m SyncFinished, ts time.Time) error
	// UpsertDiffRun records a source-vs-destination checksum comparison for a
	// stream in a sync (the data-diff badge). match is source==destination.
	UpsertDiffRun(ctx context.Context, pipelineID int64, syncID, stream string, d DiffRun) error
	// RecordLineage upserts one source→destination column edge.
	RecordLineage(ctx context.Context, pipelineID int64, stream string, m ColumnMapping, syncID string) error
	// MarkSynced updates the pipeline's last-sync timestamp.
	MarkSynced(ctx context.Context, pipelineID int64, e Event) error
}

// DiffRun is a resolved source/destination comparison for one stream.
type DiffRun struct {
	SourceRows     int64
	DestRows       int64
	SourceChecksum string
	DestChecksum   string
	Match          bool
}

// Processor is an optional post-persist hook: after an event is stored and
// derived, the Ingester hands it here so the live intelligence layer (rules →
// incidents → alerts, anomaly, quality) can react. Defined here (consumer side)
// and injected, so the collector never depends on the rules packages. A
// processor error is logged by the implementation, never fatal to ingestion.
type Processor func(ctx context.Context, pipelineID int64, e Event)

// Ingester consumes a JSONL event stream for one pipeline run and persists
// raw + derived rows through a Sink, optionally forwarding each event to a
// Processor for live reaction.
type Ingester struct {
	sink Sink
	proc Processor
}

// NewIngester constructs an Ingester over the given sink. Options attach an
// optional live Processor.
func NewIngester(sink Sink, opts ...Option) *Ingester {
	i := &Ingester{sink: sink}
	for _, o := range opts {
		o(i)
	}
	return i
}

// Option configures an Ingester.
type Option func(*Ingester)

// WithProcessor forwards each persisted event to proc for live reaction.
func WithProcessor(proc Processor) Option {
	return func(i *Ingester) { i.proc = proc }
}

// checksumPair accumulates the two sides of a stream's checksum until both are
// seen, at which point a diff_run is emitted.
type checksumPair struct {
	source *Checksum
	dest   *Checksum
}

// Ingest reads JSONL events from r until EOF, persisting each and deriving
// metrics, diff runs, and lineage. Malformed lines are skipped with an error
// return only when nothing could be processed; individual bad lines are
// tolerated so one corrupt event never drops a whole run. It returns the count
// of events successfully stored.
func (i *Ingester) Ingest(ctx context.Context, pipelineID int64, r io.Reader) (int, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4<<20)

	pairs := map[string]*checksumPair{} // key: syncID|stream
	var stored int

	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Event
		if err := json.Unmarshal(line, &e); err != nil {
			continue // tolerate a single malformed line
		}
		if err := i.sink.InsertEvent(ctx, pipelineID, e); err != nil {
			return stored, fmt.Errorf("insert event: %w", err)
		}
		stored++
		if err := i.derive(ctx, pipelineID, e, pairs); err != nil {
			return stored, err
		}
		if i.proc != nil {
			i.proc(ctx, pipelineID, e)
		}
	}
	if err := sc.Err(); err != nil {
		return stored, fmt.Errorf("scan events: %w", err)
	}
	return stored, nil
}

// derive fans one event out into the derived tables.
func (i *Ingester) derive(ctx context.Context, pipelineID int64, e Event, pairs map[string]*checksumPair) error {
	switch e.Kind {
	case KindSyncFinished:
		var p SyncFinished
		if err := json.Unmarshal(e.Payload, &p); err != nil {
			return fmt.Errorf("decode sync_finished: %w", err)
		}
		if err := i.sink.RecordMetric(ctx, pipelineID, e.SyncID, p, e.TS); err != nil {
			return fmt.Errorf("record metric: %w", err)
		}
		if err := i.sink.MarkSynced(ctx, pipelineID, e); err != nil {
			return fmt.Errorf("mark synced: %w", err)
		}

	case KindChecksumComputed:
		var c Checksum
		if err := json.Unmarshal(e.Payload, &c); err != nil {
			return fmt.Errorf("decode checksum: %w", err)
		}
		key := e.SyncID + "|" + e.Stream
		pair := pairs[key]
		if pair == nil {
			pair = &checksumPair{}
			pairs[key] = pair
		}
		if c.Side == "source" {
			pair.source = &c
		} else {
			pair.dest = &c
		}
		if pair.source != nil && pair.dest != nil {
			d := DiffRun{
				SourceRows:     pair.source.Rows,
				DestRows:       pair.dest.Rows,
				SourceChecksum: pair.source.Checksum,
				DestChecksum:   pair.dest.Checksum,
				Match: pair.source.Rows == pair.dest.Rows &&
					pair.source.Checksum == pair.dest.Checksum,
			}
			if err := i.sink.UpsertDiffRun(ctx, pipelineID, e.SyncID, e.Stream, d); err != nil {
				return fmt.Errorf("upsert diff run: %w", err)
			}
			delete(pairs, key)
		}

	case KindColumnMapping:
		var m ColumnMapping
		if err := json.Unmarshal(e.Payload, &m); err != nil {
			return fmt.Errorf("decode column_mapping: %w", err)
		}
		if err := i.sink.RecordLineage(ctx, pipelineID, e.Stream, m, e.SyncID); err != nil {
			return fmt.Errorf("record lineage: %w", err)
		}
	}
	return nil
}

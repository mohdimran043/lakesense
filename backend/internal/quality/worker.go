package quality

import (
	"context"
	"fmt"
	"time"
)

// Monitor is one configured data-quality check. Config and Baseline hold the
// kind-specific numbers (e.g. {"threshold_seconds":3600}, {"max_increase":0.1};
// baseline {"mean":1000} / {"null_rate":0.02}).
type Monitor struct {
	ID         int64
	PipelineID int64
	Stream     string
	Column     string // "" for table-level (freshness/volume)
	Kind       string // freshness | volume | null_rate | distribution
	Config     map[string]float64
	Baseline   map[string]float64
}

// Stat is the latest per-column measurement a monitor evaluates against.
type Stat struct {
	Rows   int64
	Nulls  int64
	SyncID string
	TS     time.Time
}

// Store surfaces the inputs a worker needs and persists results.
type Store interface {
	// Monitors returns every enabled monitor.
	Monitors(ctx context.Context) ([]Monitor, error)
	// LatestStat returns the most recent column_stats for (pipeline, stream,
	// column); an empty column matches the stream's most recent stat.
	LatestStat(ctx context.Context, pipelineID int64, stream, column string) (Stat, bool, error)
	// SinceLastSync returns how long ago a pipeline last synced.
	SinceLastSync(ctx context.Context, pipelineID int64) (time.Duration, bool, error)
	// RecordResult persists a monitor evaluation.
	RecordResult(ctx context.Context, monitorID int64, syncID string, r Result, ts time.Time) error
}

// Emit delivers a breach into the platform (as a quality_breach event down the
// rules→incident→alert path).
type Emit func(ctx context.Context, pipelineID int64, m Monitor, r Result) error

// Worker evaluates every enabled monitor on each Tick against the latest stats
// and stored baselines, records results, and emits breaches.
type Worker struct {
	store Store
	emit  Emit
	now   func() time.Time
}

// NewWorker builds a quality worker.
func NewWorker(store Store, emit Emit, now func() time.Time) *Worker {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Worker{store: store, emit: emit, now: now}
}

// Tick evaluates all monitors once. Distribution monitors are skipped in v1
// (they need a histogram sketch the engine does not yet emit).
func (w *Worker) Tick(ctx context.Context) error {
	monitors, err := w.store.Monitors(ctx)
	if err != nil {
		return fmt.Errorf("load monitors: %w", err)
	}
	now := w.now()
	for _, m := range monitors {
		res, syncID, ok, err := w.evaluate(ctx, m)
		if err != nil {
			return err
		}
		if !ok {
			continue // not enough data yet (no baseline / no stats / no sync)
		}
		if err := w.store.RecordResult(ctx, m.ID, syncID, res, now); err != nil {
			return fmt.Errorf("record result for monitor %d: %w", m.ID, err)
		}
		if res.Breached && w.emit != nil {
			if err := w.emit(ctx, m.PipelineID, m, res); err != nil {
				return fmt.Errorf("emit breach for monitor %d: %w", m.ID, err)
			}
		}
	}
	return nil
}

// evaluate runs one monitor's pure evaluator against fresh inputs. ok is false
// when the monitor lacks the data to be evaluated.
func (w *Worker) evaluate(ctx context.Context, m Monitor) (res Result, syncID string, ok bool, err error) {
	switch m.Kind {
	case "freshness":
		since, has, e := w.store.SinceLastSync(ctx, m.PipelineID)
		if e != nil || !has {
			return Result{}, "", false, e
		}
		thr := time.Duration(m.Config["threshold_seconds"]) * time.Second
		return Freshness(since, thr), "", true, nil

	case "volume":
		stat, has, e := w.store.LatestStat(ctx, m.PipelineID, m.Stream, m.Column)
		if e != nil || !has {
			return Result{}, "", false, e
		}
		return Volume(float64(stat.Rows), m.Baseline["mean"], m.Config["max_deviation"]), stat.SyncID, true, nil

	case "null_rate":
		stat, has, e := w.store.LatestStat(ctx, m.PipelineID, m.Stream, m.Column)
		if e != nil || !has || stat.Rows == 0 {
			return Result{}, "", false, e
		}
		current := float64(stat.Nulls) / float64(stat.Rows)
		return NullRate(current, m.Baseline["null_rate"], m.Config["max_increase"]), stat.SyncID, true, nil

	default:
		// distribution (needs a histogram) and unknown kinds are skipped.
		return Result{}, "", false, nil
	}
}

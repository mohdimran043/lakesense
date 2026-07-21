package anomaly

import (
	"context"
	"fmt"
	"time"
)

// Series is one pipeline's ordered (oldest→newest) metric history.
type Series struct {
	PipelineID int64
	Values     []float64
}

// MetricsSource loads recent per-pipeline metric series to score.
type MetricsSource interface {
	// RecentSeries returns, per active pipeline, up to limit most-recent values
	// of the named metric in chronological order.
	RecentSeries(ctx context.Context, metric string, limit int) ([]Series, error)
}

// Emitter delivers a detected anomaly into the platform (as an anomaly_detected
// event through the collector, so it flows the same rules→incident→alert path).
type Emitter func(ctx context.Context, pipelineID int64, metric string, r Result) error

// Worker scores each active pipeline's latest metric against its own learned
// baseline on every Tick, emitting an anomaly when the latest point is an
// outlier. Cold-start suppression in Baseline keeps new pipelines quiet.
type Worker struct {
	source  MetricsSource
	emitter Emitter
	metric  string
	limit   int
	now     func() time.Time
}

// NewWorker builds an anomaly worker over a metric (e.g. "rows_written").
func NewWorker(source MetricsSource, emitter Emitter, metric string, now func() time.Time) *Worker {
	if metric == "" {
		metric = "rows_written"
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Worker{source: source, emitter: emitter, metric: metric, limit: 200, now: now}
}

// Tick scores every pipeline's latest observation. It learns the baseline from
// all but the latest value, then checks the latest — so a point is judged
// against its own history, not itself.
func (w *Worker) Tick(ctx context.Context) error {
	series, err := w.source.RecentSeries(ctx, w.metric, w.limit)
	if err != nil {
		return fmt.Errorf("load metric series: %w", err)
	}
	for _, s := range series {
		if len(s.Values) < 2 {
			continue // nothing to compare the latest point against
		}
		b := NewBaseline()
		for _, v := range s.Values[:len(s.Values)-1] {
			b.Observe(v)
		}
		latest := s.Values[len(s.Values)-1]
		res := b.Check(latest)
		if !res.Anomaly {
			continue
		}
		if err := w.emitter(ctx, s.PipelineID, w.metric, res); err != nil {
			return fmt.Errorf("emit anomaly for pipeline %d: %w", s.PipelineID, err)
		}
	}
	return nil
}

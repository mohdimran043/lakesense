// Package anomaly is LakeSense's zero-dependency, in-process anomaly detector.
// It watches per-pipeline metric streams (rows/sec, duration, volume, lag) and
// flags observations that deviate from a learned baseline, emitting
// anomaly_detected events into the same rule pipeline as everything else.
//
// Detection is robust-statistics based: a modified z-score over a rolling
// window's median and MAD (median absolute deviation), which — unlike a plain
// mean/stddev z-score — is not dragged around by the very outliers it is trying
// to catch. A Welford running mean/stddev is kept as a fallback for the
// degenerate MAD==0 case and for reporting the expected value.
package anomaly

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// Defaults tuned for pipeline metrics: enough history to be meaningful,
// a modified-z threshold that catches real shifts without crying at noise.
const (
	defaultMinSamples = 8
	defaultWindow     = 200
	defaultThreshold  = 3.5 // |modified z-score| above this ⇒ anomaly
	madScale          = 0.6745
)

// Result is the outcome of checking one observation.
type Result struct {
	Anomaly  bool
	Score    float64 // |modified z-score| (or z-score in the fallback)
	Method   string  // "mad" | "zscore" | "cold_start"
	Expected float64 // baseline central value
	Value    float64
}

// Baseline learns one metric's distribution and scores new observations.
type Baseline struct {
	window     []float64
	maxWindow  int
	minSamples int
	threshold  float64

	// Welford running stats (fallback + expected reporting).
	count int
	mean  float64
	m2    float64
}

// NewBaseline builds a baseline with the default configuration.
func NewBaseline() *Baseline {
	return &Baseline{maxWindow: defaultWindow, minSamples: defaultMinSamples, threshold: defaultThreshold}
}

// Observe folds a value into the baseline without scoring it.
func (b *Baseline) Observe(v float64) {
	b.count++
	delta := v - b.mean
	b.mean += delta / float64(b.count)
	b.m2 += delta * (v - b.mean)

	b.window = append(b.window, v)
	if len(b.window) > b.maxWindow {
		b.window = b.window[1:]
	}
}

// Check scores a value against the current baseline WITHOUT folding it in. Until
// minSamples observations exist it never flags (cold start), so a new pipeline
// does not alert on its own first runs.
func (b *Baseline) Check(v float64) Result {
	if b.count < b.minSamples {
		return Result{Method: "cold_start", Value: v, Expected: b.mean}
	}
	median, mad := medianMAD(b.window)
	if mad > 0 {
		score := math.Abs(madScale * (v - median) / mad)
		return Result{Anomaly: score > b.threshold, Score: score, Method: "mad", Expected: median, Value: v}
	}
	// Degenerate MAD (near-constant history): fall back to mean/stddev z-score.
	std := b.stddev()
	if std == 0 {
		return Result{Method: "zscore", Expected: b.mean, Value: v, Anomaly: v != b.mean, Score: 0}
	}
	score := math.Abs((v - b.mean) / std)
	return Result{Anomaly: score > b.threshold, Score: score, Method: "zscore", Expected: b.mean, Value: v}
}

func (b *Baseline) stddev() float64 {
	if b.count < 2 {
		return 0
	}
	return math.Sqrt(b.m2 / float64(b.count-1))
}

// medianMAD returns the median and median-absolute-deviation of xs.
func medianMAD(xs []float64) (median, mad float64) {
	if len(xs) == 0 {
		return 0, 0
	}
	median = medianOf(xs)
	dev := make([]float64, len(xs))
	for i, x := range xs {
		dev[i] = math.Abs(x - median)
	}
	mad = medianOf(dev)
	return median, mad
}

func medianOf(xs []float64) float64 {
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	n := len(s)
	if n%2 == 1 {
		return s[n/2]
	}
	return (s[n/2-1] + s[n/2]) / 2
}

// Model holds baselines keyed by an arbitrary series key (e.g.
// "pipeline:7|duration" or a weekday-hour seasonal bucket).
type Model struct {
	baselines map[string]*Baseline
}

// NewModel builds an empty model.
func NewModel() *Model { return &Model{baselines: map[string]*Baseline{}} }

// Check scores v for key against its baseline, then folds v in so the baseline
// keeps learning. Returns the result for alerting.
func (m *Model) Check(key string, v float64) Result {
	b := m.baselines[key]
	if b == nil {
		b = NewBaseline()
		m.baselines[key] = b
	}
	res := b.Check(v)
	b.Observe(v)
	return res
}

// SeasonalKey buckets a timestamp by weekday and hour so a Sunday-3am baseline
// is not compared against a Monday-9am peak.
func SeasonalKey(base string, ts time.Time) string {
	return fmt.Sprintf("%s|%d-%02d", base, int(ts.Weekday()), ts.Hour())
}

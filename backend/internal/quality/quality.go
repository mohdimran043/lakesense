// Package quality is LakeSense's data-quality monitor suite — the Monte-Carlo
// core given away free. Monitors run against per-column stats collected during
// sync and breach against learned baselines: freshness (is the data late?),
// volume (did the row count crater or balloon?), null-rate (did a column start
// coming back empty?), and distribution drift (did the shape of the data change?
// measured by Population Stability Index). Each evaluator is a pure function so
// the breach logic is exhaustively testable.
package quality

import (
	"fmt"
	"math"
	"time"
)

// Result is a monitor evaluation outcome.
type Result struct {
	Breached bool
	Value    float64 // the measured quantity (seconds late, drop fraction, null rate, PSI)
	Detail   string
}

// Freshness breaches when the data is older than the threshold.
func Freshness(sinceLastSync, threshold time.Duration) Result {
	late := sinceLastSync > threshold
	return Result{
		Breached: late,
		Value:    sinceLastSync.Seconds(),
		Detail:   fmt.Sprintf("last sync %s ago (threshold %s)", sinceLastSync.Truncate(time.Second), threshold),
	}
}

// Volume breaches when the current row count deviates from the baseline mean by
// more than maxDeviation (a fraction, e.g. 0.5 = ±50%). Catches both a collapse
// and an unexpected explosion.
func Volume(current, baselineMean, maxDeviation float64) Result {
	if baselineMean <= 0 {
		return Result{Value: current, Detail: "no baseline yet"}
	}
	dev := math.Abs(current-baselineMean) / baselineMean
	return Result{
		Breached: dev > maxDeviation,
		Value:    dev,
		Detail:   fmt.Sprintf("%.0f rows vs baseline %.0f (%.1f%% deviation, limit %.0f%%)", current, baselineMean, dev*100, maxDeviation*100),
	}
}

// NullRate breaches when a column's null fraction rises more than maxIncrease
// above its baseline null fraction (absolute, e.g. 0.1 = +10 points).
func NullRate(current, baseline, maxIncrease float64) Result {
	increase := current - baseline
	return Result{
		Breached: increase > maxIncrease,
		Value:    current,
		Detail:   fmt.Sprintf("null rate %.1f%% vs baseline %.1f%% (+%.1f pts, limit +%.1f pts)", current*100, baseline*100, increase*100, maxIncrease*100),
	}
}

// Distribution breaches when the Population Stability Index between the baseline
// and current histograms exceeds threshold (industry rule of thumb: <0.1 stable,
// 0.1–0.25 moderate shift, >0.25 significant). baseline and current are aligned
// bucket counts.
func Distribution(baseline, current []float64, threshold float64) Result {
	psi := PSI(baseline, current)
	return Result{
		Breached: psi > threshold,
		Value:    psi,
		Detail:   fmt.Sprintf("PSI %.3f (threshold %.3f)", psi, threshold),
	}
}

// PSI computes the Population Stability Index between two aligned histograms of
// bucket counts. Proportions are smoothed by a small epsilon so empty buckets do
// not blow up the logarithm. Mismatched lengths return 0 (not comparable).
func PSI(baseline, current []float64) float64 {
	if len(baseline) != len(current) || len(baseline) == 0 {
		return 0
	}
	const eps = 1e-6
	bTotal, cTotal := sum(baseline), sum(current)
	if bTotal == 0 || cTotal == 0 {
		return 0
	}
	var psi float64
	for i := range baseline {
		b := baseline[i]/bTotal + eps
		c := current[i]/cTotal + eps
		psi += (c - b) * math.Log(c/b)
	}
	return psi
}

func sum(xs []float64) float64 {
	var s float64
	for _, x := range xs {
		s += x
	}
	return s
}

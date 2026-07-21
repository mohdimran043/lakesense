package quality

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeStore struct {
	monitors []Monitor
	stat     Stat
	statOK   bool
	since    time.Duration
	sinceOK  bool
	results  []Result
}

func (f *fakeStore) Monitors(context.Context) ([]Monitor, error) { return f.monitors, nil }
func (f *fakeStore) LatestStat(context.Context, int64, string, string) (Stat, bool, error) {
	return f.stat, f.statOK, nil
}
func (f *fakeStore) SinceLastSync(context.Context, int64) (time.Duration, bool, error) {
	return f.since, f.sinceOK, nil
}
func (f *fakeStore) RecordResult(_ context.Context, _ int64, _ string, r Result, _ time.Time) error {
	f.results = append(f.results, r)
	return nil
}

func TestWorkerNullRateBreachEmits(t *testing.T) {
	store := &fakeStore{
		monitors: []Monitor{{
			ID: 1, PipelineID: 7, Stream: "public.a", Column: "email", Kind: "null_rate",
			Config: map[string]float64{"max_increase": 0.1}, Baseline: map[string]float64{"null_rate": 0.02},
		}},
		stat:   Stat{Rows: 100, Nulls: 40, SyncID: "s1"}, // 40% null vs 2% baseline, +38pts > 10pts
		statOK: true,
	}
	var emitted []Monitor
	emit := func(_ context.Context, _ int64, m Monitor, r Result) error {
		require.True(t, r.Breached)
		emitted = append(emitted, m)
		return nil
	}
	require.NoError(t, NewWorker(store, emit, nil).Tick(context.Background()))
	require.Len(t, emitted, 1, "null-rate spike trips the monitor")
	require.Len(t, store.results, 1, "the evaluation was recorded")
	require.True(t, store.results[0].Breached)
}

func TestWorkerWithinThresholdNoEmit(t *testing.T) {
	store := &fakeStore{
		monitors: []Monitor{{
			ID: 2, PipelineID: 7, Stream: "public.a", Column: "email", Kind: "null_rate",
			Config: map[string]float64{"max_increase": 0.1}, Baseline: map[string]float64{"null_rate": 0.02},
		}},
		stat:   Stat{Rows: 100, Nulls: 5, SyncID: "s1"}, // 5% vs 2% baseline, +3pts < 10pts
		statOK: true,
	}
	emitCount := 0
	emit := func(context.Context, int64, Monitor, Result) error { emitCount++; return nil }
	require.NoError(t, NewWorker(store, emit, nil).Tick(context.Background()))
	require.Zero(t, emitCount, "within threshold does not alert")
	require.Len(t, store.results, 1, "but the evaluation is still recorded")
}

func TestWorkerSkipsWhenNoData(t *testing.T) {
	store := &fakeStore{
		monitors: []Monitor{{ID: 3, Kind: "volume", Config: map[string]float64{"max_deviation": 0.5}}},
		statOK:   false, // no stats yet
	}
	require.NoError(t, NewWorker(store, nil, nil).Tick(context.Background()))
	require.Empty(t, store.results, "no stats → nothing evaluated")
}

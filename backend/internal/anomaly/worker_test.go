package anomaly

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeSource struct{ series []Series }

func (f fakeSource) RecentSeries(context.Context, string, int) ([]Series, error) {
	return f.series, nil
}

func TestWorkerEmitsOnSpikeOnly(t *testing.T) {
	// Pipeline 1: a stable history then a large spike as the latest value.
	// Pipeline 2: a stable history with a normal latest value.
	stable := []float64{100, 101, 99, 100, 102, 98, 101, 100, 99, 100, 101, 100}
	spiked := append(append([]float64(nil), stable...), 100000)
	normal := append(append([]float64(nil), stable...), 100)

	src := fakeSource{series: []Series{
		{PipelineID: 1, Values: spiked},
		{PipelineID: 2, Values: normal},
	}}
	var emitted []int64
	emit := func(_ context.Context, pipelineID int64, _ string, r Result) error {
		require.True(t, r.Anomaly)
		emitted = append(emitted, pipelineID)
		return nil
	}
	w := NewWorker(src, emit, "rows_written", nil)
	require.NoError(t, w.Tick(context.Background()))

	require.Equal(t, []int64{1}, emitted, "only the spiked pipeline emits an anomaly")
}

func TestWorkerSkipsShortSeries(t *testing.T) {
	src := fakeSource{series: []Series{{PipelineID: 1, Values: []float64{42}}}}
	emit := func(context.Context, int64, string, Result) error {
		t.Fatal("must not emit for a single-point series")
		return nil
	}
	require.NoError(t, NewWorker(src, emit, "rows_written", nil).Tick(context.Background()))
}

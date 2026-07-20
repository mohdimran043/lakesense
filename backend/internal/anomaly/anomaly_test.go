package anomaly

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColdStartNeverFlags(t *testing.T) {
	b := NewBaseline()
	for i := 0; i < defaultMinSamples-1; i++ {
		res := b.Check(1000)
		assert.Equal(t, "cold_start", res.Method)
		assert.False(t, res.Anomaly)
		b.Observe(100) // even wildly different values must not flag pre-warmup
	}
}

func TestStableDataProducesNoAnomalies(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	m := NewModel()
	flags := 0
	for i := 0; i < 500; i++ {
		v := 1000 + rng.NormFloat64()*20 // tight normal noise
		if m.Check("rows", v).Anomaly {
			flags++
		}
	}
	assert.LessOrEqual(t, flags, 5, "normal noise must not produce an alert storm (got %d)", flags)
}

func TestInjectedSpikeIsDetected(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	b := NewBaseline()
	for i := 0; i < 100; i++ {
		b.Observe(1000 + rng.NormFloat64()*15)
	}
	// A 10x spike and a near-zero drop are both clear anomalies.
	assert.True(t, b.Check(10000).Anomaly, "10x spike detected")
	assert.True(t, b.Check(5).Anomaly, "volume collapse detected")
	assert.False(t, b.Check(1005).Anomaly, "a normal value is not flagged")
}

func TestConstantHistoryFallsBackToZScore(t *testing.T) {
	b := NewBaseline()
	for i := 0; i < 50; i++ {
		b.Observe(42) // MAD == 0
	}
	res := b.Check(42)
	assert.False(t, res.Anomaly)
	res = b.Check(9999)
	assert.True(t, res.Anomaly, "any deviation from a constant baseline is anomalous")
}

func TestSeasonalKeySeparatesBuckets(t *testing.T) {
	mon9 := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC) // Monday 09:00
	sun3 := time.Date(2026, 1, 4, 3, 0, 0, 0, time.UTC) // Sunday 03:00
	require.NotEqual(t, SeasonalKey("rows", mon9), SeasonalKey("rows", sun3))
	assert.Equal(t, SeasonalKey("rows", mon9), SeasonalKey("rows", mon9.AddDate(0, 0, 7)))
}

func TestSeasonalModelDoesNotCrossContaminate(t *testing.T) {
	m := NewModel()
	peak := time.Date(2026, 1, 5, 9, 0, 0, 0, time.UTC)
	quiet := time.Date(2026, 1, 4, 3, 0, 0, 0, time.UTC)
	// Peak bucket learns ~1M rows; quiet bucket learns ~1k rows.
	for i := 0; i < 30; i++ {
		m.Check(SeasonalKey("rows", peak), 1_000_000+float64(i))
		m.Check(SeasonalKey("rows", quiet), 1_000+float64(i))
	}
	// 1k rows during the peak window is an anomaly; the same value in the quiet
	// window is perfectly normal.
	assert.True(t, m.Check(SeasonalKey("rows", peak), 1_000).Anomaly)
	assert.False(t, m.Check(SeasonalKey("rows", quiet), 1_010).Anomaly)
}

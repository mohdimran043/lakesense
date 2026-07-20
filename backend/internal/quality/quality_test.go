package quality

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFreshness(t *testing.T) {
	assert.True(t, Freshness(3*time.Hour, time.Hour).Breached, "3h old exceeds 1h SLA")
	assert.False(t, Freshness(30*time.Minute, time.Hour).Breached, "fresh within SLA")
}

func TestVolume(t *testing.T) {
	assert.False(t, Volume(1000, 1000, 0.5).Breached, "on baseline")
	assert.False(t, Volume(1200, 1000, 0.5).Breached, "within +50%")
	assert.True(t, Volume(300, 1000, 0.5).Breached, "70% collapse breaches")
	assert.True(t, Volume(2000, 1000, 0.5).Breached, "2x spike breaches")
	assert.False(t, Volume(500, 0, 0.5).Breached, "no baseline ⇒ no breach")
}

func TestNullRate(t *testing.T) {
	assert.False(t, NullRate(0.05, 0.03, 0.1).Breached, "small rise ok")
	assert.True(t, NullRate(0.40, 0.03, 0.1).Breached, "null spike breaches")
	assert.False(t, NullRate(0.01, 0.20, 0.1).Breached, "nulls dropping is not a breach")
}

func TestPSIStableVsDrift(t *testing.T) {
	base := []float64{100, 200, 300, 200, 100}
	same := []float64{102, 198, 300, 205, 95}
	assert.Less(t, PSI(base, same), 0.1, "near-identical distribution is stable")

	drift := []float64{300, 200, 100, 50, 10}
	assert.Greater(t, PSI(base, drift), 0.25, "a real shift shows high PSI")

	assert.Equal(t, 0.0, PSI(base, []float64{1, 2}), "mismatched shapes not comparable")
}

func TestDistributionMonitor(t *testing.T) {
	base := []float64{100, 200, 300, 200, 100}
	assert.False(t, Distribution(base, []float64{101, 199, 300, 201, 99}, 0.2).Breached)
	assert.True(t, Distribution(base, []float64{300, 200, 100, 50, 10}, 0.2).Breached)
}

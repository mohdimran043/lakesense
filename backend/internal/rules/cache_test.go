package rules

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingLoader struct {
	calls int
	rules []Rule
}

func (l *countingLoader) LoadRules(context.Context, int64) ([]Rule, error) {
	l.calls++
	return l.rules, nil
}

func TestCachedLoaderServesWithinTTLThenReloads(t *testing.T) {
	inner := &countingLoader{rules: []Rule{{ID: 1, Name: "r"}}}
	now := time.Unix(0, 0)
	c := NewCachedLoader(inner, 10*time.Second)
	c.now = func() time.Time { return now }

	// First load hits the inner loader; the next two within TTL are served cached.
	for i := 0; i < 3; i++ {
		r, err := c.LoadRules(context.Background(), 7)
		require.NoError(t, err)
		require.Len(t, r, 1)
	}
	require.Equal(t, 1, inner.calls, "cached within TTL")

	// A different pipeline is cached independently.
	_, _ = c.LoadRules(context.Background(), 8)
	require.Equal(t, 2, inner.calls)

	// After the TTL elapses, pipeline 7 reloads.
	now = now.Add(11 * time.Second)
	_, _ = c.LoadRules(context.Background(), 7)
	require.Equal(t, 3, inner.calls)
}

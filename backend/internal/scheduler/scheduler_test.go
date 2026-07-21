package scheduler

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func tp(s string) *time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return &t
}

func TestIsDue(t *testing.T) {
	now := mustTime("2026-07-21T12:00:00Z")
	tests := []struct {
		name     string
		cron     string
		lastSync *time.Time
		want     bool
	}{
		{"manual never runs", "", nil, false},
		{"never-synced hourly is due", "@hourly", nil, true},
		{"hourly recently synced not due", "@hourly", tp("2026-07-21T11:30:00Z"), false},
		{"hourly stale is due", "@hourly", tp("2026-07-21T10:30:00Z"), true},
		{"daily within a day not due", "@daily", tp("2026-07-21T01:00:00Z"), false},
		{"daily over a day due", "@daily", tp("2026-07-20T01:00:00Z"), true},
		{"@every 5m due", "@every 5m", tp("2026-07-21T11:50:00Z"), true},
		{"@every 5m not due", "@every 5m", tp("2026-07-21T11:58:00Z"), false},
		{"unparseable never runs", "0 0 * * *", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isDue(tt.cron, tt.lastSync, now))
		})
	}
}

type fakeLister struct{ scheds []Schedule }

func (f fakeLister) Active(context.Context) ([]Schedule, error) { return f.scheds, nil }

func TestTickTriggersOnlyDuePipelines(t *testing.T) {
	now := mustTime("2026-07-21T12:00:00Z")
	lister := fakeLister{scheds: []Schedule{
		{PipelineID: 1, Cron: "@hourly", LastSync: tp("2026-07-21T10:00:00Z")}, // due
		{PipelineID: 2, Cron: "@hourly", LastSync: tp("2026-07-21T11:59:00Z")}, // not due
		{PipelineID: 3, Cron: "@daily", LastSync: nil},                         // due (never synced)
	}}
	var triggered []int64
	s := New(lister, func(id int64) { triggered = append(triggered, id) }, time.Minute,
		func() time.Time { return now }, slog.Default())

	s.tick(context.Background())
	require.ElementsMatch(t, []int64{1, 3}, triggered)
}

func mustTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

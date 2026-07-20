package rules

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/collector"
)

// --- fakes ---

type fakeStore struct {
	open    map[string]*Incident
	created []*Incident
	bumps   int
	alerts  int
	nextID  int64
}

func newFakeStore() *fakeStore { return &fakeStore{open: map[string]*Incident{}, nextID: 1} }

func (f *fakeStore) FindOpenIncident(_ context.Context, fp string) (*Incident, error) {
	return f.open[fp], nil
}
func (f *fakeStore) CreateIncident(_ context.Context, inc *Incident) (int64, error) {
	id := f.nextID
	f.nextID++
	inc.ID = id
	cp := *inc
	f.created = append(f.created, &cp)
	f.open[inc.Fingerprint] = &cp
	return id, nil
}
func (f *fakeStore) BumpIncident(_ context.Context, _ int64, _ time.Time) error {
	f.bumps++
	return nil
}
func (f *fakeStore) RecordAlert(_ context.Context, _, _, _ int64, _ bool, _ string) error {
	f.alerts++
	return nil
}

type fakeNotifier struct{ sent []Notification }

func (f *fakeNotifier) Send(_ context.Context, n Notification) error {
	f.sent = append(f.sent, n)
	return nil
}

func ev(kind, stream string, payload map[string]any) collector.Event {
	raw, _ := json.Marshal(payload)
	return collector.Event{V: 1, TS: time.Now(), Kind: kind, Stream: stream, Payload: raw}
}

func failRule() Rule {
	return Rule{
		ID: 1, Name: "Sync failures", Severity: SeverityCritical,
		Condition:  Condition{Event: "sync_failed"},
		ChannelIDs: []int64{10}, Enabled: true, DedupWindow: 5 * time.Minute,
	}
}

// --- tests ---

func TestNewFailureOpensIncidentAndAlerts(t *testing.T) {
	st, nf := newFakeStore(), &fakeNotifier{}
	eng := NewEngine(st, nf, fixedNow())

	err := eng.Evaluate(context.Background(), 7, ev("sync_failed", "public.orders", map[string]any{"code": "boom"}), []Rule{failRule()})
	require.NoError(t, err)

	require.Len(t, st.created, 1)
	assert.Equal(t, SeverityCritical, st.created[0].Severity)
	assert.Equal(t, "open", st.created[0].Status)
	require.Len(t, nf.sent, 1, "one alert to the one channel")
	assert.Equal(t, int64(10), nf.sent[0].ChannelID)
	assert.Equal(t, 1, st.alerts)
}

func TestRepeatWhileOpenDedupsWithoutNewAlert(t *testing.T) {
	st, nf := newFakeStore(), &fakeNotifier{}
	eng := NewEngine(st, nf, fixedNow())
	e := ev("sync_failed", "public.orders", map[string]any{"code": "boom"})

	require.NoError(t, eng.Evaluate(context.Background(), 7, e, []Rule{failRule()}))
	require.NoError(t, eng.Evaluate(context.Background(), 7, e, []Rule{failRule()}))
	require.NoError(t, eng.Evaluate(context.Background(), 7, e, []Rule{failRule()}))

	assert.Len(t, st.created, 1, "one incident for the storm")
	assert.Equal(t, 2, st.bumps, "subsequent matches bump, not re-open")
	assert.Len(t, nf.sent, 1, "one alert thread")
}

func TestThresholdCondition(t *testing.T) {
	st, nf := newFakeStore(), &fakeNotifier{}
	eng := NewEngine(st, nf, fixedNow())
	slow := Rule{
		ID: 2, Name: "Slow sync", Severity: SeverityWarning, Enabled: true,
		Condition:  Condition{Event: "sync_finished", Field: "duration_seconds", Op: "gt", Value: 120.0},
		ChannelIDs: []int64{1},
	}

	require.NoError(t, eng.Evaluate(context.Background(), 1, ev("sync_finished", "", map[string]any{"duration_seconds": 45.0}), []Rule{slow}))
	assert.Empty(t, st.created, "fast sync does not fire")

	require.NoError(t, eng.Evaluate(context.Background(), 1, ev("sync_finished", "", map[string]any{"duration_seconds": 300.0}), []Rule{slow}))
	assert.Len(t, st.created, 1, "slow sync fires")
}

func TestBooleanFalseCondition(t *testing.T) {
	st, nf := newFakeStore(), &fakeNotifier{}
	eng := NewEngine(st, nf, fixedNow())
	mismatch := Rule{
		ID: 3, Name: "Diff mismatch", Severity: SeverityCritical, Enabled: true,
		Condition:  Condition{Event: "diff_mismatch", Field: "match", Op: "is_false"},
		ChannelIDs: []int64{1},
	}
	require.NoError(t, eng.Evaluate(context.Background(), 1, ev("diff_mismatch", "public.a", map[string]any{"match": false}), []Rule{mismatch}))
	assert.Len(t, st.created, 1)
}

func TestQuietHoursMuteDeliveryButTrackIncident(t *testing.T) {
	st, nf := newFakeStore(), &fakeNotifier{}
	// 02:00 UTC, quiet 00:00–06:00.
	at := time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)
	eng := NewEngine(st, nf, func() time.Time { return at })
	r := failRule()
	r.QuietHours = QuietHours{TZ: "UTC", Ranges: []TimeRange{{Start: "00:00", End: "06:00"}}}

	require.NoError(t, eng.Evaluate(context.Background(), 7, ev("sync_failed", "s", nil), []Rule{r}))
	assert.Len(t, st.created, 1, "incident still tracked")
	assert.Empty(t, nf.sent, "delivery muted during quiet hours")
}

func TestMaintenanceWindowFullyMutes(t *testing.T) {
	st, nf := newFakeStore(), &fakeNotifier{}
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	eng := NewEngine(st, nf, func() time.Time { return at })
	r := failRule()
	r.MaintenanceUntil = at.Add(time.Hour)

	require.NoError(t, eng.Evaluate(context.Background(), 7, ev("sync_failed", "s", nil), []Rule{r}))
	assert.Empty(t, st.created, "no incident during maintenance")
	assert.Empty(t, nf.sent)
}

func TestRuleScoping(t *testing.T) {
	r := failRule()
	r.PipelineID = 5
	r.Stream = "public.orders"

	assert.True(t, r.Matches(5, ev("sync_failed", "public.orders", nil)))
	assert.False(t, r.Matches(9, ev("sync_failed", "public.orders", nil)), "wrong pipeline")
	assert.False(t, r.Matches(5, ev("sync_failed", "public.customers", nil)), "wrong stream")
	assert.False(t, r.Matches(5, ev("sync_finished", "public.orders", nil)), "wrong kind")
}

func fixedNow() func() time.Time {
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	return func() time.Time { return at }
}

package escalation

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/rules"
)

// --- fakes ---

type fakeStore struct {
	due       []EscalatingIncident
	advanced  []advance
	dueByTime func(now time.Time) []EscalatingIncident
}
type advance struct {
	id       int64
	nextStep int
	nextAt   *time.Time
}

func (f *fakeStore) DueIncidents(_ context.Context, now time.Time) ([]EscalatingIncident, error) {
	if f.dueByTime != nil {
		return f.dueByTime(now), nil
	}
	return f.due, nil
}
func (f *fakeStore) AdvanceEscalation(_ context.Context, id int64, nextStep int, nextAt *time.Time) error {
	f.advanced = append(f.advanced, advance{id, nextStep, nextAt})
	return nil
}

type fakePolicies map[int64]Policy

func (f fakePolicies) Policy(_ context.Context, id int64) (Policy, error) { return f[id], nil }

type fakeSchedules map[int64]OnCall

func (f fakeSchedules) Schedule(_ context.Context, id int64) (OnCall, error) { return f[id], nil }

type fakeNotifier struct{ sent []rules.Notification }

func (f *fakeNotifier) Send(_ context.Context, n rules.Notification) error {
	f.sent = append(f.sent, n)
	return nil
}

// --- tests ---

func twoStepPolicy() Policy {
	return Policy{ID: 1, Name: "default", Steps: []Step{
		{After: 5 * time.Minute, ChannelIDs: []int64{100}},
		{After: 10 * time.Minute, ChannelIDs: []int64{200}, OnCallScheduleID: 9},
	}}
}

func TestEscalatesStepThenSchedulesNext(t *testing.T) {
	now := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{due: []EscalatingIncident{{ID: 1, PolicyID: 1, Step: 0, Title: "orders down", Severity: rules.SeverityCritical}}}
	nf := &fakeNotifier{}
	w := NewWorker(st, fakePolicies{1: twoStepPolicy()}, fakeSchedules{}, nf, func() time.Time { return now })

	require.NoError(t, w.Tick(context.Background()))

	require.Len(t, nf.sent, 1, "step 1 notifies its channel")
	assert.Equal(t, int64(100), nf.sent[0].ChannelID)
	assert.Contains(t, nf.sent[0].Title, "escalation step 1")

	require.Len(t, st.advanced, 1)
	assert.Equal(t, 1, st.advanced[0].nextStep)
	require.NotNil(t, st.advanced[0].nextAt, "next step scheduled")
	assert.Equal(t, now.Add(10*time.Minute), *st.advanced[0].nextAt)
}

func TestSecondStepAddsOnCallAndExhausts(t *testing.T) {
	now := time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)
	st := &fakeStore{due: []EscalatingIncident{{ID: 1, PolicyID: 1, Step: 1, Title: "orders down", Severity: rules.SeverityCritical}}}
	nf := &fakeNotifier{}
	sched := fakeSchedules{9: {ID: 9, Responders: []Responder{{Name: "A", ChannelIDs: []int64{300}}}}}
	w := NewWorker(st, fakePolicies{1: twoStepPolicy()}, sched, nf, func() time.Time { return now })

	require.NoError(t, w.Tick(context.Background()))

	// step channel 200 + on-call channel 300
	require.Len(t, nf.sent, 2)
	chans := map[int64]bool{nf.sent[0].ChannelID: true, nf.sent[1].ChannelID: true}
	assert.True(t, chans[200] && chans[300])

	require.Len(t, st.advanced, 1)
	assert.Equal(t, 2, st.advanced[0].nextStep)
	assert.Nil(t, st.advanced[0].nextAt, "policy exhausted ⇒ no further escalation")
}

func TestAckedIncidentNotEscalated(t *testing.T) {
	// The store only returns unacked, open, due incidents. An empty due list
	// (as when the incident was acked) yields no notifications.
	st := &fakeStore{due: nil}
	nf := &fakeNotifier{}
	w := NewWorker(st, fakePolicies{1: twoStepPolicy()}, fakeSchedules{}, nf, func() time.Time { return time.Now() })
	require.NoError(t, w.Tick(context.Background()))
	assert.Empty(t, nf.sent)
	assert.Empty(t, st.advanced)
}

func TestOnCallRotationAndOverride(t *testing.T) {
	responders := []Responder{{Name: "A", ChannelIDs: []int64{1}}, {Name: "B", ChannelIDs: []int64{2}}}
	oc := OnCall{Responders: responders}

	// Two different ISO weeks pick different responders (deterministic mod).
	w1 := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)  // ISO week 2
	w2 := time.Date(2026, 1, 12, 0, 0, 0, 0, time.UTC) // ISO week 3
	assert.NotEqual(t, oc.Current(w1).Name, oc.Current(w2).Name)

	// An active override wins over the rotation.
	oc.Overrides = []Override{{
		Start: w1.Add(-time.Hour), End: w1.Add(time.Hour),
		Responder: Responder{Name: "OVR", ChannelIDs: []int64{99}},
	}}
	assert.Equal(t, "OVR", oc.Current(w1).Name)
}

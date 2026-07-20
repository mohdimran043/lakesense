package audit

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeRecorder struct{ entries []Entry }

func (f *fakeRecorder) Record(_ context.Context, e Entry) error {
	f.entries = append(f.entries, e)
	return nil
}

func TestLogRecordsBeforeAfter(t *testing.T) {
	f := &fakeRecorder{}
	before := map[string]any{"schedule": "@daily", "status": "active"}
	after := map[string]any{"schedule": "@hourly", "status": "active"}
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	err := Log(context.Background(), f, "alice", "pipeline.update", "pipeline", "7", before, after, at)
	require.NoError(t, err)
	require.Len(t, f.entries, 1)
	e := f.entries[0]
	assert.Equal(t, "alice", e.Actor)
	assert.Equal(t, "pipeline.update", e.Action)
	assert.Equal(t, "7", e.EntityID)
	assert.JSONEq(t, `{"schedule":"@daily","status":"active"}`, string(e.Before))
	assert.JSONEq(t, `{"schedule":"@hourly","status":"active"}`, string(e.After))
}

func TestLogHandlesNilForCreateAndDelete(t *testing.T) {
	f := &fakeRecorder{}
	require.NoError(t, Log(context.Background(), f, "bob", "rule.create", "rule", "1", nil, map[string]any{"name": "x"}, time.Now()))
	assert.Equal(t, json.RawMessage("{}"), f.entries[0].Before)
	assert.JSONEq(t, `{"name":"x"}`, string(f.entries[0].After))
}

func TestDiffFindsChangedFields(t *testing.T) {
	before := json.RawMessage(`{"schedule":"@daily","status":"active","enabled":true}`)
	after := json.RawMessage(`{"schedule":"@hourly","status":"active","note":"faster"}`)

	changes := Diff(before, after)
	byField := map[string]FieldChange{}
	for _, c := range changes {
		byField[c.Field] = c
	}

	// schedule changed
	assert.Equal(t, "@daily", byField["schedule"].From)
	assert.Equal(t, "@hourly", byField["schedule"].To)
	// enabled removed
	assert.Equal(t, true, byField["enabled"].From)
	assert.Nil(t, byField["enabled"].To)
	// note added
	assert.Nil(t, byField["note"].From)
	assert.Equal(t, "faster", byField["note"].To)
	// status unchanged → not present
	_, ok := byField["status"]
	assert.False(t, ok)
}

func TestDiffOrderIndependent(t *testing.T) {
	a := json.RawMessage(`{"a":1,"b":2}`)
	b := json.RawMessage(`{"b":2,"a":1}`)
	assert.Empty(t, Diff(a, b), "reordered identical objects have no changes")
}

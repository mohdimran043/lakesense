// Package audit is the append-only audit log: every config change, rule edit,
// manual sync/backfill, ack, and setting change is recorded with actor,
// timestamp, and a before/after diff. The enterprise-tier feature everyone
// paywalls — free here. The Recorder is the consumer-side seam; a PgRecorder
// persists, and handlers call Record(...) after a successful mutation.
package audit

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

// Entry is one audited action.
type Entry struct {
	Actor      string          `json:"actor"`
	Action     string          `json:"action"`      // e.g. "pipeline.update", "incident.ack"
	EntityType string          `json:"entity_type"` // e.g. "pipeline", "rule"
	EntityID   string          `json:"entity_id"`
	Before     json.RawMessage `json:"before"`
	After      json.RawMessage `json:"after"`
	At         time.Time       `json:"at"`
}

// FieldChange is one differing field between before and after.
type FieldChange struct {
	Field string `json:"field"`
	From  any    `json:"from"`
	To    any    `json:"to"`
}

// Recorder persists audit entries. Append-only: there is no update or delete.
type Recorder interface {
	Record(ctx context.Context, e Entry) error
}

// Log is the high-level helper handlers call: it marshals before/after and
// records the entry. before/after may be any JSON-serializable value (nil for
// creates/deletes).
func Log(ctx context.Context, r Recorder, actor, action, entityType, entityID string, before, after any, now time.Time) error {
	e := Entry{
		Actor:      actor,
		Action:     action,
		EntityType: entityType,
		EntityID:   entityID,
		Before:     mustJSON(before),
		After:      mustJSON(after),
		At:         now,
	}
	return r.Record(ctx, e)
}

// Diff computes the field-level changes between two JSON objects — the
// before/after view the audit UI renders. Only top-level fields are compared;
// values are compared by their JSON encoding so order-independent maps match.
func Diff(before, after json.RawMessage) []FieldChange {
	b := decodeObject(before)
	a := decodeObject(after)

	keys := map[string]struct{}{}
	for k := range b {
		keys[k] = struct{}{}
	}
	for k := range a {
		keys[k] = struct{}{}
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var out []FieldChange
	for _, k := range sorted {
		bv, bok := b[k]
		av, aok := a[k]
		if bok && aok && jsonEqual(bv, av) {
			continue // unchanged
		}
		if !bok && !aok {
			continue
		}
		out = append(out, FieldChange{Field: k, From: valueOrNil(bok, bv), To: valueOrNil(aok, av)})
	}
	return out
}

func decodeObject(raw json.RawMessage) map[string]json.RawMessage {
	if len(raw) == 0 {
		return map[string]json.RawMessage{}
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]json.RawMessage{}
	}
	return m
}

func jsonEqual(a, b json.RawMessage) bool {
	var av, bv any
	if json.Unmarshal(a, &av) != nil || json.Unmarshal(b, &bv) != nil {
		return string(a) == string(b)
	}
	ab, _ := json.Marshal(av)
	bb, _ := json.Marshal(bv)
	return string(ab) == string(bb)
}

func valueOrNil(present bool, raw json.RawMessage) any {
	if !present {
		return nil
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return string(raw)
	}
	return v
}

func mustJSON(v any) json.RawMessage {
	if v == nil {
		return json.RawMessage("{}")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("{}")
	}
	return b
}

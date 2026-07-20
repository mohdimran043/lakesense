// Package rules is the notification rule engine: it evaluates the event stream
// against per-pipeline/per-stream rules and opens deduplicated incidents,
// enqueuing alerts through the consumer-side Notifier interface (implemented by
// the channel adapters in package channels). Everything the platform alerts on
// — engine failures, anomalies, quality breaches, diff mismatches — arrives
// here as an event, so there is one alerting path, not many.
package rules

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lakesense/lakesense/backend/internal/collector"
)

// Severity is an alert level.
type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Condition is a predicate over an event. Event (when set) must equal the event
// kind; when Field is set, the payload value at Field is compared to Value with
// Op. This covers "alert on sync_failed", "alert when duration_seconds > 120",
// and "alert when match is false".
type Condition struct {
	Event string `json:"event"`
	Field string `json:"field,omitempty"`
	Op    string `json:"op,omitempty"` // eq ne gt gte lt lte contains exists is_true is_false
	Value any    `json:"value,omitempty"`
}

// Rule binds a condition to a severity and delivery targets.
type Rule struct {
	ID                 int64
	PipelineID         int64 // 0 = global
	Stream             string
	Name               string
	Condition          Condition
	Severity           Severity
	ChannelIDs         []int64
	EscalationPolicyID int64
	Enabled            bool
	DedupWindow        time.Duration
	QuietHours         QuietHours
	MaintenanceUntil   time.Time
}

// Matches reports whether the rule fires for an event on a given pipeline.
func (r Rule) Matches(pipelineID int64, e collector.Event) bool {
	if !r.Enabled {
		return false
	}
	if r.PipelineID != 0 && r.PipelineID != pipelineID {
		return false
	}
	if r.Stream != "" && r.Stream != e.Stream {
		return false
	}
	return r.Condition.match(e)
}

// match evaluates the predicate against the event.
func (c Condition) match(e collector.Event) bool {
	if c.Event != "" && c.Event != e.Kind {
		return false
	}
	if c.Field == "" {
		return true // kind match is sufficient
	}
	got, ok := payloadField(e.Payload, c.Field)
	switch c.Op {
	case "exists":
		return ok
	case "is_true":
		b, _ := got.(bool)
		return ok && b
	case "is_false":
		b, isBool := got.(bool)
		return ok && isBool && !b
	}
	if !ok {
		return false
	}
	return compare(got, c.Op, c.Value)
}

// payloadField extracts a top-level payload field by name.
func payloadField(raw json.RawMessage, field string) (any, bool) {
	if len(raw) == 0 {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, false
	}
	v, ok := m[field]
	return v, ok
}

// compare applies a comparison operator, coercing numerics to float64.
func compare(got any, op string, want any) bool {
	switch op {
	case "eq", "":
		return fmt.Sprint(got) == fmt.Sprint(want)
	case "ne":
		return fmt.Sprint(got) != fmt.Sprint(want)
	case "contains":
		return strings.Contains(fmt.Sprint(got), fmt.Sprint(want))
	}
	gf, gok := toFloat(got)
	wf, wok := toFloat(want)
	if !gok || !wok {
		return false
	}
	switch op {
	case "gt":
		return gf > wf
	case "gte":
		return gf >= wf
	case "lt":
		return gf < wf
	case "lte":
		return gf <= wf
	default:
		return false
	}
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

// QuietHours mutes alert delivery (not incident tracking) during recurring
// daily windows in a timezone.
type QuietHours struct {
	TZ     string      `json:"tz,omitempty"`
	Ranges []TimeRange `json:"ranges,omitempty"`
}

// TimeRange is an "HH:MM"–"HH:MM" daily window (may wrap past midnight).
type TimeRange struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// Active reports whether now falls in any quiet-hours range.
func (q QuietHours) Active(now time.Time) bool {
	if len(q.Ranges) == 0 {
		return false
	}
	loc := time.UTC
	if q.TZ != "" {
		if l, err := time.LoadLocation(q.TZ); err == nil {
			loc = l
		}
	}
	local := now.In(loc)
	minutes := local.Hour()*60 + local.Minute()
	for _, r := range q.Ranges {
		s, ok1 := parseHM(r.Start)
		e, ok2 := parseHM(r.End)
		if !ok1 || !ok2 {
			continue
		}
		if s <= e {
			if minutes >= s && minutes < e {
				return true
			}
		} else { // wraps midnight, e.g. 22:00–06:00
			if minutes >= s || minutes < e {
				return true
			}
		}
	}
	return false
}

func parseHM(s string) (int, bool) {
	var h, m int
	if _, err := fmt.Sscanf(s, "%d:%d", &h, &m); err != nil {
		return 0, false
	}
	if h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}

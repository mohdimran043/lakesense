package rules

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/lakesense/lakesense/backend/internal/collector"
)

// Incident is one deduplicated alert thread.
type Incident struct {
	ID          int64
	PipelineID  int64
	Title       string
	Severity    Severity
	Status      string // open | acked | snoozed | resolved
	Fingerprint string
	EventCount  int
	Summary     string
	OpenedAt    time.Time
	LastEventAt time.Time
	RuleID      int64
	PolicyID    int64
	Stream      string
}

// Notification is one alert to deliver to one channel.
type Notification struct {
	Incident  *Incident
	Rule      Rule
	ChannelID int64
	Severity  Severity
	Title     string
	Body      string
}

// Notifier delivers a notification to a channel. Defined here (consumer side);
// implemented by package channels. The engine never knows channel specifics.
type Notifier interface {
	Send(ctx context.Context, n Notification) error
}

// Store persists incidents and alerts. FindOpenIncident returns nil when no
// open incident shares the fingerprint (the dedup key).
type Store interface {
	FindOpenIncident(ctx context.Context, fingerprint string) (*Incident, error)
	CreateIncident(ctx context.Context, inc *Incident) (int64, error)
	BumpIncident(ctx context.Context, id int64, at time.Time) error
	RecordAlert(ctx context.Context, incidentID, ruleID, channelID int64, sent bool, errMsg string) error
}

// Engine evaluates events against rules and manages incidents.
type Engine struct {
	store    Store
	notifier Notifier
	now      func() time.Time
}

// NewEngine builds a rule engine. now is injectable for deterministic tests.
func NewEngine(store Store, notifier Notifier, now func() time.Time) *Engine {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Engine{store: store, notifier: notifier, now: now}
}

// Evaluate runs one event against a rule set. A matching rule either opens a new
// incident (and dispatches alerts, unless muted by quiet hours) or, when an
// incident already exists for its fingerprint, bumps that incident without a
// new alert — one incident is one alert thread.
func (e *Engine) Evaluate(ctx context.Context, pipelineID int64, ev collector.Event, ruleSet []Rule) error {
	now := e.now()
	for _, r := range ruleSet {
		if !r.Matches(pipelineID, ev) {
			continue
		}
		if !r.MaintenanceUntil.IsZero() && now.Before(r.MaintenanceUntil) {
			continue // maintenance window: fully muted
		}
		fp := fingerprint(pipelineID, r.ID, ev.Stream, ev.Kind)

		existing, err := e.store.FindOpenIncident(ctx, fp)
		if err != nil {
			return fmt.Errorf("find open incident: %w", err)
		}
		if existing != nil {
			if err := e.store.BumpIncident(ctx, existing.ID, now); err != nil {
				return fmt.Errorf("bump incident: %w", err)
			}
			continue // deduped: no new alert while the thread is open
		}

		inc := &Incident{
			PipelineID:  pipelineID,
			Title:       title(r, ev),
			Severity:    r.Severity,
			Status:      "open",
			Fingerprint: fp,
			EventCount:  1,
			Summary:     summary(ev),
			OpenedAt:    now,
			LastEventAt: now,
			RuleID:      r.ID,
			PolicyID:    r.EscalationPolicyID,
			Stream:      ev.Stream,
		}
		id, err := e.store.CreateIncident(ctx, inc)
		if err != nil {
			return fmt.Errorf("create incident: %w", err)
		}
		inc.ID = id

		// Quiet hours mute delivery but not incident tracking.
		if r.QuietHours.Active(now) {
			continue
		}
		if err := e.dispatch(ctx, r, inc); err != nil {
			return err
		}
	}
	return nil
}

// dispatch sends the incident to each of the rule's channels, recording each
// attempt. A single channel failure does not abort the others.
func (e *Engine) dispatch(ctx context.Context, r Rule, inc *Incident) error {
	if e.notifier == nil {
		return nil
	}
	for _, ch := range r.ChannelIDs {
		n := Notification{
			Incident: inc, Rule: r, ChannelID: ch,
			Severity: r.Severity, Title: inc.Title, Body: inc.Summary,
		}
		sendErr := e.notifier.Send(ctx, n)
		msg := ""
		if sendErr != nil {
			msg = sendErr.Error()
		}
		if err := e.store.RecordAlert(ctx, inc.ID, r.ID, ch, sendErr == nil, msg); err != nil {
			return fmt.Errorf("record alert: %w", err)
		}
	}
	return nil
}

// fingerprint is the dedup key: one open incident per (pipeline, rule, stream,
// kind). Stable across repeated matches so a storm collapses to one thread.
func fingerprint(pipelineID, ruleID int64, stream, kind string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%d|%s|%s", pipelineID, ruleID, stream, kind)))
	return hex.EncodeToString(sum[:16])
}

func title(r Rule, ev collector.Event) string {
	if ev.Stream != "" {
		return fmt.Sprintf("%s: %s on %s", r.Name, ev.Kind, ev.Stream)
	}
	return fmt.Sprintf("%s: %s", r.Name, ev.Kind)
}

func summary(ev collector.Event) string {
	if len(ev.Payload) == 0 {
		return ev.Kind
	}
	return fmt.Sprintf("%s — %s", ev.Kind, string(ev.Payload))
}

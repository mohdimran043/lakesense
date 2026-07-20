// Package escalation drives incidents up an ordered policy until they are
// acknowledged: notify step 1's targets, wait N minutes, and if still unacked
// notify step 2, and so on — the PagerDuty-style behaviour, given away free.
// The worker is a pure state machine over a Store and an injectable clock, so
// the whole escalation lifecycle is deterministically testable.
package escalation

import (
	"context"
	"fmt"
	"time"

	"github.com/lakesense/lakesense/backend/internal/rules"
)

// Step is one rung of an escalation policy.
type Step struct {
	After            time.Duration `json:"after"`
	ChannelIDs       []int64       `json:"channel_ids"`
	OnCallScheduleID int64         `json:"oncall_schedule_id,omitempty"`
}

// Policy is an ordered list of steps.
type Policy struct {
	ID    int64
	Name  string
	Steps []Step
}

// EscalatingIncident is the worker's view of an incident due for escalation.
type EscalatingIncident struct {
	ID         int64
	PipelineID int64
	PolicyID   int64
	Step       int
	Title      string
	Summary    string
	Severity   rules.Severity
}

// Store surfaces due incidents and persists escalation advancement.
type Store interface {
	// DueIncidents returns open, unacked incidents whose next_escalation_at is
	// at or before now and that have an escalation policy.
	DueIncidents(ctx context.Context, now time.Time) ([]EscalatingIncident, error)
	// AdvanceEscalation records the next step and when it is next due (nil when
	// the policy is exhausted, which stops further escalation).
	AdvanceEscalation(ctx context.Context, incidentID int64, nextStep int, nextAt *time.Time) error
}

// Policies and Schedules load escalation configuration.
type Policies interface {
	Policy(ctx context.Context, id int64) (Policy, error)
}
type Schedules interface {
	Schedule(ctx context.Context, id int64) (OnCall, error)
}

// Worker processes due escalations on each Tick.
type Worker struct {
	store     Store
	policies  Policies
	schedules Schedules
	notifier  rules.Notifier
	now       func() time.Time
}

// NewWorker builds an escalation worker.
func NewWorker(store Store, policies Policies, schedules Schedules, notifier rules.Notifier, now func() time.Time) *Worker {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Worker{store: store, policies: policies, schedules: schedules, notifier: notifier, now: now}
}

// Tick processes every incident currently due for escalation. It is safe to
// call on a ticker; a run with nothing due is a no-op.
func (w *Worker) Tick(ctx context.Context) error {
	now := w.now()
	due, err := w.store.DueIncidents(ctx, now)
	if err != nil {
		return fmt.Errorf("load due incidents: %w", err)
	}
	for _, inc := range due {
		if err := w.escalate(ctx, inc, now); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) escalate(ctx context.Context, inc EscalatingIncident, now time.Time) error {
	policy, err := w.policies.Policy(ctx, inc.PolicyID)
	if err != nil {
		return fmt.Errorf("load policy %d: %w", inc.PolicyID, err)
	}
	// Exhausted: nothing left to do; clear the due time so it stops recurring.
	if inc.Step >= len(policy.Steps) {
		return w.store.AdvanceEscalation(ctx, inc.ID, inc.Step, nil)
	}

	step := policy.Steps[inc.Step]
	targets := append([]int64(nil), step.ChannelIDs...)
	if step.OnCallScheduleID != 0 {
		sched, err := w.schedules.Schedule(ctx, step.OnCallScheduleID)
		if err != nil {
			return fmt.Errorf("load schedule %d: %w", step.OnCallScheduleID, err)
		}
		targets = append(targets, sched.Current(now).ChannelIDs...)
	}

	incident := &rules.Incident{ID: inc.ID, PipelineID: inc.PipelineID, Title: inc.Title, Severity: inc.Severity}
	for _, ch := range targets {
		note := rules.Notification{
			Incident: incident, ChannelID: ch, Severity: inc.Severity,
			Title: fmt.Sprintf("[escalation step %d] %s", inc.Step+1, inc.Title),
			Body:  inc.Summary,
		}
		if err := w.notifier.Send(ctx, note); err != nil {
			// One channel failing must not stall the escalation state machine;
			// record-and-continue is handled by the notifier layer. Here we
			// keep advancing so a bad channel can't freeze the incident.
			continue
		}
	}

	nextStep := inc.Step + 1
	var nextAt *time.Time
	if nextStep < len(policy.Steps) {
		t := now.Add(policy.Steps[nextStep].After)
		nextAt = &t
	}
	return w.store.AdvanceEscalation(ctx, inc.ID, nextStep, nextAt)
}

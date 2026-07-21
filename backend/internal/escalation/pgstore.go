package escalation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lakesense/lakesense/backend/internal/rules"
)

// PgStore implements Store: due-incident discovery and escalation advancement
// over the incidents table.
type PgStore struct{ pool *pgxpool.Pool }

// NewPgStore builds a Postgres-backed escalation Store.
func NewPgStore(pool *pgxpool.Pool) *PgStore { return &PgStore{pool: pool} }

// DueIncidents returns open, unacked incidents whose next_escalation_at has
// arrived and that carry an escalation policy. Acked/snoozed/resolved incidents
// are excluded, so acknowledging stops escalation.
func (s *PgStore) DueIncidents(ctx context.Context, now time.Time) ([]EscalatingIncident, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, COALESCE(pipeline_id,0), escalation_policy_id, escalation_step,
		        title, summary, severity
		 FROM incidents
		 WHERE status = 'open'
		   AND escalation_policy_id IS NOT NULL
		   AND next_escalation_at IS NOT NULL
		   AND next_escalation_at <= $1`, now)
	if err != nil {
		return nil, fmt.Errorf("query due incidents: %w", err)
	}
	defer rows.Close()
	var out []EscalatingIncident
	for rows.Next() {
		var (
			inc EscalatingIncident
			sev string
		)
		if err := rows.Scan(&inc.ID, &inc.PipelineID, &inc.PolicyID, &inc.Step,
			&inc.Title, &inc.Summary, &sev); err != nil {
			return nil, fmt.Errorf("scan due incident: %w", err)
		}
		inc.Severity = rules.Severity(sev)
		out = append(out, inc)
	}
	return out, rows.Err()
}

// AdvanceEscalation records the next step and when it is next due. A nil nextAt
// clears the due time, which stops recurrence (the policy is exhausted).
func (s *PgStore) AdvanceEscalation(ctx context.Context, incidentID int64, nextStep int, nextAt *time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE incidents SET escalation_step = $2, next_escalation_at = $3 WHERE id = $1`,
		incidentID, nextStep, nextAt)
	if err != nil {
		return fmt.Errorf("advance escalation: %w", err)
	}
	return nil
}

// PgPolicies loads escalation policies from escalation_policies.
type PgPolicies struct{ pool *pgxpool.Pool }

// NewPgPolicies builds a Postgres-backed Policies loader.
func NewPgPolicies(pool *pgxpool.Pool) *PgPolicies { return &PgPolicies{pool: pool} }

// dbStep is the on-disk step shape ({after_seconds, channel_ids, oncall_schedule_id}).
type dbStep struct {
	AfterSeconds     int     `json:"after_seconds"`
	ChannelIDs       []int64 `json:"channel_ids"`
	OnCallScheduleID int64   `json:"oncall_schedule_id"`
}

// Policy loads a policy and maps its stored steps to escalation Steps.
func (p *PgPolicies) Policy(ctx context.Context, id int64) (Policy, error) {
	var (
		pol     Policy
		stepRaw []byte
	)
	err := p.pool.QueryRow(ctx,
		`SELECT id, name, steps FROM escalation_policies WHERE id = $1`, id).
		Scan(&pol.ID, &pol.Name, &stepRaw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Policy{}, fmt.Errorf("escalation policy %d not found", id)
		}
		return Policy{}, fmt.Errorf("load policy %d: %w", id, err)
	}
	var steps []dbStep
	if len(stepRaw) > 0 {
		if err := json.Unmarshal(stepRaw, &steps); err != nil {
			return Policy{}, fmt.Errorf("decode policy %d steps: %w", id, err)
		}
	}
	pol.Steps = make([]Step, len(steps))
	for i, s := range steps {
		pol.Steps[i] = Step{
			After:            time.Duration(s.AfterSeconds) * time.Second,
			ChannelIDs:       s.ChannelIDs,
			OnCallScheduleID: s.OnCallScheduleID,
		}
	}
	return pol, nil
}

// PgSchedules loads on-call schedules from oncall_schedules.
type PgSchedules struct{ pool *pgxpool.Pool }

// NewPgSchedules builds a Postgres-backed Schedules loader.
func NewPgSchedules(pool *pgxpool.Pool) *PgSchedules { return &PgSchedules{pool: pool} }

// Schedule loads an on-call rotation and its overrides.
func (s *PgSchedules) Schedule(ctx context.Context, id int64) (OnCall, error) {
	var (
		oc          OnCall
		rotationRaw []byte
		overrideRaw []byte
	)
	err := s.pool.QueryRow(ctx,
		`SELECT id, name, rotation, overrides FROM oncall_schedules WHERE id = $1`, id).
		Scan(&oc.ID, &oc.Name, &rotationRaw, &overrideRaw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return OnCall{}, fmt.Errorf("oncall schedule %d not found", id)
		}
		return OnCall{}, fmt.Errorf("load schedule %d: %w", id, err)
	}
	if len(rotationRaw) > 0 {
		_ = json.Unmarshal(rotationRaw, &oc.Responders)
	}
	if len(overrideRaw) > 0 {
		_ = json.Unmarshal(overrideRaw, &oc.Overrides)
	}
	return oc, nil
}

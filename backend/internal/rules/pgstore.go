package rules

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore implements Store over Postgres: incident dedup/create/bump and alert
// recording against the incidents and alerts tables.
type PgStore struct{ pool *pgxpool.Pool }

// NewPgStore builds a Postgres-backed rules Store.
func NewPgStore(pool *pgxpool.Pool) *PgStore { return &PgStore{pool: pool} }

// FindOpenIncident returns the open (or acked/snoozed) incident with the given
// fingerprint, or nil when there is none — the dedup check.
func (s *PgStore) FindOpenIncident(ctx context.Context, fingerprint string) (*Incident, error) {
	var inc Incident
	var pipelineID *int64
	err := s.pool.QueryRow(ctx,
		`SELECT id, pipeline_id, title, severity, status, fingerprint, event_count, summary, opened_at
		 FROM incidents
		 WHERE fingerprint = $1 AND status IN ('open','acked','snoozed')
		 LIMIT 1`, fingerprint).Scan(
		&inc.ID, &pipelineID, &inc.Title, &inc.Severity, &inc.Status, &inc.Fingerprint,
		&inc.EventCount, &inc.Summary, &inc.OpenedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("find open incident: %w", err)
	}
	if pipelineID != nil {
		inc.PipelineID = *pipelineID
	}
	return &inc, nil
}

// CreateIncident inserts a new incident and returns its id.
func (s *PgStore) CreateIncident(ctx context.Context, inc *Incident) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO incidents (pipeline_id, title, severity, status, fingerprint,
		     event_count, summary, escalation_policy_id, opened_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`,
		nullID(inc.PipelineID), inc.Title, inc.Severity, inc.Status, inc.Fingerprint,
		inc.EventCount, inc.Summary, nullID(inc.PolicyID), inc.OpenedAt).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("create incident: %w", err)
	}
	return id, nil
}

// BumpIncident increments the event count of an existing incident (dedup path).
func (s *PgStore) BumpIncident(ctx context.Context, id int64, _ time.Time) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE incidents SET event_count = event_count + 1 WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("bump incident: %w", err)
	}
	return nil
}

// RecordAlert appends one alert-delivery record.
func (s *PgStore) RecordAlert(ctx context.Context, incidentID, ruleID, channelID int64, sent bool, errMsg string) error {
	status := "sent"
	if !sent {
		status = "failed"
	}
	_, err := s.pool.Exec(ctx,
		`INSERT INTO alerts (incident_id, rule_id, channel_id, status, error)
		 VALUES ($1,$2,$3,$4,$5)`,
		incidentID, nullID(ruleID), nullID(channelID), status, errMsg)
	if err != nil {
		return fmt.Errorf("record alert: %w", err)
	}
	return nil
}

// PgLoader loads the applicable rule set for a pipeline from the rules table.
type PgLoader struct{ pool *pgxpool.Pool }

// NewPgLoader builds a Postgres-backed rule loader.
func NewPgLoader(pool *pgxpool.Pool) *PgLoader { return &PgLoader{pool: pool} }

// LoadRules returns the enabled rules that apply to a pipeline: its own rules
// plus global (pipeline_id IS NULL) rules.
func (l *PgLoader) LoadRules(ctx context.Context, pipelineID int64) ([]Rule, error) {
	rows, err := l.pool.Query(ctx,
		`SELECT id, pipeline_id, stream, name, condition, severity, channel_ids,
		        escalation_policy_id, enabled, dedup_window_seconds, quiet_hours, maintenance_until
		 FROM rules
		 WHERE enabled AND (pipeline_id = $1 OR pipeline_id IS NULL)`, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("load rules for pipeline %d: %w", pipelineID, err)
	}
	defer rows.Close()

	var out []Rule
	for rows.Next() {
		var (
			r          Rule
			pid        *int64
			policyID   *int64
			condRaw    []byte
			quietRaw   []byte
			dedupSecs  int
			maintUntil *time.Time
		)
		if err := rows.Scan(&r.ID, &pid, &r.Stream, &r.Name, &condRaw, &r.Severity,
			&r.ChannelIDs, &policyID, &r.Enabled, &dedupSecs, &quietRaw, &maintUntil); err != nil {
			return nil, fmt.Errorf("scan rule: %w", err)
		}
		if pid != nil {
			r.PipelineID = *pid
		}
		if policyID != nil {
			r.EscalationPolicyID = *policyID
		}
		if err := json.Unmarshal(condRaw, &r.Condition); err != nil {
			return nil, fmt.Errorf("decode rule %d condition: %w", r.ID, err)
		}
		if len(quietRaw) > 0 {
			_ = json.Unmarshal(quietRaw, &r.QuietHours) // best-effort; empty on error
		}
		r.DedupWindow = time.Duration(dedupSecs) * time.Second
		if maintUntil != nil {
			r.MaintenanceUntil = *maintUntil
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// nullID maps a zero id to SQL NULL for nullable foreign keys.
func nullID(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

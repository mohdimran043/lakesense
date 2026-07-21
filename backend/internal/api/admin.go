package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/envs"
	"github.com/lakesense/lakesense/backend/internal/runner"
)

// This file holds the "admin" write endpoints (B4): incident actions, rule and
// channel management, and config export. Handlers own their SQL (mirroring the
// read handlers) and record an audit entry after each successful mutation.

func (s *Server) registerAdmin(r chi.Router) {
	r.Post("/incidents/{id}/ack", s.ackIncident)
	r.Post("/incidents/{id}/snooze", s.snoozeIncident)
	r.Post("/incidents/{id}/resolve", s.resolveIncident)
	r.Post("/rules", s.createRule)
	r.Delete("/rules/{id}", s.deleteRule)
	r.Post("/channels", s.createChannel)
	r.Delete("/channels/{id}", s.deleteChannel)
	r.Get("/pipelines/{id}/config", s.exportConfig)
	r.Post("/pipelines/{id}/config", s.applyConfig)
	r.Post("/pipelines/{id}/backfill", s.launchBackfill)
	r.Post("/escalation-policies", s.createEscalationPolicy)
	r.Post("/oncall-schedules", s.createOncallSchedule)
	r.Post("/pipelines/{id}/promote", s.promotePipeline)
	r.Post("/quality-monitors", s.createQualityMonitor)
}

var qualityKinds = map[string]bool{"freshness": true, "volume": true, "null_rate": true, "distribution": true}

func (s *Server) createQualityMonitor(w http.ResponseWriter, r *http.Request) {
	var req struct {
		PipelineID int64           `json:"pipeline_id"`
		Stream     string          `json:"stream"`
		Column     string          `json:"column"`
		Kind       string          `json:"kind"`
		Config     json.RawMessage `json:"config"`
		Baseline   json.RawMessage `json:"baseline"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.PipelineID == 0 || req.Stream == "" || !qualityKinds[req.Kind] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "pipeline_id, stream, and a valid kind (freshness|volume|null_rate|distribution) are required"})
		return
	}
	config := req.Config
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	baseline := req.Baseline
	if len(baseline) == 0 {
		baseline = json.RawMessage(`{}`)
	}
	var id int64
	err := s.pool.QueryRow(r.Context(),
		`INSERT INTO quality_monitors (pipeline_id, stream, column_name, kind, config, baseline)
		 VALUES ($1,$2,$3,$4,$5,$6) RETURNING id`,
		req.PipelineID, req.Stream, req.Column, req.Kind, config, baseline).Scan(&id)
	if err != nil {
		writeErr(w, "create quality monitor", err)
		return
	}
	s.audited(r, "quality_monitor.create", "quality_monitor", itoa(id), map[string]any{"kind": req.Kind, "stream": req.Stream})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

// promotePipeline clones a pipeline's latest config into a target environment
// with credential overrides, creating a new pipeline there.
func (s *Server) promotePipeline(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct {
		TargetEnv            string            `json:"target_env"`
		SourceOverrides      map[string]string `json:"source_overrides"`
		DestinationOverrides map[string]string `json:"destination_overrides"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	p, err := s.pipelines.Promote(r.Context(), actor(r), id, req.TargetEnv,
		envs.Overrides{Source: req.SourceOverrides, Destination: req.DestinationOverrides})
	if err != nil {
		writeWriteErr(w, err)
		return
	}
	s.audited(r, "pipeline.promote", "pipeline", chi.URLParam(r, "id"), map[string]any{"target_env": req.TargetEnv, "new_pipeline_id": p.ID})
	writeJSON(w, http.StatusCreated, p)
}

// clock returns the server clock, defaulting to wall time when unset (tests may
// leave it nil).
func (s *Server) clock() time.Time {
	if s.now != nil {
		return s.now().UTC()
	}
	return time.Now().UTC()
}

// audited records an audit entry for a mutation, logging (not failing) on error
// — the mutation already succeeded. before is nil for these create/delete/action
// events; the after value captures the change.
func (s *Server) audited(r *http.Request, action, entityType, entityID string, after any) {
	if s.audit == nil {
		return
	}
	if err := audit.Log(r.Context(), s.audit, actor(r), action, entityType, entityID, nil, after, s.clock()); err != nil {
		s.logger.Error("audit", "action", action, "err", err)
	}
}

// --- incident actions ---

func (s *Server) ackIncident(w http.ResponseWriter, r *http.Request) {
	// ack stamps acked_by ($2), so it takes the actor argument.
	s.transitionIncident(w, r, "ack", "acked", true,
		`UPDATE incidents SET status='acked', acked_at=now(), acked_by=$2
		 WHERE id=$1 AND status IN ('open','snoozed') RETURNING id`)
}

func (s *Server) resolveIncident(w http.ResponseWriter, r *http.Request) {
	// resolve takes only the id ($1) — no actor column, so needsActor is false.
	s.transitionIncident(w, r, "resolve", "resolved", false,
		`UPDATE incidents SET status='resolved', resolved_at=now()
		 WHERE id=$1 AND status IN ('open','acked','snoozed') RETURNING id`)
}

func (s *Server) snoozeIncident(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var body struct {
		Until string `json:"until"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	until, err := time.Parse(time.RFC3339, body.Until)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "until must be an RFC3339 timestamp"})
		return
	}
	var gotID int64
	err = s.pool.QueryRow(r.Context(),
		`UPDATE incidents SET status='snoozed', snoozed_until=$2
		 WHERE id=$1 AND status IN ('open','acked') RETURNING id`, id, until).Scan(&gotID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "incident not found or not snoozable"})
		return
	}
	_, _ = s.pool.Exec(r.Context(), `INSERT INTO acks (incident_id, actor, action) VALUES ($1,$2,'snooze')`, id, actor(r))
	s.audited(r, "incident.snooze", "incident", chi.URLParam(r, "id"), map[string]string{"until": body.Until})
	writeJSON(w, http.StatusOK, map[string]string{"status": "snoozed"})
}

// transitionIncident runs a status-changing UPDATE that RETURNs the id, records
// an ack row + audit, and maps a no-row result to 404. needsActor selects
// whether the SQL binds the actor as $2 (ack stamps acked_by; resolve does not).
func (s *Server) transitionIncident(w http.ResponseWriter, r *http.Request, action, status string, needsActor bool, sql string) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var gotID int64
	var err error
	if needsActor {
		err = s.pool.QueryRow(r.Context(), sql, id, actor(r)).Scan(&gotID)
	} else {
		err = s.pool.QueryRow(r.Context(), sql, id).Scan(&gotID)
	}
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "incident not found or not in a valid state"})
		return
	}
	_, _ = s.pool.Exec(r.Context(), `INSERT INTO acks (incident_id, actor, action) VALUES ($1,$2,$3)`, id, actor(r), action)
	s.audited(r, "incident."+action, "incident", chi.URLParam(r, "id"), map[string]string{"status": status})
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}

// --- rules ---

type ruleRequest struct {
	PipelineID         *int64          `json:"pipeline_id"`
	Stream             string          `json:"stream"`
	Name               string          `json:"name"`
	Condition          json.RawMessage `json:"condition"`
	Severity           string          `json:"severity"`
	ChannelIDs         []int64         `json:"channel_ids"`
	EscalationPolicyID *int64          `json:"escalation_policy_id"`
	Enabled            *bool           `json:"enabled"`
	DedupWindowSeconds int             `json:"dedup_window_seconds"`
	QuietHours         json.RawMessage `json:"quiet_hours"`
}

func (s *Server) createRule(w http.ResponseWriter, r *http.Request) {
	var req ruleRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.Name == "" || len(req.Condition) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name and condition are required"})
		return
	}
	sev := req.Severity
	if sev == "" {
		sev = "warning"
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	dedup := req.DedupWindowSeconds
	if dedup == 0 {
		dedup = 300
	}
	quiet := req.QuietHours
	if len(quiet) == 0 {
		quiet = json.RawMessage(`{}`)
	}
	channelIDs := req.ChannelIDs
	if channelIDs == nil {
		channelIDs = []int64{}
	}
	var id int64
	err := s.pool.QueryRow(r.Context(),
		`INSERT INTO rules (pipeline_id, stream, name, condition, severity, channel_ids,
		     escalation_policy_id, enabled, dedup_window_seconds, quiet_hours)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`,
		req.PipelineID, req.Stream, req.Name, req.Condition, sev, channelIDs,
		req.EscalationPolicyID, enabled, dedup, quiet).Scan(&id)
	if err != nil {
		writeErr(w, "create rule", err)
		return
	}
	s.audited(r, "rule.create", "rule", itoa(id), req)
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (s *Server) deleteRule(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	tag, err := s.pool.Exec(r.Context(), `DELETE FROM rules WHERE id=$1`, id)
	if err != nil {
		writeErr(w, "delete rule", err)
		return
	}
	if tag.RowsAffected() == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "rule not found"})
		return
	}
	s.audited(r, "rule.delete", "rule", chi.URLParam(r, "id"), nil)
	w.WriteHeader(http.StatusNoContent)
}

// --- channels ---

type channelRequest struct {
	Name   string          `json:"name"`
	Type   string          `json:"type"`
	Config json.RawMessage `json:"config"`
}

var channelTypes = map[string]bool{"slack": true, "telegram": true, "email": true, "webhook": true}

func (s *Server) createChannel(w http.ResponseWriter, r *http.Request) {
	var req channelRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.Name == "" || !channelTypes[req.Type] {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name and a valid type (slack|telegram|email|webhook) are required"})
		return
	}
	cfg := req.Config
	if len(cfg) == 0 {
		cfg = json.RawMessage(`{}`)
	}
	var id int64
	err := s.pool.QueryRow(r.Context(),
		`INSERT INTO channels (name, type, config) VALUES ($1,$2,$3) RETURNING id`,
		req.Name, req.Type, cfg).Scan(&id)
	if err != nil {
		writeErr(w, "create channel", err)
		return
	}
	s.audited(r, "channel.create", "channel", itoa(id), map[string]string{"name": req.Name, "type": req.Type})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (s *Server) deleteChannel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	tag, err := s.pool.Exec(r.Context(), `DELETE FROM channels WHERE id=$1`, id)
	if err != nil {
		writeErr(w, "delete channel", err)
		return
	}
	if tag.RowsAffected() == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "channel not found"})
		return
	}
	s.audited(r, "channel.delete", "channel", chi.URLParam(r, "id"), nil)
	w.WriteHeader(http.StatusNoContent)
}

// --- config export (pipeline-as-code) ---

func (s *Server) exportConfig(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var (
		version int
		yamlDoc string
	)
	err := s.pool.QueryRow(r.Context(),
		`SELECT version, yaml FROM pipeline_config_versions
		 WHERE pipeline_id=$1 ORDER BY version DESC LIMIT 1`, id).Scan(&version, &yamlDoc)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "no config versions for pipeline"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"version": version, "yaml": yamlDoc})
}

// --- escalation policies & on-call schedules ---

func (s *Server) createEscalationPolicy(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name  string          `json:"name"`
		Steps json.RawMessage `json:"steps"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
		return
	}
	steps := req.Steps
	if len(steps) == 0 {
		steps = json.RawMessage(`[]`)
	}
	var id int64
	if err := s.pool.QueryRow(r.Context(),
		`INSERT INTO escalation_policies (name, steps) VALUES ($1,$2) RETURNING id`,
		req.Name, steps).Scan(&id); err != nil {
		writeErr(w, "create escalation policy", err)
		return
	}
	s.audited(r, "escalation_policy.create", "escalation_policy", itoa(id), map[string]string{"name": req.Name})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

func (s *Server) createOncallSchedule(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string          `json:"name"`
		Rotation  json.RawMessage `json:"rotation"`
		Overrides json.RawMessage `json:"overrides"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
		return
	}
	rotation, overrides := req.Rotation, req.Overrides
	if len(rotation) == 0 {
		rotation = json.RawMessage(`[]`)
	}
	if len(overrides) == 0 {
		overrides = json.RawMessage(`[]`)
	}
	var id int64
	if err := s.pool.QueryRow(r.Context(),
		`INSERT INTO oncall_schedules (name, rotation, overrides) VALUES ($1,$2,$3) RETURNING id`,
		req.Name, rotation, overrides).Scan(&id); err != nil {
		writeErr(w, "create oncall schedule", err)
		return
	}
	s.audited(r, "oncall_schedule.create", "oncall_schedule", itoa(id), map[string]string{"name": req.Name})
	writeJSON(w, http.StatusCreated, map[string]any{"id": id})
}

// applyConfig imports a pipeline-as-code YAML document, creating a new config
// version (no-op when unchanged) — the write side of config export.
func (s *Server) applyConfig(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req struct {
		YAML string `json:"yaml"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.YAML == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "a yaml field is required"})
		return
	}
	p, err := s.pipelines.ApplyYAML(r.Context(), actor(r), id, req.YAML)
	if err != nil {
		writeWriteErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// --- backfill launch ---

type backfillRequest struct {
	Stream     string `json:"stream"`
	PKMin      string `json:"pk_min"`
	PKMax      string `json:"pk_max"`
	SinceField string `json:"since_field"`
	SinceValue string `json:"since_value"`
}

// launchBackfill records a backfill job and triggers it in the background,
// returning 202 with the job id. The engine backfill is idempotent (merge-on-
// read) and state-safe, so it never disturbs the pipeline's ongoing sync state.
func (s *Server) launchBackfill(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req backfillRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	if req.Stream == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "stream is required"})
		return
	}
	mode := "pk_range"
	if req.SinceField != "" {
		mode = "changed_since"
	} else if req.PKMin == "" && req.PKMax == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide a pk_min/pk_max range or a since_field/since_value window"})
		return
	}
	params, _ := json.Marshal(req)

	var jobID int64
	err := s.pool.QueryRow(r.Context(),
		`INSERT INTO backfill_jobs (pipeline_id, stream, mode, params, status, requested_by)
		 VALUES ($1,$2,$3,$4,'queued',$5) RETURNING id`,
		id, req.Stream, mode, params, actor(r)).Scan(&jobID)
	if err != nil {
		writeErr(w, "queue backfill", err)
		return
	}
	s.audited(r, "pipeline.backfill", "pipeline", chi.URLParam(r, "id"), map[string]any{"job_id": jobID, "stream": req.Stream})

	opts := runner.BackfillOpts{Stream: req.Stream, PKMin: req.PKMin, PKMax: req.PKMax, SinceField: req.SinceField, SinceValue: req.SinceValue}
	go s.runBackfillJob(jobID, id, opts)

	writeJSON(w, http.StatusAccepted, map[string]any{"status": "started", "job_id": jobID})
}

// runBackfillJob executes a queued backfill and records its lifecycle. It uses a
// background context so it survives the HTTP response.
func (s *Server) runBackfillJob(jobID, pipelineID int64, opts runner.BackfillOpts) {
	ctx := context.Background()
	_, _ = s.pool.Exec(ctx, `UPDATE backfill_jobs SET status='running', started_at=now() WHERE id=$1`, jobID)
	if s.runner == nil {
		return
	}
	res, err := s.runner.Backfill(ctx, pipelineID, opts)
	if err != nil {
		s.logger.Error("backfill failed", "job_id", jobID, "pipeline_id", pipelineID, "err", err)
		_, _ = s.pool.Exec(ctx, `UPDATE backfill_jobs SET status='failed', error=$2, finished_at=now() WHERE id=$1`, jobID, err.Error())
		return
	}
	_, _ = s.pool.Exec(ctx, `UPDATE backfill_jobs SET status='succeeded', rows=$2, finished_at=now() WHERE id=$1`, jobID, res.Events)
}

func itoa(id int64) string { return strconv.FormatInt(id, 10) }

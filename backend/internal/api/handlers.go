package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

// This file exposes the read API over the control-plane store: the queries the
// dashboard renders (pipeline health, incidents, diff badges, analytics,
// lineage, audit). Handlers are thin; each owns its SQL. Health scores are
// computed on read from recent signals so a fresh seed needs no batch job.

func (s *Server) registerData(r chi.Router) {
	r.Get("/pipelines", s.listPipelines)
	r.Get("/pipelines/{id}", s.getPipeline)
	r.Get("/pipelines/{id}/metrics", s.pipelineMetrics)
	r.Get("/pipelines/{id}/diffs", s.pipelineDiffs)
	r.Get("/pipelines/{id}/lineage", s.pipelineLineage)
	r.Get("/incidents", s.listIncidents)
	r.Get("/analytics", s.analytics)
	r.Get("/audit", s.auditLog)
}

// Pipeline is the list/detail view model, enriched with derived health.
type Pipeline struct {
	ID              int64      `json:"id"`
	Name            string     `json:"name"`
	Slug            string     `json:"slug"`
	Environment     string     `json:"environment"`
	SourceType      string     `json:"source_type"`
	DestinationType string     `json:"destination_type"`
	Status          string     `json:"status"`
	Schedule        string     `json:"schedule"`
	LastSyncAt      *time.Time `json:"last_sync_at"`
	HealthScore     int        `json:"health_score"`
	// DiffVerified is the latest diff badge: all recent stream diffs matched.
	DiffVerified  bool  `json:"diff_verified"`
	VerifiedRows  int64 `json:"verified_rows"`
	OpenIncidents int   `json:"open_incidents"`
}

func (s *Server) listPipelines(w http.ResponseWriter, r *http.Request) {
	rows, err := s.pool.Query(r.Context(), pipelineSelectSQL+" ORDER BY p.name")
	if err != nil {
		writeErr(w, "query pipelines", err)
		return
	}
	defer rows.Close()
	out, err := scanPipelines(rows)
	if err != nil {
		writeErr(w, "scan pipelines", err)
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getPipeline(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	rows, err := s.pool.Query(r.Context(), pipelineSelectSQL+" WHERE p.id = $1", id)
	if err != nil {
		writeErr(w, "query pipeline", err)
		return
	}
	defer rows.Close()
	out, err := scanPipelines(rows)
	if err != nil {
		writeErr(w, "scan pipeline", err)
		return
	}
	if len(out) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "pipeline not found"})
		return
	}
	writeJSON(w, http.StatusOK, out[0])
}

// pipelineSelectSQL computes the health score and diff badge inline. The diff
// badge reflects the LATEST sync (not a wall-clock window), so a historical
// mismatch that has since been corrected doesn't keep a pipeline red:
//   - start at 100
//   - −40 if the most recent run failed
//   - −25 if the latest sync's diffs didn't all verify
//   - −10 per open incident (capped at −30)
const pipelineSelectSQL = `
WITH latest AS (
  SELECT DISTINCT ON (pipeline_id) pipeline_id, sync_id
  FROM diff_runs ORDER BY pipeline_id, created_at DESC
)
SELECT p.id, p.name, p.slug, e.slug AS env, p.source_type, p.destination_type,
       p.status, p.schedule, p.last_sync_at,
       COALESCE((SELECT bool_and(d.match) FROM diff_runs d
                 JOIN latest l ON l.pipeline_id = d.pipeline_id AND l.sync_id = d.sync_id
                 WHERE d.pipeline_id = p.id), true) AS diff_verified,
       COALESCE((SELECT sum(d.dest_rows) FROM diff_runs d
                 JOIN latest l ON l.pipeline_id = d.pipeline_id AND l.sync_id = d.sync_id
                 WHERE d.pipeline_id = p.id AND d.match), 0) AS verified_rows,
       (SELECT count(*) FROM incidents i
        WHERE i.pipeline_id = p.id AND i.status IN ('open','acked','snoozed'))::int AS open_incidents,
       (SELECT bool_or(ev.kind = 'sync_failed') FROM events ev
        WHERE ev.pipeline_id = p.id AND ev.ts > now() - interval '1 day') AS recent_failure
FROM pipelines p
JOIN environments e ON e.id = p.environment_id`

func scanPipelines(rows pgx.Rows) ([]Pipeline, error) {
	var out []Pipeline
	for rows.Next() {
		var p Pipeline
		var recentFailure *bool
		if err := rows.Scan(&p.ID, &p.Name, &p.Slug, &p.Environment, &p.SourceType,
			&p.DestinationType, &p.Status, &p.Schedule, &p.LastSyncAt,
			&p.DiffVerified, &p.VerifiedRows, &p.OpenIncidents, &recentFailure); err != nil {
			return nil, err
		}
		p.HealthScore = computeHealth(recentFailure, p.DiffVerified, p.OpenIncidents)
		out = append(out, p)
	}
	return out, rows.Err()
}

// computeHealth is the composite 0..100 score surfaced on every pipeline card.
func computeHealth(recentFailure *bool, diffVerified bool, openIncidents int) int {
	score := 100
	if recentFailure != nil && *recentFailure {
		score -= 40
	}
	if !diffVerified {
		score -= 25
	}
	score -= min(openIncidents*10, 30)
	if score < 0 {
		score = 0
	}
	return score
}

func (s *Server) pipelineMetrics(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	rows, err := s.pool.Query(r.Context(),
		`SELECT ts, rows_written, bytes_written, duration_seconds
		 FROM metrics WHERE pipeline_id = $1 ORDER BY ts`, id)
	if err != nil {
		writeErr(w, "query metrics", err)
		return
	}
	defer rows.Close()
	type point struct {
		TS       time.Time `json:"ts"`
		Rows     int64     `json:"rows_written"`
		Bytes    int64     `json:"bytes_written"`
		Duration float64   `json:"duration_seconds"`
	}
	var out []point
	for rows.Next() {
		var p point
		if err := rows.Scan(&p.TS, &p.Rows, &p.Bytes, &p.Duration); err != nil {
			writeErr(w, "scan metric", err)
			return
		}
		out = append(out, p)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) pipelineDiffs(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	rows, err := s.pool.Query(r.Context(),
		`SELECT stream, sync_id, source_rows, dest_rows, source_checksum, dest_checksum, match, created_at
		 FROM diff_runs WHERE pipeline_id = $1 ORDER BY created_at DESC LIMIT 200`, id)
	if err != nil {
		writeErr(w, "query diffs", err)
		return
	}
	defer rows.Close()
	type diff struct {
		Stream    string    `json:"stream"`
		SyncID    string    `json:"sync_id"`
		SrcRows   int64     `json:"source_rows"`
		DestRows  int64     `json:"dest_rows"`
		SrcSum    string    `json:"source_checksum"`
		DestSum   string    `json:"dest_checksum"`
		Match     bool      `json:"match"`
		CreatedAt time.Time `json:"created_at"`
	}
	var out []diff
	for rows.Next() {
		var d diff
		if err := rows.Scan(&d.Stream, &d.SyncID, &d.SrcRows, &d.DestRows, &d.SrcSum, &d.DestSum, &d.Match, &d.CreatedAt); err != nil {
			writeErr(w, "scan diff", err)
			return
		}
		out = append(out, d)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) pipelineLineage(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	rows, err := s.pool.Query(r.Context(),
		`SELECT source_stream, source_column, source_type, dest_table, dest_column, dest_type
		 FROM lineage_edges WHERE pipeline_id = $1 ORDER BY source_stream, source_column`, id)
	if err != nil {
		writeErr(w, "query lineage", err)
		return
	}
	defer rows.Close()
	type edge struct {
		SourceStream string `json:"source_stream"`
		SourceColumn string `json:"source_column"`
		SourceType   string `json:"source_type"`
		DestTable    string `json:"dest_table"`
		DestColumn   string `json:"dest_column"`
		DestType     string `json:"dest_type"`
	}
	var out []edge
	for rows.Next() {
		var e edge
		if err := rows.Scan(&e.SourceStream, &e.SourceColumn, &e.SourceType, &e.DestTable, &e.DestColumn, &e.DestType); err != nil {
			writeErr(w, "scan edge", err)
			return
		}
		out = append(out, e)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) listIncidents(w http.ResponseWriter, r *http.Request) {
	rows, err := s.pool.Query(r.Context(),
		`SELECT i.id, COALESCE(p.name,''), i.title, i.severity, i.status, i.event_count,
		        i.summary, i.opened_at, i.acked_at, i.resolved_at
		 FROM incidents i LEFT JOIN pipelines p ON p.id = i.pipeline_id
		 ORDER BY i.opened_at DESC LIMIT 200`)
	if err != nil {
		writeErr(w, "query incidents", err)
		return
	}
	defer rows.Close()
	type incident struct {
		ID         int64      `json:"id"`
		Pipeline   string     `json:"pipeline"`
		Title      string     `json:"title"`
		Severity   string     `json:"severity"`
		Status     string     `json:"status"`
		EventCount int        `json:"event_count"`
		Summary    string     `json:"summary"`
		OpenedAt   time.Time  `json:"opened_at"`
		AckedAt    *time.Time `json:"acked_at"`
		ResolvedAt *time.Time `json:"resolved_at"`
	}
	var out []incident
	for rows.Next() {
		var i incident
		if err := rows.Scan(&i.ID, &i.Pipeline, &i.Title, &i.Severity, &i.Status, &i.EventCount,
			&i.Summary, &i.OpenedAt, &i.AckedAt, &i.ResolvedAt); err != nil {
			writeErr(w, "scan incident", err)
			return
		}
		out = append(out, i)
	}
	writeJSON(w, http.StatusOK, out)
}

// analytics returns per-pipeline totals and a simple cost estimate. The cost
// model is intentionally transparent (anti-Fivetran-opacity): $/GB stored +
// $/compute-hour, both configurable via query params.
func (s *Server) analytics(w http.ResponseWriter, r *http.Request) {
	costPerGB := floatParam(r, "cost_per_gb", 0.023)    // ~S3 standard
	costPerHour := floatParam(r, "cost_per_hour", 0.10) // modest compute

	rows, err := s.pool.Query(r.Context(),
		`SELECT p.name,
		        COALESCE(sum(m.rows_written),0),
		        COALESCE(sum(m.bytes_written),0),
		        COALESCE(sum(m.duration_seconds),0)
		 FROM pipelines p LEFT JOIN metrics m ON m.pipeline_id = p.id
		 GROUP BY p.name ORDER BY p.name`)
	if err != nil {
		writeErr(w, "query analytics", err)
		return
	}
	defer rows.Close()
	type row struct {
		Pipeline   string  `json:"pipeline"`
		Rows       int64   `json:"rows"`
		Bytes      int64   `json:"bytes"`
		Seconds    float64 `json:"seconds"`
		EstCostUSD float64 `json:"est_cost_usd"`
	}
	var out []row
	var totalCost float64
	for rows.Next() {
		var x row
		if err := rows.Scan(&x.Pipeline, &x.Rows, &x.Bytes, &x.Seconds); err != nil {
			writeErr(w, "scan analytics", err)
			return
		}
		gb := float64(x.Bytes) / (1 << 30)
		hours := x.Seconds / 3600
		x.EstCostUSD = round2(gb*costPerGB + hours*costPerHour)
		totalCost += x.EstCostUSD
		out = append(out, x)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"pipelines":          out,
		"total_est_cost_usd": round2(totalCost),
		"cost_per_gb":        costPerGB,
		"cost_per_hour":      costPerHour,
	})
}

func (s *Server) auditLog(w http.ResponseWriter, r *http.Request) {
	rows, err := s.pool.Query(r.Context(),
		`SELECT actor, action, entity_type, entity_id, created_at
		 FROM audit_log ORDER BY created_at DESC LIMIT 500`)
	if err != nil {
		writeErr(w, "query audit", err)
		return
	}
	defer rows.Close()
	type entry struct {
		Actor      string    `json:"actor"`
		Action     string    `json:"action"`
		EntityType string    `json:"entity_type"`
		EntityID   string    `json:"entity_id"`
		CreatedAt  time.Time `json:"created_at"`
	}
	var out []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.Actor, &e.Action, &e.EntityType, &e.EntityID, &e.CreatedAt); err != nil {
			writeErr(w, "scan audit", err)
			return
		}
		out = append(out, e)
	}
	writeJSON(w, http.StatusOK, out)
}

// --- helpers ---

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return 0, false
	}
	return id, true
}

func floatParam(r *http.Request, key string, def float64) float64 {
	if v := r.URL.Query().Get(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// writeErr responds with a human-readable 500. The underlying error is left to
// the recoverer/logging middleware; the body never leaks raw SQL or driver text.
func writeErr(w http.ResponseWriter, msg string, _ error) {
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": msg})
}

func round2(f float64) float64 { return float64(int64(f*100)) / 100 }

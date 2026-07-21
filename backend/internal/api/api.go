// Package api is the control-plane HTTP surface: a chi router with health,
// readiness, and (as features land) the pipeline/incident/rule/etc. resources.
// Handlers accept context and return JSON; feature packages register their own
// sub-routers here.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/buildinfo"
	"github.com/lakesense/lakesense/backend/internal/pipelines"
)

// Server holds shared dependencies for handlers.
type Server struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	pipelines *pipelines.Service
	runner    pipelineRunner
	audit     audit.Recorder
	now       func() time.Time
}

// New builds the router with logging, recovery, request-id, and timeout
// middleware plus the base routes, wiring the pipeline write service and the
// runner over the pool.
func New(pool *pgxpool.Pool, logger *slog.Logger, run pipelineRunner) http.Handler {
	rec := audit.NewPgRecorder(pool)
	svc := pipelines.NewService(pipelines.NewPgRepo(pool), rec, nil)
	s := &Server{pool: pool, logger: logger, pipelines: svc, runner: run, audit: rec, now: time.Now}
	return chiRouter(s)
}

// chiRouter builds the router for a Server. Shared by New and tests so handler
// tests can run against a Server with a fake-backed service.
func chiRouter(s *Server) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	// RealIP is intentionally omitted: it trusts client-supplied forwarding
	// headers (IP-spoofable). Deployments that need real client IPs should set
	// them at a trusted reverse proxy instead.
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", s.health)
	r.Get("/readyz", s.ready)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/version", s.version)
		s.registerData(r)
		s.registerWrites(r)
		s.registerAdmin(r)
	})
	return r
}

// health is a liveness probe — always 200 if the process is up.
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ready is a readiness probe — 200 only when the database is reachable.
func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.pool.Ping(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"status": "unavailable", "error": "database unreachable",
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"name":    "lakesense",
		"version": buildinfo.Version,
	})
}

// writeJSON writes v as a JSON response with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

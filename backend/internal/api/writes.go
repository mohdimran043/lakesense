package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/lakesense/lakesense/backend/internal/pipelines"
	"github.com/lakesense/lakesense/backend/internal/runner"
)

// pipelineRunner triggers pipeline runs and backfills. Implemented by
// *runner.Runner; faked in tests.
type pipelineRunner interface {
	Run(ctx context.Context, id int64) (runner.RunResult, error)
	Backfill(ctx context.Context, id int64, o runner.BackfillOpts) (runner.RunResult, error)
}

// registerWrites mounts the pipeline write endpoints. The read API (registerData)
// stays untouched; writes are additive.
func (s *Server) registerWrites(r chi.Router) {
	r.Post("/pipelines", s.createPipeline)
	r.Patch("/pipelines/{id}", s.updatePipeline)
	r.Post("/pipelines/{id}/pause", s.statusSetter("paused"))
	r.Post("/pipelines/{id}/resume", s.statusSetter("active"))
	r.Delete("/pipelines/{id}", s.archivePipeline)
	r.Post("/pipelines/{id}/rollback/{version}", s.rollbackPipeline)
	r.Post("/pipelines/{id}/run", s.runPipeline)
}

// runPipeline launches a pipeline run in the background and returns 202. The run
// uses its own context (not the request's) so it survives the response.
func (s *Server) runPipeline(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	go func() {
		if _, err := s.runner.Run(context.Background(), id); err != nil {
			s.logger.Error("pipeline run failed", "pipeline_id", id, "err", err)
		}
	}()
	writeJSON(w, http.StatusAccepted, map[string]any{"status": "started", "pipeline_id": id})
}

// actor reads the X-Actor header, defaulting to "system" (no auth in B1).
func actor(r *http.Request) string {
	if a := r.Header.Get("X-Actor"); a != "" {
		return a
	}
	return "system"
}

func (s *Server) createPipeline(w http.ResponseWriter, r *http.Request) {
	var req pipelines.CreatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	p, err := s.pipelines.Create(r.Context(), actor(r), req)
	if err != nil {
		writeWriteErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, p)
}

func (s *Server) updatePipeline(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	var req pipelines.CreatePipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return
	}
	p, err := s.pipelines.Update(r.Context(), actor(r), id, req)
	if err != nil {
		writeWriteErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (s *Server) statusSetter(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := pathID(w, r)
		if !ok {
			return
		}
		if err := s.pipelines.SetStatus(r.Context(), actor(r), id, status); err != nil {
			writeWriteErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": status})
	}
}

func (s *Server) archivePipeline(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	if err := s.pipelines.SetStatus(r.Context(), actor(r), id, "archived"); err != nil {
		writeWriteErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) rollbackPipeline(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	target, err := strconv.Atoi(chi.URLParam(r, "version"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid version"})
		return
	}
	p, err := s.pipelines.Rollback(r.Context(), actor(r), id, target)
	if err != nil {
		writeWriteErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, p)
}

// writeWriteErr maps domain errors to status codes without leaking internals.
func writeWriteErr(w http.ResponseWriter, err error) {
	var ve *pipelines.ValidationError
	var nf *pipelines.NotFoundError
	switch {
	case errors.As(err, &ve):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": ve.Error()})
	case errors.As(err, &nf):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": nf.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}

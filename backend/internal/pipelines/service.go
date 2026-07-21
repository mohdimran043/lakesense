package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"strings"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/configver"
	"github.com/lakesense/lakesense/backend/internal/envs"
)

// Repo is the consumer-side persistence seam for the write path. Multi-table
// mutations are atomic inside the implementation (a pgx transaction).
type Repo interface {
	EnsureEnvironment(ctx context.Context, slug string) (int64, error)
	CreatePipeline(ctx context.Context, envID int64, p PipelineRow, v configver.Version, cfgJSON []byte) (int64, error)
	GetPipeline(ctx context.Context, id int64) (PipelineRow, bool, error)
	ConfigHistory(ctx context.Context, id int64) ([]configver.Version, error)
	UpdatePipeline(ctx context.Context, id int64, p PipelineRow, v configver.Version, cfgJSON []byte, newVersion bool) error
	SetStatus(ctx context.Context, id int64, status string) error
}

// Service is the pipeline write-path domain logic.
type Service struct {
	repo Repo
	rec  audit.Recorder
	now  func() time.Time
}

// NewService builds a Service. now is injectable for deterministic tests.
func NewService(repo Repo, rec audit.Recorder, now func() time.Time) *Service {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Service{repo: repo, rec: rec, now: now}
}

// Create validates the request, writes the pipeline + config version 1 in one
// transaction, and records a pipeline.create audit entry.
func (s *Service) Create(ctx context.Context, actor string, req CreatePipelineRequest) (Pipeline, error) {
	if err := validate(req); err != nil {
		return Pipeline{}, err
	}
	envSlug := req.Environment
	if envSlug == "" {
		envSlug = "dev"
	}
	cfg := toConfig(req)
	now := s.now()
	v, _, err := configver.NewVersion(nil, cfg, "initial", actor, now)
	if err != nil {
		return Pipeline{}, fmt.Errorf("build config version: %w", err)
	}
	cfgJSON, err := json.Marshal(cfg)
	if err != nil {
		return Pipeline{}, fmt.Errorf("marshal config: %w", err)
	}
	envID, err := s.repo.EnsureEnvironment(ctx, envSlug)
	if err != nil {
		return Pipeline{}, err
	}
	row := rowFrom(req, cfg, "active", v.Number)
	id, err := s.repo.CreatePipeline(ctx, envID, row, v, cfgJSON)
	if err != nil {
		return Pipeline{}, fmt.Errorf("create pipeline: %w", err)
	}
	if err := audit.Log(ctx, s.rec, actor, "pipeline.create", "pipeline", fmt.Sprint(id), nil, cfg, now); err != nil {
		return Pipeline{}, fmt.Errorf("audit create: %w", err)
	}
	return viewFrom(id, envSlug, row), nil
}

// Update replaces a pipeline's config, creating a new version only when the
// canonical YAML actually changed (configver dedup), and audits before/after.
func (s *Service) Update(ctx context.Context, actor string, id int64, req CreatePipelineRequest) (Pipeline, error) {
	if err := validate(req); err != nil {
		return Pipeline{}, err
	}
	existing, ok, err := s.repo.GetPipeline(ctx, id)
	if err != nil {
		return Pipeline{}, err
	}
	if !ok {
		return Pipeline{}, &NotFoundError{ID: id}
	}
	history, err := s.repo.ConfigHistory(ctx, id)
	if err != nil {
		return Pipeline{}, err
	}
	var before any
	if n := len(history); n > 0 {
		before = history[n-1].YAML
	}
	cfg := toConfig(req)
	now := s.now()
	v, isNew, err := configver.NewVersion(history, cfg, "update", actor, now)
	if err != nil {
		return Pipeline{}, err
	}
	cfgJSON, _ := json.Marshal(cfg)
	row := rowFrom(req, cfg, existing.Status, v.Number)
	if err := s.repo.UpdatePipeline(ctx, id, row, v, cfgJSON, isNew); err != nil {
		return Pipeline{}, fmt.Errorf("update pipeline: %w", err)
	}
	if err := audit.Log(ctx, s.rec, actor, "pipeline.update", "pipeline", fmt.Sprint(id), before, v.YAML, now); err != nil {
		return Pipeline{}, fmt.Errorf("audit update: %w", err)
	}
	return viewFrom(id, "", row), nil
}

// Promote clones a pipeline's latest config into a target environment with
// per-endpoint credential overrides, creating a new pipeline there. Sensitive
// settings that were not overridden are rejected so a dev credential never
// silently leaks into prod.
func (s *Service) Promote(ctx context.Context, actor string, sourceID int64, targetEnv string, o envs.Overrides) (Pipeline, error) {
	if targetEnv == "" {
		return Pipeline{}, &ValidationError{Msg: "target environment is required"}
	}
	if _, ok, err := s.repo.GetPipeline(ctx, sourceID); err != nil {
		return Pipeline{}, err
	} else if !ok {
		return Pipeline{}, &NotFoundError{ID: sourceID}
	}
	history, err := s.repo.ConfigHistory(ctx, sourceID)
	if err != nil {
		return Pipeline{}, err
	}
	if len(history) == 0 {
		return Pipeline{}, &ValidationError{Msg: "source pipeline has no config to promote"}
	}
	cfg, err := configver.Parse(history[len(history)-1].YAML)
	if err != nil {
		return Pipeline{}, fmt.Errorf("parse source config: %w", err)
	}
	if missing := envs.MissingCredentials(cfg, o); len(missing) > 0 {
		return Pipeline{}, &ValidationError{Msg: "missing credential overrides for target: " + strings.Join(missing, ", ")}
	}
	promoted := envs.Promote(cfg, o)
	req := requestFromConfig(promoted)
	req.Environment = targetEnv
	return s.Create(ctx, actor, req)
}

// ApplyYAML imports a canonical pipeline-as-code document, applying it as an
// update (which creates a new version only when the config actually changed).
func (s *Service) ApplyYAML(ctx context.Context, actor string, id int64, yamlDoc string) (Pipeline, error) {
	cfg, err := configver.Parse(yamlDoc)
	if err != nil {
		return Pipeline{}, &ValidationError{Msg: err.Error()}
	}
	return s.Update(ctx, actor, id, requestFromConfig(cfg))
}

// requestFromConfig maps a canonical config back to an update request.
func requestFromConfig(cfg configver.Config) CreatePipelineRequest {
	streams := make([]Stream, len(cfg.Streams))
	for i, st := range cfg.Streams {
		streams[i] = Stream{Name: st.Name, Mode: st.Mode, CursorField: st.CursorField}
	}
	return CreatePipelineRequest{
		Name:        cfg.Name,
		Source:      Endpoint{Type: cfg.Source.Type, Settings: cfg.Source.Settings},
		Destination: Endpoint{Type: cfg.Destination.Type, Settings: cfg.Destination.Settings},
		Schedule:    cfg.Schedule,
		Streams:     streams,
	}
}

// Rollback appends a new version whose config equals a prior version's.
func (s *Service) Rollback(ctx context.Context, actor string, id int64, target int) (Pipeline, error) {
	existing, ok, err := s.repo.GetPipeline(ctx, id)
	if err != nil {
		return Pipeline{}, err
	}
	if !ok {
		return Pipeline{}, &NotFoundError{ID: id}
	}
	history, err := s.repo.ConfigHistory(ctx, id)
	if err != nil {
		return Pipeline{}, err
	}
	now := s.now()
	v, err := configver.Rollback(history, target, actor, now)
	if err != nil {
		return Pipeline{}, &ValidationError{Msg: err.Error()}
	}
	cfg, err := configver.Parse(v.YAML)
	if err != nil {
		return Pipeline{}, fmt.Errorf("parse rolled-back config: %w", err)
	}
	cfgJSON, _ := json.Marshal(cfg)
	row := existing
	row.CurrentVersion = v.Number
	if err := s.repo.UpdatePipeline(ctx, id, row, v, cfgJSON, true); err != nil {
		return Pipeline{}, fmt.Errorf("apply rollback: %w", err)
	}
	if err := audit.Log(ctx, s.rec, actor, "pipeline.rollback", "pipeline", fmt.Sprint(id), nil, map[string]int{"to_version": target}, now); err != nil {
		return Pipeline{}, fmt.Errorf("audit rollback: %w", err)
	}
	return viewFrom(id, "", row), nil
}

// SetStatus flips a pipeline's status (active/paused/archived) and audits it.
func (s *Service) SetStatus(ctx context.Context, actor string, id int64, status string) error {
	action := map[string]string{"active": "pipeline.resume", "paused": "pipeline.pause", "archived": "pipeline.archive"}[status]
	if action == "" {
		return &ValidationError{Msg: "status must be active, paused, or archived"}
	}
	if _, ok, err := s.repo.GetPipeline(ctx, id); err != nil {
		return err
	} else if !ok {
		return &NotFoundError{ID: id}
	}
	if err := s.repo.SetStatus(ctx, id, status); err != nil {
		return fmt.Errorf("set status: %w", err)
	}
	return audit.Log(ctx, s.rec, actor, action, "pipeline", fmt.Sprint(id), nil, map[string]string{"status": status}, s.now())
}

// rowFrom assembles the persisted row from a request + canonical config.
func rowFrom(req CreatePipelineRequest, cfg configver.Config, status string, version int) PipelineRow {
	srcJSON, _ := json.Marshal(req.Source)
	dstJSON, _ := json.Marshal(req.Destination)
	catJSON, _ := json.Marshal(map[string]any{"streams": req.Streams})
	return PipelineRow{
		Name:              cfg.Name,
		Slug:              slugify(cfg.Name),
		SourceType:        cfg.Source.Type,
		DestinationType:   cfg.Destination.Type,
		Schedule:          cfg.Schedule,
		Status:            status,
		SourceConfig:      srcJSON,
		DestinationConfig: dstJSON,
		Catalog:           catJSON,
		CurrentVersion:    version,
	}
}

// viewFrom builds the API view of a persisted pipeline.
func viewFrom(id int64, env string, row PipelineRow) Pipeline {
	return Pipeline{
		ID: id, Name: row.Name, Slug: row.Slug, Environment: env,
		SourceType: row.SourceType, DestinationType: row.DestinationType,
		Status: row.Status, Schedule: row.Schedule, CurrentVersion: row.CurrentVersion,
	}
}

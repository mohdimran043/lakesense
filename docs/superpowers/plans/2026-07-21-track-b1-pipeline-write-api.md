# Track B1 — Pipeline Write API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the control plane create, update, pause, and delete pipelines through the API, each mutation atomically writing a numbered config version and an append-only audit entry.

**Architecture:** A new `backend/internal/pipelines` package holds a `Service` with pure domain logic over a consumer-side `Repo` interface and an `audit.Recorder`. A pgx-backed `pgRepo` does each multi-table mutation in one transaction. Thin HTTP handlers in `api/writes.go` decode requests, call the service, and map errors to status codes.

**Tech Stack:** Go 1.26, chi router, pgx/pgxpool, existing `configver` + `audit` packages, `testify`, Docker Postgres for env-gated integration tests.

## Global Constraints

- Go module `github.com/lakesense/lakesense/backend`; standard `cmd/`+`internal/` layout.
- Interfaces defined consumer-side: `Repo` lives in `pipelines`, implemented by `pgRepo`.
- Errors wrapped `%w`, handled at boundaries; every DB call takes `context.Context`.
- Multi-table mutations are atomic — the transaction lives inside the `Repo` method, never in the service or handler.
- Audit = explicit `audit.Log(...)` after a successful mutation (not middleware). Actor from `X-Actor` header, default `"system"`.
- HTTP error bodies never leak SQL (match existing `writeErr`): validation→400, not-found→404, else→500.
- Compose the already-built `configver` (canonical YAML, `NewVersion` dedup, `Rollback`) and `audit` (`Recorder`, `Log`) — do not fork them.
- Table-driven `testify` tests, `-race`. Integration tests gate on `LAKESENSE_TEST_DB` (a full DSN) and skip when unset. `make check` must pass with them skipped.
- Commit per task. Branch `track-b1-pipeline-write-api`; PR to `main` when green. `PROGRESS.md` is gitignored — update locally, do not `git add` it.

## Reference facts (verified in the codebase)

- `pipelines` columns: `environment_id, name, slug, source_type, source_config, destination_type, destination_config, catalog, schedule, status, current_version` (+ id, timestamps). `UNIQUE (environment_id, slug)`. `status IN ('active','paused','archived')`.
- `pipeline_config_versions`: `pipeline_id, version, yaml, config, note, created_by`. `UNIQUE (pipeline_id, version)`.
- `environments`: `name, slug, kind` (`kind IN ('dev','staging','prod')`), `slug` UNIQUE.
- `audit.NewPgRecorder(pool)`; `audit.Log(ctx, recorder, actor, action, entityType, entityID, before, after any, now)`.
- `configver.Config{Name, Source Endpoint, Destination Endpoint, Schedule, Streams []Stream}`; `Endpoint{Type, Settings map[string]string}`; `Stream{Name, Mode, CursorField}`; `configver.NewVersion(history []Version, c Config, note, by string, now) (Version, bool, error)`; `configver.Rollback(history, target, by, now) (Version, error)`; `Version{Number, YAML, Note, CreatedBy, CreatedAt}`.
- `store.Migrate(dsn)` applies the embedded schema; `store.Open(ctx, dsn)` returns `*store.Store{Pool}`.

---

## File Structure

- `backend/internal/pipelines/pipelines.go` — domain types (requests, `Pipeline`, `PipelineRow`), `toConfig`, `deriveColumns`, `slugify`, validation.
- `backend/internal/pipelines/service.go` — `Service`, `Repo` interface, Create/Update/Rollback/SetStatus ops.
- `backend/internal/pipelines/pgrepo.go` — `pgRepo` (transactional pgx implementation).
- `backend/internal/pipelines/service_test.go` — fake-Repo/fake-Recorder unit tests.
- `backend/internal/pipelines/pgrepo_test.go` — env-gated Docker-Postgres integration tests.
- `backend/internal/api/writes.go` — HTTP handlers + `registerWrites`.
- `backend/internal/api/writes_test.go` — httptest handler tests over a fake Repo.
- Modify: `backend/internal/api/api.go` (call `s.registerWrites(r)` and hold the service).

---

### Task 1: pipelines domain types, validation, and Service.Create

**Files:**
- Create: `backend/internal/pipelines/pipelines.go`
- Create: `backend/internal/pipelines/service.go`
- Test: `backend/internal/pipelines/service_test.go`

**Interfaces:**
- Produces: `Service`, `Repo`, `NewService(repo Repo, rec audit.Recorder, now func() time.Time) *Service`, `Service.Create(ctx, actor string, req CreatePipelineRequest) (Pipeline, error)`, `PipelineRow`, `CreatePipelineRequest`, `Endpoint`, `Stream`, `Pipeline`, `ValidationError`.

- [ ] **Step 1: Write the failing test**

Create `backend/internal/pipelines/service_test.go`:
```go
package pipelines

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/configver"
)

// fakeRepo is an in-memory Repo for service unit tests.
type fakeRepo struct {
	envs      map[string]int64
	pipelines map[int64]PipelineRow
	history   map[int64][]configver.Version
	nextID    int64
	failNext  error // when set, the next mutating call returns it
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{envs: map[string]int64{}, pipelines: map[int64]PipelineRow{}, history: map[int64][]configver.Version{}, nextID: 1}
}

func (f *fakeRepo) EnsureEnvironment(_ context.Context, slug string) (int64, error) {
	if id, ok := f.envs[slug]; ok {
		return id, nil
	}
	id := int64(100 + len(f.envs))
	f.envs[slug] = id
	return id, nil
}
func (f *fakeRepo) CreatePipeline(_ context.Context, _ int64, p PipelineRow, v configver.Version, _ []byte) (int64, error) {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return 0, err
	}
	id := f.nextID
	f.nextID++
	f.pipelines[id] = p
	f.history[id] = []configver.Version{v}
	return id, nil
}
func (f *fakeRepo) GetPipeline(_ context.Context, id int64) (PipelineRow, bool, error) {
	p, ok := f.pipelines[id]
	return p, ok, nil
}
func (f *fakeRepo) ConfigHistory(_ context.Context, id int64) ([]configver.Version, error) {
	return f.history[id], nil
}
func (f *fakeRepo) UpdatePipeline(_ context.Context, id int64, p PipelineRow, v configver.Version, _ []byte, newVersion bool) error {
	f.pipelines[id] = p
	if newVersion {
		f.history[id] = append(f.history[id], v)
	}
	return nil
}
func (f *fakeRepo) SetStatus(_ context.Context, id int64, status string) error {
	p := f.pipelines[id]
	p.Status = status
	f.pipelines[id] = p
	return nil
}

// fakeRecorder captures audit entries.
type fakeRecorder struct{ entries []audit.Entry }

func (r *fakeRecorder) Record(_ context.Context, e audit.Entry) error {
	r.entries = append(r.entries, e)
	return nil
}

func fixedNow() time.Time { return time.Date(2026, 7, 21, 0, 0, 0, 0, time.UTC) }

func sampleReq() CreatePipelineRequest {
	return CreatePipelineRequest{
		Name:        "Orders to Lake",
		Environment: "dev",
		Source:      Endpoint{Type: "postgres", Settings: map[string]string{"host": "db"}},
		Destination: Endpoint{Type: "parquet", Settings: map[string]string{"path": "./out"}},
		Schedule:    "@daily",
		Streams:     []Stream{{Name: "public.orders", Mode: "full_load"}},
	}
}

func TestCreatePersistsV1AndAudits(t *testing.T) {
	repo := newFakeRepo()
	rec := &fakeRecorder{}
	svc := NewService(repo, rec, fixedNow)

	p, err := svc.Create(context.Background(), "alice", sampleReq())
	require.NoError(t, err)
	require.Equal(t, "postgres", p.SourceType)
	require.Equal(t, "parquet", p.DestinationType)
	require.Equal(t, "orders-to-lake", p.Slug)
	require.Equal(t, 1, p.CurrentVersion)

	require.Len(t, repo.history[p.ID], 1)
	require.Equal(t, 1, repo.history[p.ID][0].Number)

	require.Len(t, rec.entries, 1)
	require.Equal(t, "pipeline.create", rec.entries[0].Action)
	require.Equal(t, "alice", rec.entries[0].Actor)
}

func TestCreateValidationRejectsAndDoesNotPersist(t *testing.T) {
	repo := newFakeRepo()
	rec := &fakeRecorder{}
	svc := NewService(repo, rec, fixedNow)

	bad := sampleReq()
	bad.Name = ""
	_, err := svc.Create(context.Background(), "alice", bad)

	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	require.Empty(t, repo.pipelines, "no pipeline persisted on validation failure")
	require.Empty(t, rec.entries, "no audit entry on validation failure")
}

func TestCreateIncrementalRequiresCursor(t *testing.T) {
	svc := NewService(newFakeRepo(), &fakeRecorder{}, fixedNow)
	bad := sampleReq()
	bad.Streams = []Stream{{Name: "public.orders", Mode: "incremental"}} // no cursor
	_, err := svc.Create(context.Background(), "alice", bad)
	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./internal/pipelines/ -run TestCreate`
Expected: FAIL — `undefined: NewService` etc.

- [ ] **Step 3: Implement `pipelines.go`**

Create `backend/internal/pipelines/pipelines.go`:
```go
// Package pipelines is the control plane's pipeline write path: create, update,
// pause, and delete pipelines, each mutation composing a versioned config
// (configver) and an append-only audit entry (audit) atomically. It is the
// keystone the create-pipeline UI, the runner (B2), and the live workers (B3)
// build on.
package pipelines

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/lakesense/lakesense/backend/internal/configver"
)

// Endpoint is a source or destination in a create/update request.
type Endpoint struct {
	Type     string            `json:"type"`
	Settings map[string]string `json:"settings,omitempty"`
}

// Stream is one selected stream in a request.
type Stream struct {
	Name        string `json:"name"`
	Mode        string `json:"mode"`
	CursorField string `json:"cursor_field,omitempty"`
}

// CreatePipelineRequest is the POST body; it maps directly onto configver.Config.
type CreatePipelineRequest struct {
	Name        string   `json:"name"`
	Environment string   `json:"environment"`
	Source      Endpoint `json:"source"`
	Destination Endpoint `json:"destination"`
	Schedule    string   `json:"schedule"`
	Streams     []Stream `json:"streams"`
}

// Pipeline is the write-side view returned by create/update.
type Pipeline struct {
	ID              int64  `json:"id"`
	Name            string `json:"name"`
	Slug            string `json:"slug"`
	Environment     string `json:"environment"`
	SourceType      string `json:"source_type"`
	DestinationType string `json:"destination_type"`
	Status          string `json:"status"`
	Schedule        string `json:"schedule"`
	CurrentVersion  int    `json:"current_version"`
}

// PipelineRow is the persisted shape the Repo writes.
type PipelineRow struct {
	Name              string
	Slug              string
	SourceType        string
	DestinationType   string
	Schedule          string
	Status            string
	SourceConfig      []byte // JSONB
	DestinationConfig []byte // JSONB
	Catalog           []byte // JSONB
	CurrentVersion    int
}

// ValidationError is a 400-mapped rejection of a bad request.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

// NotFoundError is a 404-mapped missing-entity error.
type NotFoundError struct{ ID int64 }

func (e *NotFoundError) Error() string { return fmt.Sprintf("pipeline %d not found", e.ID) }

var validModes = map[string]bool{"full_load": true, "incremental": true, "cdc": true}

// validate checks a request, returning a *ValidationError on the first problem.
func validate(req CreatePipelineRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return &ValidationError{"name is required"}
	}
	if req.Source.Type == "" {
		return &ValidationError{"source.type is required"}
	}
	if req.Destination.Type == "" {
		return &ValidationError{"destination.type is required"}
	}
	if len(req.Streams) == 0 {
		return &ValidationError{"at least one stream is required"}
	}
	for _, s := range req.Streams {
		if s.Name == "" {
			return &ValidationError{"every stream needs a name"}
		}
		if !validModes[s.Mode] {
			return &ValidationError{fmt.Sprintf("stream %s: mode must be full_load, incremental, or cdc", s.Name)}
		}
		if s.Mode == "incremental" && s.CursorField == "" {
			return &ValidationError{fmt.Sprintf("stream %s: incremental mode requires a cursor_field", s.Name)}
		}
	}
	return nil
}

// toConfig maps a request onto the canonical configver.Config.
func toConfig(req CreatePipelineRequest) configver.Config {
	streams := make([]configver.Stream, len(req.Streams))
	for i, s := range req.Streams {
		streams[i] = configver.Stream{Name: s.Name, Mode: s.Mode, CursorField: s.CursorField}
	}
	return configver.Config{
		Name:        req.Name,
		Source:      configver.Endpoint{Type: req.Source.Type, Settings: req.Source.Settings},
		Destination: configver.Endpoint{Type: req.Destination.Type, Settings: req.Destination.Settings},
		Schedule:    req.Schedule,
		Streams:     streams,
	}
}

var slugRe = regexp.MustCompile(`[^a-z0-9]+`)

// slugify makes a URL-safe, lowercase slug from a name.
func slugify(name string) string {
	s := slugRe.ReplaceAllString(strings.ToLower(name), "-")
	return strings.Trim(s, "-")
}
```

- [ ] **Step 4: Implement `service.go`** (Create only for this task; Update/Rollback/SetStatus land in Task 2)

Create `backend/internal/pipelines/service.go`:
```go
package pipelines

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/configver"
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd backend && go test -race ./internal/pipelines/`
Expected: PASS (create + two validation cases).

- [ ] **Step 6: Commit**

```bash
git add backend/internal/pipelines/pipelines.go backend/internal/pipelines/service.go backend/internal/pipelines/service_test.go
git commit -m "feat(backend): pipelines write service — Create with versioning + audit (B1)"
```

---

### Task 2: Update, Rollback, and status transitions

**Files:**
- Modify: `backend/internal/pipelines/service.go` (add Update, Rollback, Pause/Resume/Archive)
- Test: `backend/internal/pipelines/service_test.go` (add cases)

**Interfaces:**
- Produces: `Service.Update(ctx, actor string, id int64, req CreatePipelineRequest) (Pipeline, error)`, `Service.Rollback(ctx, actor string, id int64, target int) (Pipeline, error)`, `Service.SetStatus(ctx, actor string, id int64, status string) error`.

- [ ] **Step 1: Write the failing test**

Add to `service_test.go`:
```go
func TestUpdateChangedConfigCreatesV2(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo, &fakeRecorder{}, fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())

	changed := sampleReq()
	changed.Schedule = "@hourly"
	p2, err := svc.Update(context.Background(), "bob", p.ID, changed)
	require.NoError(t, err)
	require.Equal(t, 2, p2.CurrentVersion)
	require.Len(t, repo.history[p.ID], 2)
}

func TestUpdateIdenticalConfigCreatesNoNewVersion(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo, &fakeRecorder{}, fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())

	p2, err := svc.Update(context.Background(), "bob", p.ID, sampleReq()) // identical
	require.NoError(t, err)
	require.Equal(t, 1, p2.CurrentVersion, "no-op change keeps version 1")
	require.Len(t, repo.history[p.ID], 1)
}

func TestRollbackAppendsRestoringVersion(t *testing.T) {
	repo := newFakeRepo()
	svc := NewService(repo, &fakeRecorder{}, fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())
	changed := sampleReq()
	changed.Schedule = "@hourly"
	_, _ = svc.Update(context.Background(), "bob", p.ID, changed) // v2

	p3, err := svc.Rollback(context.Background(), "carol", p.ID, 1)
	require.NoError(t, err)
	require.Equal(t, 3, p3.CurrentVersion)
	require.Equal(t, repo.history[p.ID][0].YAML, repo.history[p.ID][2].YAML, "v3 restores v1 content")
}

func TestUpdateNotFound(t *testing.T) {
	svc := NewService(newFakeRepo(), &fakeRecorder{}, fixedNow)
	_, err := svc.Update(context.Background(), "bob", 999, sampleReq())
	var nf *NotFoundError
	require.ErrorAs(t, err, &nf)
}

func TestArchiveSetsStatusAndAudits(t *testing.T) {
	repo := newFakeRepo()
	rec := &fakeRecorder{}
	svc := NewService(repo, rec, fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())

	require.NoError(t, svc.SetStatus(context.Background(), "bob", p.ID, "archived"))
	require.Equal(t, "archived", repo.pipelines[p.ID].Status)
	require.Equal(t, "pipeline.archive", rec.entries[len(rec.entries)-1].Action)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./internal/pipelines/ -run 'TestUpdate|TestRollback|TestArchive'`
Expected: FAIL — `undefined: (*Service).Update` etc.

- [ ] **Step 3: Implement the operations**

Add to `service.go`:
```go
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && go test -race ./internal/pipelines/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/internal/pipelines/service.go backend/internal/pipelines/service_test.go
git commit -m "feat(backend): pipelines Update/Rollback/SetStatus with dedup + audit (B1)"
```

---

### Task 3: pgRepo — transactional Postgres implementation + integration tests

**Files:**
- Create: `backend/internal/pipelines/pgrepo.go`
- Test: `backend/internal/pipelines/pgrepo_test.go`

**Interfaces:**
- Produces: `NewPgRepo(pool *pgxpool.Pool) *pgRepo` implementing `Repo`.

- [ ] **Step 1: Write the failing test (env-gated)**

Create `backend/internal/pipelines/pgrepo_test.go`:
```go
package pipelines

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/store"
)

// testPool connects to LAKESENSE_TEST_DB and migrates it, or skips.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LAKESENSE_TEST_DB")
	if dsn == "" {
		t.Skip("set LAKESENSE_TEST_DB to a throwaway Postgres DSN to run pgRepo integration tests")
	}
	require.NoError(t, store.Migrate(dsn))
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	// Clean slate for repeatable runs.
	_, err = pool.Exec(context.Background(), `TRUNCATE pipelines, environments, pipeline_config_versions, audit_log RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	return pool
}

func TestPgCreatePersistsAtomically(t *testing.T) {
	pool := testPool(t)
	svc := NewService(NewPgRepo(pool), audit.NewPgRecorder(pool), fixedNow)

	p, err := svc.Create(context.Background(), "alice", sampleReq())
	require.NoError(t, err)
	require.NotZero(t, p.ID)

	var versions, audits int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pipeline_config_versions WHERE pipeline_id=$1`, p.ID).Scan(&versions))
	require.Equal(t, 1, versions)
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM audit_log WHERE entity_type='pipeline' AND action='pipeline.create'`).Scan(&audits))
	require.Equal(t, 1, audits)
}

func TestPgDuplicateSlugRollsBack(t *testing.T) {
	pool := testPool(t)
	svc := NewService(NewPgRepo(pool), audit.NewPgRecorder(pool), fixedNow)
	_, err := svc.Create(context.Background(), "alice", sampleReq())
	require.NoError(t, err)

	// Same name in the same env → duplicate (environment_id, slug) → error.
	_, err = svc.Create(context.Background(), "alice", sampleReq())
	require.Error(t, err)

	var versions int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pipeline_config_versions`).Scan(&versions))
	require.Equal(t, 1, versions, "the failed create left no orphan version row")
}

func TestPgUpdateAndHistory(t *testing.T) {
	pool := testPool(t)
	svc := NewService(NewPgRepo(pool), audit.NewPgRecorder(pool), fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())
	changed := sampleReq()
	changed.Schedule = "@hourly"
	p2, err := svc.Update(context.Background(), "bob", p.ID, changed)
	require.NoError(t, err)
	require.Equal(t, 2, p2.CurrentVersion)

	var maxV int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT max(version) FROM pipeline_config_versions WHERE pipeline_id=$1`, p.ID).Scan(&maxV))
	require.Equal(t, 2, maxV)
}
```

- [ ] **Step 2: Run test to verify it fails / skips**

Run: `cd backend && go test ./internal/pipelines/ -run TestPg`
Expected: SKIP (no `LAKESENSE_TEST_DB`) OR compile error `undefined: NewPgRepo` — it must compile, so the failure here is the undefined symbol. (After Step 3 it compiles and skips without the env.)

- [ ] **Step 3: Implement `pgrepo.go`**

Create `backend/internal/pipelines/pgrepo.go`:
```go
package pipelines

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lakesense/lakesense/backend/internal/configver"
)

// pgRepo is the Postgres-backed Repo. Multi-table mutations run in one
// transaction so a pipeline and its config version are always consistent.
type pgRepo struct{ pool *pgxpool.Pool }

// NewPgRepo builds a Postgres Repo over the pool.
func NewPgRepo(pool *pgxpool.Pool) *pgRepo { return &pgRepo{pool: pool} }

func (r *pgRepo) EnsureEnvironment(ctx context.Context, slug string) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`INSERT INTO environments (name, slug, kind) VALUES ($1, $1, 'dev')
		 ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`, slug).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ensure environment %s: %w", slug, err)
	}
	return id, nil
}

func (r *pgRepo) CreatePipeline(ctx context.Context, envID int64, p PipelineRow, v configver.Version, cfgJSON []byte) (int64, error) {
	var id int64
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO pipelines (environment_id, name, slug, source_type, source_config,
			     destination_type, destination_config, catalog, schedule, status, current_version)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id`,
			envID, p.Name, p.Slug, p.SourceType, p.SourceConfig, p.DestinationType,
			p.DestinationConfig, p.Catalog, p.Schedule, p.Status, p.CurrentVersion).Scan(&id); err != nil {
			return fmt.Errorf("insert pipeline: %w", err)
		}
		return insertVersion(ctx, tx, id, v, cfgJSON)
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *pgRepo) GetPipeline(ctx context.Context, id int64) (PipelineRow, bool, error) {
	var p PipelineRow
	err := r.pool.QueryRow(ctx,
		`SELECT name, slug, source_type, destination_type, schedule, status,
		        source_config, destination_config, catalog, current_version
		 FROM pipelines WHERE id = $1`, id).Scan(
		&p.Name, &p.Slug, &p.SourceType, &p.DestinationType, &p.Schedule, &p.Status,
		&p.SourceConfig, &p.DestinationConfig, &p.Catalog, &p.CurrentVersion)
	if err != nil {
		if err == pgx.ErrNoRows {
			return PipelineRow{}, false, nil
		}
		return PipelineRow{}, false, fmt.Errorf("get pipeline %d: %w", id, err)
	}
	return p, true, nil
}

func (r *pgRepo) ConfigHistory(ctx context.Context, id int64) ([]configver.Version, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT version, yaml, note, created_by, created_at
		 FROM pipeline_config_versions WHERE pipeline_id = $1 ORDER BY version`, id)
	if err != nil {
		return nil, fmt.Errorf("config history %d: %w", id, err)
	}
	defer rows.Close()
	var out []configver.Version
	for rows.Next() {
		var v configver.Version
		if err := rows.Scan(&v.Number, &v.YAML, &v.Note, &v.CreatedBy, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan version: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (r *pgRepo) UpdatePipeline(ctx context.Context, id int64, p PipelineRow, v configver.Version, cfgJSON []byte, newVersion bool) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE pipelines SET name=$2, slug=$3, source_type=$4, source_config=$5,
			     destination_type=$6, destination_config=$7, catalog=$8, schedule=$9,
			     status=$10, current_version=$11, updated_at=now()
			 WHERE id=$1`,
			id, p.Name, p.Slug, p.SourceType, p.SourceConfig, p.DestinationType,
			p.DestinationConfig, p.Catalog, p.Schedule, p.Status, p.CurrentVersion); err != nil {
			return fmt.Errorf("update pipeline: %w", err)
		}
		if newVersion {
			return insertVersion(ctx, tx, id, v, cfgJSON)
		}
		return nil
	})
}

func (r *pgRepo) SetStatus(ctx context.Context, id int64, status string) error {
	_, err := r.pool.Exec(ctx, `UPDATE pipelines SET status=$2, updated_at=now() WHERE id=$1`, id, status)
	if err != nil {
		return fmt.Errorf("set status %d: %w", id, err)
	}
	return nil
}

// insertVersion writes one config version row inside a transaction.
func insertVersion(ctx context.Context, tx pgx.Tx, pipelineID int64, v configver.Version, cfgJSON []byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO pipeline_config_versions (pipeline_id, version, yaml, config, note, created_by, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		pipelineID, v.Number, v.YAML, cfgJSON, v.Note, v.CreatedBy, v.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert config version %d: %w", v.Number, err)
	}
	return nil
}
```

- [ ] **Step 4: Run the integration tests against a throwaway Postgres**

Run:
```bash
docker run -d --rm --name lakesense-testdb -e POSTGRES_PASSWORD=pw -p 55432:5432 postgres:16
sleep 3
cd backend && LAKESENSE_TEST_DB="postgres://postgres:pw@localhost:55432/postgres?sslmode=disable" go test -race ./internal/pipelines/ -run TestPg -v
docker stop lakesense-testdb
```
Expected: the three `TestPg*` tests PASS.

- [ ] **Step 5: Confirm they skip cleanly without the env, then commit**

Run: `cd backend && go test ./internal/pipelines/ -run TestPg` → expect SKIP.
```bash
git add backend/internal/pipelines/pgrepo.go backend/internal/pipelines/pgrepo_test.go
git commit -m "feat(backend): pgRepo — transactional pipeline persistence + integration tests (B1)"
```

---

### Task 4: HTTP write handlers and router wiring

**Files:**
- Create: `backend/internal/api/writes.go`
- Modify: `backend/internal/api/api.go` (build the service; call `s.registerWrites`)
- Test: `backend/internal/api/writes_test.go`

**Interfaces:**
- Consumes: `pipelines.Service`, `pipelines.NewService`, `pipelines.NewPgRepo`, `audit.NewPgRecorder`.
- Produces: routes from the design's HTTP table; `Server.pipelines *pipelines.Service`.

- [ ] **Step 1: Write the failing test**

Create `backend/internal/api/writes_test.go`:
```go
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/configver"
	"github.com/lakesense/lakesense/backend/internal/pipelines"
)

// memRepo is a minimal in-memory pipelines.Repo for handler tests.
type memRepo struct {
	rows    map[int64]pipelines.PipelineRow
	hist    map[int64][]configver.Version
	next    int64
}

func newMemRepo() *memRepo {
	return &memRepo{rows: map[int64]pipelines.PipelineRow{}, hist: map[int64][]configver.Version{}, next: 1}
}
func (m *memRepo) EnsureEnvironment(context.Context, string) (int64, error) { return 1, nil }
func (m *memRepo) CreatePipeline(_ context.Context, _ int64, p pipelines.PipelineRow, v configver.Version, _ []byte) (int64, error) {
	id := m.next
	m.next++
	m.rows[id] = p
	m.hist[id] = []configver.Version{v}
	return id, nil
}
func (m *memRepo) GetPipeline(_ context.Context, id int64) (pipelines.PipelineRow, bool, error) {
	p, ok := m.rows[id]
	return p, ok, nil
}
func (m *memRepo) ConfigHistory(_ context.Context, id int64) ([]configver.Version, error) { return m.hist[id], nil }
func (m *memRepo) UpdatePipeline(_ context.Context, id int64, p pipelines.PipelineRow, v configver.Version, _ []byte, newV bool) error {
	m.rows[id] = p
	if newV {
		m.hist[id] = append(m.hist[id], v)
	}
	return nil
}
func (m *memRepo) SetStatus(_ context.Context, id int64, s string) error {
	p := m.rows[id]
	p.Status = s
	m.rows[id] = p
	return nil
}

type nopRecorder struct{}

func (nopRecorder) Record(context.Context, audit.Entry) error { return nil }

// testServer builds an api.Server with a service over memRepo, plus routes.
func testServer() http.Handler {
	svc := pipelines.NewService(newMemRepo(), nopRecorder{}, func() time.Time { return time.Unix(0, 0).UTC() })
	s := &Server{logger: slog.Default(), pipelines: svc}
	r := chiRouter(s) // small helper wiring registerWrites (see Step 3)
	return r
}

func TestCreatePipelineEndpoint(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"name": "Orders", "environment": "dev",
		"source":      map[string]any{"type": "postgres"},
		"destination": map[string]any{"type": "parquet"},
		"schedule":    "@daily",
		"streams":     []map[string]any{{"name": "public.orders", "mode": "full_load"}},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	req.Header.Set("X-Actor", "alice")
	rec := httptest.NewRecorder()
	testServer().ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var p pipelines.Pipeline
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p))
	require.Equal(t, "orders", p.Slug)
	require.Equal(t, 1, p.CurrentVersion)
}

func TestCreatePipelineValidation400(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"name": ""}) // invalid
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	testServer().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./internal/api/ -run TestCreatePipeline`
Expected: FAIL — `Server` has no `pipelines` field / `chiRouter` undefined.

- [ ] **Step 3: Implement `writes.go` and wire the router**

Create `backend/internal/api/writes.go`:
```go
package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/lakesense/lakesense/backend/internal/pipelines"
)

// registerWrites mounts the pipeline write endpoints. The read API (registerData)
// stays untouched; writes are additive.
func (s *Server) registerWrites(r chi.Router) {
	r.Post("/pipelines", s.createPipeline)
	r.Patch("/pipelines/{id}", s.updatePipeline)
	r.Post("/pipelines/{id}/pause", s.statusSetter("paused"))
	r.Post("/pipelines/{id}/resume", s.statusSetter("active"))
	r.Delete("/pipelines/{id}", s.archivePipeline)
	r.Post("/pipelines/{id}/rollback/{version}", s.rollbackPipeline)
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
```

Modify `backend/internal/api/api.go`:
- Add a `pipelines *pipelines.Service` field to `Server`.
- In `New`, build it from the pool and register the writes. Replace the `Server` construction + `Route` block:
```go
func New(pool *pgxpool.Pool, logger *slog.Logger) http.Handler {
	svc := pipelines.NewService(pipelines.NewPgRepo(pool), audit.NewPgRecorder(pool), nil)
	s := &Server{pool: pool, logger: logger, pipelines: svc}
	return chiRouter(s)
}

// chiRouter builds the router for a Server (shared by New and tests).
func chiRouter(s *Server) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))

	r.Get("/healthz", s.health)
	r.Get("/readyz", s.ready)
	r.Route("/api/v1", func(r chi.Router) {
		r.Get("/version", s.version)
		s.registerData(r)
		s.registerWrites(r)
	})
	return r
}
```
Add imports `audit` and `pipelines` to `api.go`. Add the field to the `Server` struct:
```go
type Server struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	pipelines *pipelines.Service
}
```
(`ready`/`readyz` still need the pool; tests that don't set `pool` never call `/readyz`, so a nil pool there is fine.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && go test -race ./internal/api/`
Expected: PASS (create 201, validation 400) plus existing api tests.

- [ ] **Step 5: Commit**

```bash
git add backend/internal/api/writes.go backend/internal/api/writes_test.go backend/internal/api/api.go
git commit -m "feat(backend): pipeline write HTTP endpoints wired into the API (B1)"
```

---

### Task 5: Full gate, integration smoke, and docs

**Files:**
- Modify: `README.md` (note that pipelines can now be created via the API)
- Modify: `PROGRESS.md` (local only)

- [ ] **Step 1: Run the full check**

Run: `cd /home/imran/Documents/github/LakeSense && make check`
Expected: lint 0, vet clean, `-race` tests pass (integration tests skip without the env), frontend+website build. Green.

- [ ] **Step 2: Run the integration suite once against Docker Postgres**

Run:
```bash
docker run -d --rm --name lakesense-testdb -e POSTGRES_PASSWORD=pw -p 55432:5432 postgres:16
sleep 3
cd backend && LAKESENSE_TEST_DB="postgres://postgres:pw@localhost:55432/postgres?sslmode=disable" go test -race ./internal/pipelines/ -v -run TestPg
docker stop lakesense-testdb
```
Expected: all `TestPg*` PASS.

- [ ] **Step 3: Update README + PROGRESS**

In `README.md`, update the "Create & run a pipeline" status note: pipelines can now be created/updated via the write API (`POST /api/v1/pipelines`), in addition to the engine CLI; the in-dashboard wizard consumes this next. In `PROGRESS.md` (local, gitignored) check the 4.10 audit write-path and 4.13 export/apply groundwork; add a Decisions Log line for the pipelines write-service seam. Do not `git add PROGRESS.md`.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs(backend): pipelines are now creatable via the write API (B1)"
```

---

## Self-Review

**1. Spec coverage.**
- Create/update/pause/resume/delete + versioning + audit → Tasks 1,2,4. ✓
- Atomic multi-table mutation in a transaction → Task 3 (`CreatePipeline`/`UpdatePipeline` use `pgx.BeginFunc`). ✓
- configver dedup (no-op change → no version) → Task 2 test `TestUpdateIdenticalConfigCreatesNoNewVersion`. ✓
- Rollback → Task 2. ✓
- Audit explicit, actor from header → Tasks 2/4. ✓
- Validation → 400, not-found → 404 → Tasks 1/2/4 (`writeWriteErr`). ✓
- Integration: atomic persist + tx rollback on failure → Task 3. ✓
- Testing without a DB (fake Repo) → Tasks 1/2/4. ✓

**2. Placeholder scan.** No TODO/TBD. Every code step shows complete code. The one indirection: Task 4 references `chiRouter(s)` which is fully defined in the same task's Step 3.

**3. Type consistency.** `Repo` method signatures match between `service.go` (Task 1), the fake (`service_test.go`), `memRepo` (Task 4), and `pgRepo` (Task 3): `CreatePipeline(ctx, envID int64, PipelineRow, configver.Version, []byte) (int64, error)`, `UpdatePipeline(ctx, id, PipelineRow, configver.Version, []byte, bool) error`, etc. `pipelines.Service` methods (`Create`, `Update`, `Rollback`, `SetStatus`) are used identically in handlers and tests. `audit.Log`/`audit.Recorder`/`audit.NewPgRecorder` and `configver.NewVersion`/`Rollback`/`Parse` match their verified signatures. `Server` gains a `pipelines *pipelines.Service` field used by both `New` and `chiRouter`.

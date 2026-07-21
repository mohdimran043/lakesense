# Track B1 — Pipeline Write API + Config Versioning + Audit (Design)

**Date:** 2026-07-21
**Spec refs:** `lakesense-final-prompt.md` §4.10 (audit), §4.13 (config versioning), §4.16b/g (create-pipeline wizard — the UI consumer)
**Status:** approved design, ready for implementation plan
**Parent:** Track B (control-plane write path). B1 is the first of four sub-projects
(B1 write API · B2 pipeline runner · B3 live intelligence workers · B4 remaining writes).

## Goal

Turn the read-only control plane into one that can **create, update, pause, and
delete pipelines through the API**, with every mutation automatically writing a
numbered config version (`configver`) and an append-only audit entry (`audit`),
atomically. This is the keystone that the create-pipeline wizard, the pipeline
runner (B2), and the live workers (B3) all stand on — today nothing can create a
pipeline except the `seed` command.

## Non-goals (deferred to later B sub-projects)

- Actually running a pipeline (invoking `lsengine`) — that is B2.
- Wiring rules/anomaly/quality/escalation onto live events — that is B3.
- Rules/channels/escalation CRUD, incident ack/snooze, env promotion, backfill
  launch — that is B4.
- Authentication / SSO / RBAC — deliberately the future Pro tier. B1 records an
  actor from a header, no auth.

## Architecture

A new package `backend/internal/pipelines` owns the write-side domain logic,
following the codebase's consumer-side-interface pattern (the read API keeps its
inline SQL; writes get a testable service seam because they compose three
concerns — persistence, versioning, audit — and must be transactional).

```
POST/PATCH/DELETE /api/v1/pipelines            handlers (api/writes.go)
        │  decode request → pipelines.Service
        ▼
pipelines.Service            ── configver (canonical YAML + NewVersion/Rollback)
   ├─ Repo (interface)       ── audit.Recorder (audit.Log)
   └─ now func() time.Time
        ▼
pgRepo (pipelines/pgrepo.go)   one pgx tx per mutation:
   pipelines + pipeline_config_versions written atomically
```

- **`pipelines.Service`** — pure domain logic; depends on `Repo` (interface),
  `audit.Recorder`, and a clock. Unit-tested with a fake Repo + fake Recorder.
- **`Repo`** — the consumer-side persistence interface (defined in `pipelines`,
  implemented by `pgRepo`). Methods are transactional where a mutation spans
  multiple tables.
- **`pgRepo`** — pgx-backed implementation using `*pgxpool.Pool`; env-gated
  Docker-Postgres integration tests.
- **`api/writes.go`** — thin handlers: decode, call the service, map errors to
  status codes, write JSON. Registered via `registerWrites(r)` next to the
  existing `registerData(r)`.

## Domain types

```go
// CreatePipelineRequest is the POST body; it maps directly onto configver.Config.
type CreatePipelineRequest struct {
    Name        string            `json:"name"`
    Environment string            `json:"environment"` // slug; default "dev"
    Source      Endpoint          `json:"source"`
    Destination Endpoint          `json:"destination"`
    Schedule    string            `json:"schedule"`
    Streams     []Stream          `json:"streams"`
}
type Endpoint struct {
    Type     string            `json:"type"`
    Settings map[string]string `json:"settings,omitempty"`
}
type Stream struct {
    Name string `json:"name"`; Mode string `json:"mode"`; CursorField string `json:"cursor_field,omitempty"`
}

// UpdatePipelineRequest is the PATCH body — same shape; a full config replace
// (simplest correct semantics; partial-field merge is a later refinement).
type UpdatePipelineRequest = CreatePipelineRequest

// Pipeline is the write-side view returned by create/update.
type Pipeline struct {
    ID int64; Name, Slug, Environment, SourceType, DestinationType, Status, Schedule string
    CurrentVersion int
}
```

`toConfig(req)` builds a `configver.Config`; `deriveColumns(cfg)` yields
`source_type = cfg.Source.Type`, `destination_type = cfg.Destination.Type`,
`slug = slugify(name)`.

## The Repo interface (consumer-side)

```go
type Repo interface {
    // EnsureEnvironment returns the id of the env with this slug, creating a
    // 'dev'-kind row when absent.
    EnsureEnvironment(ctx context.Context, slug string) (int64, error)
    // CreatePipeline inserts the pipeline row and its version-1 config row in one
    // transaction, returning the new pipeline id.
    CreatePipeline(ctx context.Context, envID int64, p PipelineRow, v configver.Version, cfgJSON []byte) (int64, error)
    // GetPipeline loads the row (for update's before-image and existence checks).
    GetPipeline(ctx context.Context, id int64) (PipelineRow, bool, error)
    // ConfigHistory returns all versions in order (for configver.NewVersion/Rollback).
    ConfigHistory(ctx context.Context, id int64) ([]configver.Version, error)
    // UpdatePipeline writes a new config version and updates the pipeline row in
    // one transaction (skip the version insert when v.Number == current).
    UpdatePipeline(ctx context.Context, id int64, p PipelineRow, v configver.Version, cfgJSON []byte, newVersion bool) error
    // SetStatus flips status (pause/resume/archive).
    SetStatus(ctx context.Context, id int64, status string) error
}

type PipelineRow struct {
    Name, Slug, SourceType, DestinationType, Schedule, Status string
    SourceConfig, DestinationConfig, Catalog []byte // JSONB
    CurrentVersion int
}
```

Transaction boundaries live inside `CreatePipeline`/`UpdatePipeline` — the
service never juggles a tx handle, keeping handler logic pure and the atomicity
guarantee co-located with the SQL.

## Service operations

- **Create:** validate → `toConfig` → `configver.NewVersion(nil, cfg, note, actor, now)`
  → `Repo.EnsureEnvironment` → `Repo.CreatePipeline` → `audit.Log("pipeline.create",
  before=nil, after=cfg)` → return `Pipeline`.
- **Update:** load row (404 if absent) → `Repo.ConfigHistory` →
  `configver.NewVersion(history, cfg, …)` (a no-op change yields `newVersion=false`
  and no new version) → `Repo.UpdatePipeline` → `audit.Log("pipeline.update",
  before=oldCfg, after=cfg)`.
- **Pause/Resume:** `Repo.SetStatus(paused|active)` → audit `pipeline.pause|resume`.
- **Archive (DELETE):** `Repo.SetStatus(archived)` (soft delete — diff/metric
  history is preserved) → audit `pipeline.archive`.
- **Rollback:** `configver.Rollback(history, target, actor, now)` → apply as a new
  version via `UpdatePipeline` → audit `pipeline.rollback`.

Validation (`validate(req)`): non-empty name, source.type, destination.type; each
stream has a name + a mode in {full_load, incremental, cdc}; incremental streams
carry a cursor_field. Returns a `*ValidationError` the handler maps to 400.

## Audit

Explicit `audit.Log(ctx, recorder, actor, action, "pipeline", id, before, after, now)`
calls after each successful mutation — the `audit` package is built exactly for
this (a generic HTTP middleware can't compute a domain before/after diff). Actor
comes from the `X-Actor` request header, default `"system"`.

## HTTP surface

| Method | Path | Body | Success |
|---|---|---|---|
| POST | `/api/v1/pipelines` | CreatePipelineRequest | 201 + Pipeline |
| PATCH | `/api/v1/pipelines/{id}` | UpdatePipelineRequest | 200 + Pipeline |
| POST | `/api/v1/pipelines/{id}/pause` | — | 200 |
| POST | `/api/v1/pipelines/{id}/resume` | — | 200 |
| DELETE | `/api/v1/pipelines/{id}` | — | 204 |
| GET | `/api/v1/pipelines/{id}/versions` | — | 200 + [Version] |
| POST | `/api/v1/pipelines/{id}/rollback/{version}` | — | 200 + Pipeline |

Error mapping: validation → 400; not found → 404; anything else → 500 (body
never leaks SQL, matching the existing `writeErr`).

## Testing (Rule 6 — write path gets the deepest tests)

**Unit (fake Repo + fake Recorder), no DB:**
1. Create builds a v1 config version and emits a `pipeline.create` audit entry.
2. Update with a changed config creates v2; update with an identical config
   creates **no** new version (configver dedup) but still succeeds.
3. Rollback to v1 appends a new version whose YAML equals v1's.
4. Validation rejects: empty name, missing source type, incremental stream with
   no cursor field → 400-mapped error, and **no** Repo/audit calls made.
5. Pause/resume/archive set the right status and audit action.

**Integration (env-gated `LAKESENSE_PG_IT=1`, Docker Postgres):**
6. A real POST persists the pipeline row + `pipeline_config_versions` v1 +
   `audit_log` row atomically; GET returns it.
7. A failing insert (e.g. duplicate slug in the same env) rolls the transaction
   back — no orphan pipeline row, no version row.

A helper spins a throwaway `postgres:16` container (or reuses `LAKESENSE_TEST_DB`
when provided), runs `store.Migrate`, and hands the pool to the test. `make check`
stays green with the integration tests skipped when the gate/env is absent.

## Rollout / commit plan

One commit per plan task on branch `track-b1-pipeline-write-api`; PR to `main`
when `make check` + the fake-Repo suite are green (integration tests run when
`LAKESENSE_PG_IT=1`). `PROGRESS.md` (gitignored) updated locally: 4.10 audit
write path + 4.13 export/apply groundwork advance.

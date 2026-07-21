# Track B2a — Pipeline Runner + Manual Trigger (Design)

**Date:** 2026-07-21
**Spec refs:** `lakesense-final-prompt.md` §4.1 (Event Collector launches/monitors lsengine runs)
**Status:** approved design, ready for implementation plan
**Parent:** Track B (control-plane write path). B2 = runner; this is B2a (runner +
manual trigger). B2b (scheduler) is a follow-on.

## Goal

Make a pipeline actually run: given a pipeline created via the B1 write API,
materialize its engine config files, invoke the bundled `lsengine` (discover →
sync), pipe the JSONL event stream through the existing collector `Ingester`, and
record the outcome — so real events, metrics, diff badges, and lineage flow into
the dashboard instead of only `seed` data. Expose it as `POST
/api/v1/pipelines/{id}/run`.

## Non-goals (deferred)

- Automatic scheduling (cron/ticker) — that is B2b.
- Live rules/anomaly/quality/escalation on the ingested events — that is B3.
- Backfill execution from the UI — later (engine `backfill` exists; the launch
  endpoint is B4).
- Typed (non-string) connector settings — B1 stores settings as
  `map[string]string`; B2a flattens those. Typed fields are a later refinement.

## Architecture

New package `backend/internal/runner`. Orchestration is decoupled from the
`lsengine` binary by an `Engine` seam, so the run logic is unit-testable without
executing anything.

```
Runner.Run(ctx, pipelineID)
  ├─ Loader.Load(id)               → sourceConfig, destConfig JSONB + stream selections
  ├─ scratch dir <DATA_DIR>/<id>/  (state.json persists across runs; configs rewritten)
  ├─ write source.json, dest.json  (flattenEndpoint: {type,settings} → {type, ...settings})
  ├─ Engine.Discover(ctx, source.json)      → discovered catalog (real stream schemas)
  ├─ buildCatalog(discovered, selections)   → catalog.json with selected_streams
  ├─ Engine.Sync(ctx, paths, pipelineID, w) → w is an io.Pipe writer
  │        (goroutine) Ingester.Ingest(ctx, pipelineID, pipeReader)
  └─ RunResult{Events int, SyncID string, Err error}
```

### The Engine seam

```go
// Engine abstracts the lsengine binary so the runner's orchestration is testable
// without a real process. execEngine runs the binary; tests use a fake.
type Engine interface {
    // Discover returns the JSON catalog document lsengine prints for a source.
    Discover(ctx context.Context, sourceConfigPath string) ([]byte, error)
    // Sync runs a replication and writes the JSONL event stream to out. A
    // non-zero engine exit returns an error, but whatever was written to out
    // before that is still valid and must be ingested by the caller.
    Sync(ctx context.Context, p SyncPaths, pipelineID int64, out io.Writer) error
}

type SyncPaths struct{ Source, Destination, Catalog, State string }
```

- `execEngine{path string}` — `path` is `config.EnginePath`. `Discover` runs
  `lsengine discover --config <src>` and captures stdout. `Sync` runs `lsengine
  sync --config <src> --destination <dst> --catalog <cat> --state <state>
  --pipeline-id <id>` with `cmd.Stdout = out`.
- The fake Engine in tests returns a canned catalog and writes canned JSONL.

### The ingest seam

The runner depends on a narrow `Ingest` function value, not the whole collector:
```go
type Ingest func(ctx context.Context, pipelineID int64, r io.Reader) (int, error)
```
Production wires `collector.NewIngester(collector.NewPgSink(pool)).Ingest`. Tests
pass a fake that drains the reader into an in-memory `collector.Sink`, or counts
events. This keeps the runner testable and the collector untouched.

### The Loader seam

```go
type Loader interface {
    // Load returns a pipeline's engine inputs, or ok=false when absent/archived.
    Load(ctx context.Context, id int64) (PipelineConfig, bool, error)
}
type PipelineConfig struct {
    SourceType        string
    SourceConfig      []byte // JSONB {type, settings}
    DestinationConfig []byte // JSONB {type, settings}
    Selections        []StreamSelection // from the stored catalog
}
type StreamSelection struct{ Namespace, Name, Mode, CursorField string }
```
A `pgLoader` reads `source_config`, `destination_config`, `catalog` from the
`pipelines` row. The stored catalog is `{"streams":[{name,mode,cursor_field}]}`
where `name` is `"namespace.name"`; the loader splits it into `StreamSelection`.

## Pure helpers (unit-tested)

- `flattenEndpoint(raw []byte) (map[string]any, error)` — turns the stored
  `{"type":"postgres","settings":{"host":"db"}}` into lsengine's flat
  `{"type":"postgres","host":"db"}`. `type` wins if a settings key collides.
- `buildCatalog(discovered []byte, sels []StreamSelection) ([]byte, error)` —
  parses the discovered catalog (a `model.Catalog`-shaped `{streams:[...]}`) and
  attaches `selected_streams` built from `sels` (splitting `namespace.name`),
  returning the merged catalog JSON `lsengine sync` consumes.

## HTTP surface

`POST /api/v1/pipelines/{id}/run` → launches `Runner.Run` in a background
goroutine (its own context, not the request's) and returns **202 Accepted** with
`{"status":"started","pipeline_id":id}`. A missing/archived pipeline returns 404
(checked synchronously before launching). Progress is observed through the
events/metrics/diff the Ingester writes, already surfaced by the read API.

## Wiring in `run()` (main.go)

`api.New` gains the runner so the handler can trigger it. Construct:
`runner.New(execEngine{cfg.EnginePath}, ingest, pgLoader, dataDir, now)` and pass
it into `api.New(pool, logger, runner)`. `LAKESENSE_DATA_DIR` (default
`filepath.Join(os.TempDir(), "lakesense")`) is added to `config.Config`.

## Error handling

- Discover failure → run aborts before sync; return the error (logged; no events).
- Sync non-zero exit → the pipe still delivered whatever JSONL was emitted
  (including any `sync_failed` event), so `Ingest` runs to completion first; the
  runner then returns the sync error. The pipeline is never left wedged; the next
  trigger starts fresh configs against the persisted `state.json`.
- The ingest goroutine and the sync command are coordinated so the pipe is always
  closed exactly once and both finish before `Run` returns (no leaked goroutine).

## Testing (Rule 6 — the run path is correctness-adjacent)

**Unit (no binary, no DB):**
1. `flattenEndpoint` merges settings up and keeps `type`.
2. `buildCatalog` attaches `selected_streams` with namespace/name split from the
   discovered catalog.
3. `Runner.Run` with a **fake Engine** (canned catalog + canned JSONL incl. a
   `checksum_computed` pair and `sync_finished`) + a fake ingest → asserts:
   discover was called with the written source path, the catalog handed to Sync
   carries the selections, the JSONL was ingested (event count > 0), and
   `RunResult` reports the sync id.
4. `Runner.Run` when Engine.Sync returns a non-zero exit → the emitted events are
   still ingested and the error is surfaced.
5. Loader returns not-found → `Run` returns a `NotFound`-style error, nothing execs.

**Integration (gated `LAKESENSE_ENGINE_IT=1`, builds real lsengine):**
6. Build `lsengine`, seed a sqlite db, run `Runner.Run` with the real
   `execEngine` and an in-memory `collector.Sink` → assert a `sync_finished`
   metric and a matching diff run were derived. Proves exec + pipe + ingest.

`make check` green with integration skipped when the gate is unset.

## Rollout / commit plan

One commit per plan task on branch `track-b2a-pipeline-runner`; PR to `main` when
`make check` + unit tests are green (integration runs under the gate). Update
`PROGRESS.md` (gitignored) locally: 4.1 collector "launches/monitors lsengine
runs" advances from seed-only to a real runner + trigger endpoint.

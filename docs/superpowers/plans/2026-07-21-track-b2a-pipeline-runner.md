# Track B2a — Pipeline Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a created pipeline actually run: materialize its engine config files, invoke `lsengine` (discover → sync), pipe the JSONL stream through the existing collector `Ingester`, and expose it as `POST /api/v1/pipelines/{id}/run`.

**Architecture:** A new `backend/internal/runner` package orchestrates a run behind an `Engine` seam (so the logic is testable without the binary), a narrow `Ingest` function value (the existing collector path), and a `Loader` (pipeline config from the store). Pure helpers flatten stored configs and merge stream selections into the discovered catalog.

**Tech Stack:** Go 1.26, `os/exec`, `io.Pipe`, existing `collector`/`config`/`store` packages, chi, `testify`, real `lsengine` binary for the gated integration test.

## Global Constraints

- Go module `github.com/lakesense/lakesense/backend`; `internal/` layout; consumer-side interfaces.
- The backend must NOT import the engine Go module — the runner talks to `lsengine` only as a subprocess and treats catalogs as JSON (generic maps), never engine types.
- Errors wrapped `%w`; every exec/IO takes `context.Context`; the ingest goroutine and sync command coordinate so the pipe closes exactly once and no goroutine leaks.
- Secrets/paths from env only. New `LAKESENSE_DATA_DIR` (default `filepath.Join(os.TempDir(), "lakesense")`).
- Table-driven `testify` tests, `-race`. Integration test gates on `LAKESENSE_ENGINE_IT=1`; skips otherwise. `make check` green with it skipped.
- Commit per task on `main` (user asked to stay on master). `PROGRESS.md` is gitignored — update locally, do not `git add` it.

## Reference facts (verified)

- `collector.NewIngester(sink).Ingest(ctx, pipelineID int64, r io.Reader) (int, error)`; `collector.NewPgSink(pool)`; `collector.Sink` interface (InsertEvent/RecordMetric/UpsertDiffRun/RecordLineage/MarkSynced).
- `config.Config` has `EnginePath` (env `LAKESENSE_ENGINE_PATH`, default `lsengine`).
- `pipelines` row columns: `source_type, source_config, destination_config, catalog, status` (JSONB configs). Stored `source_config` shape is `{"type":..,"settings":{..}}` (B1 `Endpoint`); stored `catalog` is `{"streams":[{"name":"ns.name","mode":..,"cursor_field":..}]}`.
- `lsengine discover --config <src>` prints a JSON catalog `{"streams":[...]}`. `lsengine sync --config <src> --destination <dst> --catalog <cat> --state <state> --pipeline-id <id>` emits JSONL on stdout and accepts `selected_streams` in the catalog.
- `api.New(pool, logger)` builds the router via `chiRouter(s)`; `Server` has `pool, logger, pipelines`.

---

## File Structure

- `backend/internal/runner/runner.go` — `Runner`, `Run`, seams (`Engine`, `Loader`, `Ingest`), result types.
- `backend/internal/runner/engine.go` — `execEngine` (the real `lsengine` subprocess).
- `backend/internal/runner/catalog.go` — pure helpers `flattenEndpoint`, `buildCatalog`, selection parsing.
- `backend/internal/runner/loader.go` — `pgLoader`.
- `backend/internal/runner/*_test.go` — unit + gated integration tests.
- `backend/internal/api/writes.go` — add the `/run` handler + route.
- Modify: `backend/internal/api/api.go` (`Server.runner`; `New` takes a runner), `backend/cmd/lakesense/main.go` (construct runner, pass to `api.New`), `backend/internal/config/config.go` (`DataDir`).

---

### Task 1: Pure helpers — flattenEndpoint and buildCatalog

**Files:**
- Create: `backend/internal/runner/catalog.go`
- Test: `backend/internal/runner/catalog_test.go`

**Interfaces:**
- Produces: `StreamSelection{Namespace, Name, Mode, CursorField string}`, `flattenEndpoint(raw []byte) (map[string]any, error)`, `parseSelections(catalogJSONB []byte) ([]StreamSelection, error)`, `buildCatalog(discovered []byte, sels []StreamSelection) ([]byte, error)`.

- [ ] **Step 1: Write the failing test**

Create `backend/internal/runner/catalog_test.go`:
```go
package runner

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFlattenEndpoint(t *testing.T) {
	out, err := flattenEndpoint([]byte(`{"type":"postgres","settings":{"host":"db","user":"ro"}}`))
	require.NoError(t, err)
	require.Equal(t, "postgres", out["type"])
	require.Equal(t, "db", out["host"])
	require.Equal(t, "ro", out["user"])
}

func TestParseSelectionsSplitsNamespace(t *testing.T) {
	sels, err := parseSelections([]byte(`{"streams":[{"name":"public.orders","mode":"full_load"},{"name":"main.items","mode":"incremental","cursor_field":"updated_at"}]}`))
	require.NoError(t, err)
	require.Len(t, sels, 2)
	require.Equal(t, "public", sels[0].Namespace)
	require.Equal(t, "orders", sels[0].Name)
	require.Equal(t, "updated_at", sels[1].CursorField)
}

func TestBuildCatalogAttachesSelectedStreams(t *testing.T) {
	discovered := []byte(`{"streams":[{"namespace":"public","name":"orders","schema":{"columns":[{"name":"id","type":"int64"}]}}]}`)
	sels := []StreamSelection{{Namespace: "public", Name: "orders", Mode: "full_load"}}
	out, err := buildCatalog(discovered, sels)
	require.NoError(t, err)

	var cat map[string]any
	require.NoError(t, json.Unmarshal(out, &cat))
	require.Contains(t, cat, "streams")
	selected, ok := cat["selected_streams"].([]any)
	require.True(t, ok)
	require.Len(t, selected, 1)
	first := selected[0].(map[string]any)
	require.Equal(t, "public", first["namespace"])
	require.Equal(t, "orders", first["name"])
	require.Equal(t, "full_load", first["mode"])
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./internal/runner/ -run 'TestFlatten|TestParse|TestBuild'`
Expected: FAIL — `undefined: flattenEndpoint`.

- [ ] **Step 3: Implement `catalog.go`**

Create `backend/internal/runner/catalog.go`:
```go
package runner

import (
	"encoding/json"
	"fmt"
	"strings"
)

// StreamSelection is one selected stream from a pipeline's stored catalog.
type StreamSelection struct {
	Namespace   string
	Name        string
	Mode        string
	CursorField string
}

// flattenEndpoint turns a stored endpoint ({"type":..,"settings":{..}}) into the
// flat config document lsengine expects ({"type":.., ...settings}). The type key
// always wins over a settings collision.
func flattenEndpoint(raw []byte) (map[string]any, error) {
	var ep struct {
		Type     string            `json:"type"`
		Settings map[string]string `json:"settings"`
	}
	if err := json.Unmarshal(raw, &ep); err != nil {
		return nil, fmt.Errorf("parse endpoint config: %w", err)
	}
	out := make(map[string]any, len(ep.Settings)+1)
	for k, v := range ep.Settings {
		out[k] = v
	}
	out["type"] = ep.Type
	return out, nil
}

// parseSelections reads a pipeline's stored catalog ({"streams":[{name,mode,
// cursor_field}]}) into StreamSelections, splitting "namespace.name".
func parseSelections(catalogJSONB []byte) ([]StreamSelection, error) {
	var doc struct {
		Streams []struct {
			Name        string `json:"name"`
			Mode        string `json:"mode"`
			CursorField string `json:"cursor_field"`
		} `json:"streams"`
	}
	if len(catalogJSONB) > 0 {
		if err := json.Unmarshal(catalogJSONB, &doc); err != nil {
			return nil, fmt.Errorf("parse stored catalog: %w", err)
		}
	}
	sels := make([]StreamSelection, 0, len(doc.Streams))
	for _, s := range doc.Streams {
		ns, name := splitStream(s.Name)
		mode := s.Mode
		if mode == "" {
			mode = "full_load"
		}
		sels = append(sels, StreamSelection{Namespace: ns, Name: name, Mode: mode, CursorField: s.CursorField})
	}
	return sels, nil
}

// splitStream splits "namespace.name" on the first dot; an undotted value has an
// empty namespace.
func splitStream(id string) (ns, name string) {
	if i := strings.IndexByte(id, '.'); i >= 0 {
		return id[:i], id[i+1:]
	}
	return "", id
}

// buildCatalog parses the discovered catalog and attaches selected_streams built
// from sels, returning the catalog document lsengine sync consumes. The
// discovered streams (with their schemas) are preserved verbatim.
func buildCatalog(discovered []byte, sels []StreamSelection) ([]byte, error) {
	var cat map[string]json.RawMessage
	if err := json.Unmarshal(discovered, &cat); err != nil {
		return nil, fmt.Errorf("parse discovered catalog: %w", err)
	}
	selected := make([]map[string]any, 0, len(sels))
	for _, s := range sels {
		m := map[string]any{"namespace": s.Namespace, "name": s.Name, "mode": s.Mode}
		if s.CursorField != "" {
			m["cursor_field"] = s.CursorField
		}
		selected = append(selected, m)
	}
	raw, err := json.Marshal(selected)
	if err != nil {
		return nil, fmt.Errorf("marshal selected streams: %w", err)
	}
	cat["selected_streams"] = raw
	out, err := json.Marshal(cat)
	if err != nil {
		return nil, fmt.Errorf("marshal catalog: %w", err)
	}
	return out, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd backend && go test -race ./internal/runner/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add backend/internal/runner/catalog.go backend/internal/runner/catalog_test.go
git commit -m "feat(backend): runner catalog helpers — flatten config + merge selections (B2a)"
```

---

### Task 2: Runner orchestration + execEngine

**Files:**
- Create: `backend/internal/runner/runner.go`
- Create: `backend/internal/runner/engine.go`
- Test: `backend/internal/runner/runner_test.go`

**Interfaces:**
- Produces: `Engine` interface, `SyncPaths`, `Ingest` func type, `Loader` interface, `PipelineConfig`, `Runner`, `New(engine Engine, ingest Ingest, loader Loader, dataDir string, now func() time.Time) *Runner`, `Runner.Run(ctx, pipelineID int64) (RunResult, error)`, `RunResult{Events int, SyncID string}`, `NotFoundError`.
- Consumes: `flattenEndpoint`, `parseSelections`, `buildCatalog` (Task 1).

- [ ] **Step 1: Write the failing test**

Create `backend/internal/runner/runner_test.go`:
```go
package runner

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeEngine returns a canned discovered catalog and writes canned JSONL.
type fakeEngine struct {
	discoverPath string
	syncCatalog  string
	syncErr      error
	jsonl        string
}

func (f *fakeEngine) Discover(_ context.Context, sourceConfigPath string) ([]byte, error) {
	f.discoverPath = sourceConfigPath
	return []byte(`{"streams":[{"namespace":"main","name":"items","schema":{"columns":[{"name":"id","type":"int64"}]}}]}`), nil
}
func (f *fakeEngine) Sync(_ context.Context, p SyncPaths, _ int64, out io.Writer) error {
	// Record the catalog the runner built, then emit the canned stream.
	b, _ := io.ReadAll(readFile(p.Catalog))
	f.syncCatalog = string(b)
	_, _ = io.WriteString(out, f.jsonl)
	return f.syncErr
}

// fakeLoader hands back a fixed pipeline config.
type fakeLoader struct {
	cfg PipelineConfig
	ok  bool
}

func (l fakeLoader) Load(context.Context, int64) (PipelineConfig, bool, error) {
	return l.cfg, l.ok, nil
}

func cannedJSONL() string {
	return `{"v":1,"event":"sync_started","sync_id":"s1","payload":{}}
{"v":1,"event":"checksum_computed","sync_id":"s1","stream":"main.items","payload":{"side":"source","rows":3,"checksum":"abc"}}
{"v":1,"event":"checksum_computed","sync_id":"s1","stream":"main.items","payload":{"side":"destination","rows":3,"checksum":"abc"}}
{"v":1,"event":"sync_finished","sync_id":"s1","payload":{"rows_read":3,"rows_written":3,"bytes_written":100,"duration_seconds":0.1}}
`
}

func TestRunOrchestratesDiscoverThenSyncAndIngests(t *testing.T) {
	eng := &fakeEngine{jsonl: cannedJSONL()}
	var ingested int
	ingest := func(_ context.Context, _ int64, r io.Reader) (int, error) {
		b, _ := io.ReadAll(r)
		// count non-empty lines
		for _, line := range splitLines(string(b)) {
			if line != "" {
				ingested++
			}
		}
		return ingested, nil
	}
	loader := fakeLoader{ok: true, cfg: PipelineConfig{
		SourceConfig:      []byte(`{"type":"sqlite","settings":{"path":"/tmp/x.db"}}`),
		DestinationConfig: []byte(`{"type":"parquet","settings":{"path":"/tmp/out"}}`),
		Selections:        []StreamSelection{{Namespace: "main", Name: "items", Mode: "full_load"}},
	}}
	r := New(eng, ingest, loader, t.TempDir(), func() time.Time { return time.Unix(0, 0) })

	res, err := r.Run(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, 4, res.Events)
	require.Contains(t, eng.syncCatalog, "selected_streams")
	require.Contains(t, eng.discoverPath, "source.json")
}

func TestRunIngestsEvenWhenSyncFails(t *testing.T) {
	eng := &fakeEngine{jsonl: cannedJSONL(), syncErr: fmt.Errorf("exit 1")}
	var ingested int
	ingest := func(_ context.Context, _ int64, r io.Reader) (int, error) {
		b, _ := io.ReadAll(r)
		for _, line := range splitLines(string(b)) {
			if line != "" {
				ingested++
			}
		}
		return ingested, nil
	}
	loader := fakeLoader{ok: true, cfg: PipelineConfig{
		SourceConfig:      []byte(`{"type":"sqlite","settings":{}}`),
		DestinationConfig: []byte(`{"type":"ndjson","settings":{"path":"/tmp/o"}}`),
		Selections:        []StreamSelection{{Namespace: "main", Name: "items", Mode: "full_load"}},
	}}
	r := New(eng, ingest, loader, t.TempDir(), func() time.Time { return time.Unix(0, 0) })

	_, err := r.Run(context.Background(), 7)
	require.Error(t, err, "sync failure is surfaced")
	require.Equal(t, 4, ingested, "events emitted before the failure are still ingested")
}

func TestRunNotFound(t *testing.T) {
	r := New(&fakeEngine{}, nil, fakeLoader{ok: false}, t.TempDir(), time.Now)
	_, err := r.Run(context.Background(), 99)
	var nf *NotFoundError
	require.ErrorAs(t, err, &nf)
}
```
Add small test helpers at the bottom of the file:
```go
func readFile(p string) io.Reader { b, _ := osReadFile(p); return bytesReader(b) }
```
(Use `os.ReadFile` and `bytes.NewReader` directly instead of the shims — inline them: `func readFile(p string) io.Reader { b, _ := os.ReadFile(p); return bytes.NewReader(b) }`, and `splitLines` via `strings.Split(s, "\n")`. Add `os`, `bytes`, `strings` imports.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./internal/runner/ -run TestRun`
Expected: FAIL — `undefined: New` / `Runner`.

- [ ] **Step 3: Implement `runner.go`**

Create `backend/internal/runner/runner.go`:
```go
// Package runner executes pipelines: it materializes a pipeline's engine config
// files, drives lsengine (discover then sync) behind an Engine seam, and pipes
// the JSONL event stream through the collector Ingester. This is the collector's
// real job (spec 4.1) — the platform now runs pipelines, not just seed data.
package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Engine abstracts the lsengine binary so orchestration is testable without a
// process. execEngine (engine.go) runs the real binary.
type Engine interface {
	Discover(ctx context.Context, sourceConfigPath string) ([]byte, error)
	Sync(ctx context.Context, p SyncPaths, pipelineID int64, out io.Writer) error
}

// SyncPaths are the file paths handed to lsengine sync.
type SyncPaths struct{ Source, Destination, Catalog, State string }

// Ingest consumes a JSONL event stream for a pipeline (the collector path).
type Ingest func(ctx context.Context, pipelineID int64, r io.Reader) (int, error)

// Loader loads a pipeline's engine inputs from the store.
type Loader interface {
	Load(ctx context.Context, id int64) (PipelineConfig, bool, error)
}

// PipelineConfig is a pipeline's engine inputs.
type PipelineConfig struct {
	SourceType        string
	SourceConfig      []byte
	DestinationConfig []byte
	Selections        []StreamSelection
}

// RunResult summarizes a completed run.
type RunResult struct {
	Events int
	SyncID string
}

// NotFoundError is returned when a pipeline is absent or archived.
type NotFoundError struct{ ID int64 }

func (e *NotFoundError) Error() string { return fmt.Sprintf("pipeline %d not found or not runnable", e.ID) }

// Runner executes one pipeline per Run call.
type Runner struct {
	engine  Engine
	ingest  Ingest
	loader  Loader
	dataDir string
	now     func() time.Time
}

// New builds a Runner.
func New(engine Engine, ingest Ingest, loader Loader, dataDir string, now func() time.Time) *Runner {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &Runner{engine: engine, ingest: ingest, loader: loader, dataDir: dataDir, now: now}
}

// Run executes the pipeline: load config, materialize files, discover, build the
// catalog, sync, and ingest the event stream. lsengine's stdout is piped to the
// ingester so events land as they are emitted; a sync failure still ingests
// everything written before it, then surfaces the error.
func (r *Runner) Run(ctx context.Context, pipelineID int64) (RunResult, error) {
	cfg, ok, err := r.loader.Load(ctx, pipelineID)
	if err != nil {
		return RunResult{}, fmt.Errorf("load pipeline %d: %w", pipelineID, err)
	}
	if !ok {
		return RunResult{}, &NotFoundError{ID: pipelineID}
	}

	dir := filepath.Join(r.dataDir, fmt.Sprint(pipelineID))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return RunResult{}, fmt.Errorf("create run dir: %w", err)
	}
	paths := SyncPaths{
		Source:      filepath.Join(dir, "source.json"),
		Destination: filepath.Join(dir, "dest.json"),
		Catalog:     filepath.Join(dir, "catalog.json"),
		State:       filepath.Join(dir, "state.json"),
	}
	if err := writeFlattened(paths.Source, cfg.SourceConfig); err != nil {
		return RunResult{}, err
	}
	if err := writeFlattened(paths.Destination, cfg.DestinationConfig); err != nil {
		return RunResult{}, err
	}

	discovered, err := r.engine.Discover(ctx, paths.Source)
	if err != nil {
		return RunResult{}, fmt.Errorf("discover: %w", err)
	}
	catalog, err := buildCatalog(discovered, cfg.Selections)
	if err != nil {
		return RunResult{}, err
	}
	if err := os.WriteFile(paths.Catalog, catalog, 0o644); err != nil {
		return RunResult{}, fmt.Errorf("write catalog: %w", err)
	}

	// Pipe sync stdout → ingester. The ingester runs in a goroutine draining the
	// pipe; Sync writes to the pipe writer. Both must finish before Run returns.
	pr, pw := io.Pipe()
	type ingestResult struct {
		n   int
		err error
	}
	done := make(chan ingestResult, 1)
	go func() {
		n, ierr := r.ingest(ctx, pipelineID, pr)
		_ = pr.CloseWithError(ierr) // unblock the writer if ingest stops early
		done <- ingestResult{n, ierr}
	}()

	syncErr := r.engine.Sync(ctx, paths, pipelineID, pw)
	_ = pw.Close() // signal EOF to the ingester
	ing := <-done

	if ing.err != nil {
		return RunResult{}, fmt.Errorf("ingest run of pipeline %d: %w", pipelineID, ing.err)
	}
	if syncErr != nil {
		return RunResult{Events: ing.n}, fmt.Errorf("sync pipeline %d: %w", pipelineID, syncErr)
	}
	return RunResult{Events: ing.n}, nil
}

// writeFlattened flattens a stored endpoint config and writes it as a JSON file.
func writeFlattened(path string, raw []byte) error {
	flat, err := flattenEndpoint(raw)
	if err != nil {
		return err
	}
	b, err := jsonMarshal(flat)
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
```
Add a tiny `jsonMarshal` wrapper in `catalog.go` (or inline `encoding/json`): use `json.Marshal` directly — replace `jsonMarshal(flat)` with `json.Marshal(flat)` and import `encoding/json` in `runner.go`.

- [ ] **Step 4: Implement `engine.go`**

Create `backend/internal/runner/engine.go`:
```go
package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
)

// execEngine runs the real lsengine binary at Path.
type execEngine struct{ Path string }

// NewExecEngine builds an Engine backed by the lsengine binary.
func NewExecEngine(path string) Engine { return execEngine{Path: path} }

// Discover runs `lsengine discover --config <src>` and returns its stdout.
func (e execEngine) Discover(ctx context.Context, sourceConfigPath string) ([]byte, error) {
	var out, errb bytes.Buffer
	cmd := exec.CommandContext(ctx, e.Path, "discover", "--config", sourceConfigPath)
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("lsengine discover: %w: %s", err, errb.String())
	}
	return out.Bytes(), nil
}

// Sync runs `lsengine sync ...`, streaming the JSONL event log to out.
func (e execEngine) Sync(ctx context.Context, p SyncPaths, pipelineID int64, out io.Writer) error {
	cmd := exec.CommandContext(ctx, e.Path, "sync",
		"--config", p.Source, "--destination", p.Destination,
		"--catalog", p.Catalog, "--state", p.State,
		"--pipeline-id", fmt.Sprint(pipelineID))
	cmd.Stdout = out
	// Engine diagnostics go to stderr; surface them in the error only.
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("lsengine sync exited: %w: %s", err, errb.String())
	}
	return nil
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd backend && go test -race ./internal/runner/`
Expected: PASS (orchestration, sync-fail-still-ingests, not-found).

- [ ] **Step 6: Commit**

```bash
git add backend/internal/runner/runner.go backend/internal/runner/engine.go backend/internal/runner/runner_test.go
git commit -m "feat(backend): pipeline runner — discover→sync→ingest behind an Engine seam (B2a)"
```

---

### Task 3: pgLoader, config DataDir, HTTP trigger, and wiring

**Files:**
- Create: `backend/internal/runner/loader.go`
- Modify: `backend/internal/config/config.go` (add `DataDir`)
- Modify: `backend/internal/api/api.go` (Server.runner; `New` signature)
- Modify: `backend/internal/api/writes.go` (add `/run` handler + route)
- Modify: `backend/cmd/lakesense/main.go` (construct runner, pass to `api.New`)
- Test: `backend/internal/api/writes_test.go` (add `/run` handler test)

**Interfaces:**
- Produces: `NewPgLoader(pool *pgxpool.Pool) Loader`; `api.New(pool, logger, runner *runner.Runner)`; route `POST /pipelines/{id}/run`.

- [ ] **Step 1: Write the failing test**

Add to `backend/internal/api/writes_test.go`:
```go
// fakeRunner records Run calls for the handler test.
type fakeRunner struct{ ran []int64; notFound bool }

func (f *fakeRunner) Run(_ context.Context, id int64) (runner.RunResult, error) {
	f.ran = append(f.ran, id)
	if f.notFound {
		return runner.RunResult{}, &runner.NotFoundError{ID: id}
	}
	return runner.RunResult{Events: 3}, nil
}

func TestRunEndpointAccepts202(t *testing.T) {
	fr := &fakeRunner{}
	svc := pipelines.NewService(newMemRepo(), nopRecorder{}, func() time.Time { return time.Unix(0, 0).UTC() })
	// seed a pipeline so the pre-check passes
	_, _ = svc.Create(context.Background(), "t", pipelines.CreatePipelineRequest{
		Name: "p", Source: pipelines.Endpoint{Type: "sqlite"}, Destination: pipelines.Endpoint{Type: "ndjson"},
		Streams: []pipelines.Stream{{Name: "main.items", Mode: "full_load"}},
	})
	s := &Server{logger: slog.Default(), pipelines: svc, runner: fr}
	h := chiRouter(s)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/1/run", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())

	require.Eventually(t, func() bool { return len(fr.ran) == 1 }, time.Second, 5*time.Millisecond)
}
```
Add imports for `runner` and `context` if missing. Note: the handler starts the run in a goroutine, so the test uses `require.Eventually`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd backend && go test ./internal/api/ -run TestRunEndpoint`
Expected: FAIL — `Server` has no `runner` field / `chiRouter` needs it.

- [ ] **Step 3: Define a runner seam in api and add the handler**

The api package should not depend on the concrete `*runner.Runner` for testability. Add a small interface in `writes.go`:
```go
// pipelineRunner triggers a pipeline run. Implemented by *runner.Runner; faked
// in tests.
type pipelineRunner interface {
	Run(ctx context.Context, id int64) (runner.RunResult, error)
}
```
Add the route in `registerWrites`:
```go
	r.Post("/pipelines/{id}/run", s.runPipeline)
```
Add the handler:
```go
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
```
Add `"context"` and the `runner` import to `writes.go`.

- [ ] **Step 4: Add the `runner` field and update `api.New`**

In `api.go`:
- Add to `Server`: `runner pipelineRunner`.
- Change `New` to accept and store it:
```go
func New(pool *pgxpool.Pool, logger *slog.Logger, run pipelineRunner) http.Handler {
	svc := pipelines.NewService(pipelines.NewPgRepo(pool), audit.NewPgRecorder(pool), nil)
	s := &Server{pool: pool, logger: logger, pipelines: svc, runner: run}
	return chiRouter(s)
}
```

- [ ] **Step 5: Implement `loader.go`**

Create `backend/internal/runner/loader.go`:
```go
package runner

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgLoader loads a runnable pipeline's engine inputs from the store.
type pgLoader struct{ pool *pgxpool.Pool }

// NewPgLoader builds a Postgres-backed Loader.
func NewPgLoader(pool *pgxpool.Pool) Loader { return &pgLoader{pool: pool} }

// Load returns a pipeline's config, or ok=false when it is absent or archived.
func (l *pgLoader) Load(ctx context.Context, id int64) (PipelineConfig, bool, error) {
	var (
		cfg    PipelineConfig
		status string
		cat    []byte
	)
	err := l.pool.QueryRow(ctx,
		`SELECT source_type, source_config, destination_config, catalog, status
		 FROM pipelines WHERE id = $1`, id).Scan(
		&cfg.SourceType, &cfg.SourceConfig, &cfg.DestinationConfig, &cat, &status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PipelineConfig{}, false, nil
		}
		return PipelineConfig{}, false, fmt.Errorf("load pipeline %d: %w", id, err)
	}
	if status == "archived" {
		return PipelineConfig{}, false, nil
	}
	sels, err := parseSelections(cat)
	if err != nil {
		return PipelineConfig{}, false, err
	}
	cfg.Selections = sels
	return cfg, true, nil
}
```

- [ ] **Step 6: Add `DataDir` to config and wire `main.go`**

In `config.go`, add the field + default:
```go
	// DataDir is the root for per-pipeline run scratch (configs, state, output).
	DataDir string
```
and in `Load()`:
```go
		DataDir: env("LAKESENSE_DATA_DIR", filepath.Join(os.TempDir(), "lakesense")),
```
Add `"path/filepath"` import.

In `main.go` `run()`, before building the server, construct the runner and pass it to `api.New`:
```go
	ingest := collector.NewIngester(collector.NewPgSink(st.Pool)).Ingest
	run := runner.New(runner.NewExecEngine(cfg.EnginePath), ingest, runner.NewPgLoader(st.Pool), cfg.DataDir, nil)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st.Pool, logger, run),
		ReadHeaderTimeout: 10 * time.Second,
	}
```
Add imports `collector` and `runner` to `main.go`.

- [ ] **Step 7: Run tests to verify they pass**

Run: `cd backend && go test -race ./internal/api/ ./internal/runner/ && go build ./...`
Expected: PASS + build OK.

- [ ] **Step 8: Commit**

```bash
git add backend/internal/runner/loader.go backend/internal/config/config.go backend/internal/api/api.go backend/internal/api/writes.go backend/internal/api/writes_test.go backend/cmd/lakesense/main.go
git commit -m "feat(backend): pgLoader + POST /pipelines/{id}/run trigger, wired into serve (B2a)"
```

---

### Task 4: Real-engine integration test, full gate, and docs

**Files:**
- Create: `backend/internal/runner/integration_test.go`
- Modify: `deploy/.env.example` (document `LAKESENSE_DATA_DIR`)
- Modify: `README.md` (pipelines can now be triggered to run)

**Interfaces:**
- Consumes: `NewExecEngine`, `Runner`, a fake `collector.Sink`.

- [ ] **Step 1: Write the gated integration test**

Create `backend/internal/runner/integration_test.go`:
```go
package runner_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/collector"
	"github.com/lakesense/lakesense/backend/internal/runner"
)

// countingSink is an in-memory collector.Sink that records what was derived.
type countingSink struct {
	metrics int
	diffs   int
	matched bool
}

func (s *countingSink) InsertEvent(context.Context, int64, collector.Event) error { return nil }
func (s *countingSink) RecordMetric(context.Context, int64, string, collector.SyncFinished, time.Time) error {
	s.metrics++
	return nil
}
func (s *countingSink) UpsertDiffRun(_ context.Context, _ int64, _ , _ string, d collector.DiffRun) error {
	s.diffs++
	s.matched = d.Match
	return nil
}
func (s *countingSink) RecordLineage(context.Context, int64, string, collector.ColumnMapping, string) error {
	return nil
}
func (s *countingSink) MarkSynced(context.Context, int64, collector.Event) error { return nil }

type fixedLoader struct{ cfg runner.PipelineConfig }

func (l fixedLoader) Load(context.Context, int64) (runner.PipelineConfig, bool, error) {
	return l.cfg, true, nil
}

func TestRunnerAgainstRealEngineSqlite(t *testing.T) {
	if os.Getenv("LAKESENSE_ENGINE_IT") == "" {
		t.Skip("set LAKESENSE_ENGINE_IT=1 to run the real-engine integration test")
	}
	work := t.TempDir()

	// Build lsengine from the engine module into the work dir.
	bin := filepath.Join(work, "lsengine")
	build := exec.Command("go", "build", "-o", bin, "./cmd/lsengine")
	build.Dir = engineDir(t)
	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))

	// Seed a sqlite source with a known table.
	db := filepath.Join(work, "src.db")
	sq, err := sql.Open("sqlite", "file:"+db)
	require.NoError(t, err)
	_, err = sq.Exec(`CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = sq.Exec(`INSERT INTO items(id,v) VALUES (1,'a'),(2,'b'),(3,'c')`)
	require.NoError(t, err)
	_ = sq.Close()

	sink := &countingSink{}
	ingest := collector.NewIngester(sink).Ingest
	loader := fixedLoader{cfg: runner.PipelineConfig{
		SourceType:        "sqlite",
		SourceConfig:      []byte(`{"type":"sqlite","settings":{"path":"` + db + `"}}`),
		DestinationConfig: []byte(`{"type":"parquet","settings":{"path":"` + filepath.Join(work, "out") + `"}}`),
		Selections:        []runner.StreamSelection{{Namespace: "main", Name: "items", Mode: "full_load"}},
	}}
	r := runner.New(runner.NewExecEngine(bin), ingest, loader, work, nil)

	res, err := r.Run(context.Background(), 42)
	require.NoError(t, err)
	require.Greater(t, res.Events, 0)
	require.Equal(t, 1, sink.metrics, "a sync_finished metric was derived")
	require.Equal(t, 1, sink.diffs)
	require.True(t, sink.matched, "source and destination checksums matched")
}

// engineDir returns the sibling engine module directory.
func engineDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd() // .../backend/internal/runner
	require.NoError(t, err)
	return filepath.Join(wd, "..", "..", "..", "engine")
}
```
Note: this test is `package runner_test` (external) and needs `collector.SyncFinished`, `collector.DiffRun`, `collector.ColumnMapping`, `collector.Event` to be exported (they are). If any field/name differs, adjust the sink signatures to match `collector.Sink` exactly.

- [ ] **Step 2: Run it under the gate**

Run:
```bash
cd backend && LAKESENSE_ENGINE_IT=1 go test -race ./internal/runner/ -run TestRunnerAgainstRealEngineSqlite -v
```
Expected: PASS — the real engine syncs sqlite→parquet and the ingester derives a matched diff. (Without the env: SKIP.)

- [ ] **Step 3: Full gate + docs**

Run: `cd /home/imran/Documents/github/LakeSense && make check` → green.

In `deploy/.env.example`, add a documented `LAKESENSE_DATA_DIR` line. In `README.md`, update the "Create & run a pipeline" note: a created pipeline can now be triggered with `POST /api/v1/pipelines/{id}/run` (auto-scheduling is the next step). In `PROGRESS.md` (local) advance 4.1: the collector now launches/monitors real `lsengine` runs via the runner + trigger endpoint (seed remains for demo). Do not `git add PROGRESS.md`.

- [ ] **Step 4: Commit**

```bash
git add backend/internal/runner/integration_test.go deploy/.env.example README.md
git commit -m "test(backend): real-engine runner integration (sqlite→parquet→ingest); docs (B2a)"
```

---

## Self-Review

**1. Spec coverage.**
- Materialize configs (flatten) + discover→buildCatalog→sync → Tasks 1,2. ✓
- Pipe stdout → Ingester, coordinated close, no goroutine leak → Task 2 (`io.Pipe` + done channel). ✓
- Sync failure still ingests, then surfaces error → Task 2 `TestRunIngestsEvenWhenSyncFails`. ✓
- Loader from store, archived → not runnable → Task 3 `pgLoader`. ✓
- `POST /pipelines/{id}/run` async 202 → Task 3. ✓
- `LAKESENSE_DATA_DIR`, wiring in serve → Task 3. ✓
- Real-engine integration (gated) → Task 4. ✓
- No engine-module import (catalogs as JSON maps) → Task 1 `buildCatalog`. ✓

**2. Placeholder scan.** No TODO/TBD. Two test-only shims are clarified inline (Task 2 `readFile`/`splitLines` → use `os.ReadFile`/`bytes.NewReader`/`strings.Split`; Task 2 `jsonMarshal` → `json.Marshal`).

**3. Type consistency.** `Engine.Sync(ctx, SyncPaths, int64, io.Writer)`, `Ingest func(ctx, int64, io.Reader)(int,error)`, `Loader.Load(ctx,int64)(PipelineConfig,bool,error)`, `Runner.Run(ctx,int64)(RunResult,error)`, `New(Engine,Ingest,Loader,string,func()time.Time)` are used identically across `runner.go`, the fakes, `engine.go`, `loader.go`, the api seam (`pipelineRunner.Run`), and `main.go`. `api.New` gains a third `pipelineRunner` param, updated in `main.go` and both api tests (the existing `writes_test.go` builds `Server` directly via `chiRouter`, so it is unaffected except the new `/run` test which sets `runner: fr`).

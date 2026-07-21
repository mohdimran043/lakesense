// Package runner executes pipelines: it materializes a pipeline's engine config
// files, drives lsengine (discover then sync) behind an Engine seam, and pipes
// the JSONL event stream through the collector Ingester. This is the collector's
// real job (spec 4.1) — the platform now runs pipelines, not just seed data.
package runner

import (
	"context"
	"encoding/json"
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

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("pipeline %d not found or not runnable", e.ID)
}

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
	b, err := json.Marshal(flat)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

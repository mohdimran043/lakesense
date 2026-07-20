// Package cli implements lsengine's command surface:
// spec | check | discover | sync | backfill | verify | version.
//
// File-based I/O contract (docs/analysis/engine-protocol.md §1): configs and
// catalogs come in as JSON file paths; progress and results are emitted as
// JSONL events on stdout; state is rewritten atomically as sync progresses.
// Command output shapes differ by consumer: spec and discover print a single
// JSON document (schema / catalog), check prints a human status line, and the
// data-path commands (sync/backfill/verify) emit the JSONL event stream.
package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/lakesense/lakesense/engine/internal/buildinfo"
	"github.com/lakesense/lakesense/engine/internal/config"
	"github.com/lakesense/lakesense/engine/internal/connectors"
	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/lakesense/lakesense/engine/internal/syncrun"
	"github.com/lakesense/lakesense/engine/internal/verify"
)

// commonFlags holds the flags shared by data-path commands.
type commonFlags struct {
	connector   string
	config      string
	destination string
	catalog     string
	state       string
	pipelineID  string
}

func (c *commonFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&c.connector, "connector", "", "connector type (defaults to the config's \"type\" field)")
	fs.StringVar(&c.config, "config", "", "path to source config JSON (required)")
	fs.StringVar(&c.destination, "destination", "", "path to destination config JSON")
	fs.StringVar(&c.catalog, "catalog", "", "path to stream catalog JSON")
	fs.StringVar(&c.state, "state", "", "path to state JSON (created if absent)")
	fs.StringVar(&c.pipelineID, "pipeline-id", "", "control-plane pipeline ID stamped on events")
}

// Run dispatches lsengine commands. args excludes the program name.
func Run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cmd, rest := args[0], args[1:]
	var err error
	switch cmd {
	case "spec":
		err = runSpec(ctx, rest, stdout)
	case "check":
		err = runCheck(ctx, rest, stdout)
	case "discover":
		err = runDiscover(ctx, rest, stdout)
	case "sync":
		err = runSync(ctx, rest, stdout)
	case "backfill":
		err = runStub(ctx, "backfill", rest, stdout)
	case "verify":
		err = runVerify(ctx, rest, stdout)
	case "version":
		fmt.Fprintln(stdout, buildinfo.Version)
	case "help", "-h", "--help":
		usage(stdout)
	default:
		fmt.Fprintf(stderr, "lsengine: unknown command %q\n\n", cmd)
		usage(stderr)
		return 2
	}
	if err != nil {
		fmt.Fprintf(stderr, "lsengine %s: %v\n", cmd, err)
		return 1
	}
	return 0
}

func usage(w io.Writer) {
	fmt.Fprintf(w, `lsengine — LakeSense replication engine

Usage:
  lsengine <command> [flags]

Commands:
  spec      print the JSON schema of a connector's config
  check     validate connectivity and configuration
  discover  list streams and their schemas as a catalog
  sync      run replication for the selected streams
  backfill  re-sync a bounded slice of a stream (PK range or time window)
  verify    re-check counts and checksums source vs destination
  version   print the engine version
`)
}

// runSpec emits a connector's Spec (identity, capabilities, config schema) as a
// JSON document — the surface a UI renders a source form from. Spec must work
// without connecting, so no Setup is performed.
func runSpec(_ context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("spec", flag.ContinueOnError)
	connector := fs.String("connector", "", "connector type (required)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *connector == "" {
		return fmt.Errorf("--connector is required")
	}
	c, err := connectors.Default().New(*connector)
	if err != nil {
		return err
	}
	return writeJSON(stdout, c.Spec())
}

// runCheck validates connectivity and source-side prerequisites, printing a
// human status line on success and an actionable error otherwise.
func runCheck(ctx context.Context, args []string, stdout io.Writer) error {
	cf, err := parseCommon("check", args)
	if err != nil {
		return err
	}
	c, typ, err := openConnector(ctx, cf)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	if err := c.Check(ctx); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "check ok: %s connection and prerequisites verified\n", typ)
	return nil
}

// runDiscover lists streams with schemas and supported modes, emitting the full
// catalog as a JSON document (discover layer 1). User selections are added by
// the caller before sync.
func runDiscover(ctx context.Context, args []string, stdout io.Writer) error {
	cf, err := parseCommon("discover", args)
	if err != nil {
		return err
	}
	c, _, err := openConnector(ctx, cf)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	streams, err := c.Discover(ctx)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	return writeJSON(stdout, model.Catalog{Streams: streams})
}

// runSync drives the orchestrator: engine_info first (so a run is always
// identifiable even if setup fails), then connector setup, catalog/destination
// resolution, and the full-load → incremental → CDC flow, streaming JSONL
// events on stdout throughout.
func runSync(ctx context.Context, args []string, stdout io.Writer) error {
	cf, err := parseCommon("sync", args)
	if err != nil {
		return err
	}

	em := events.NewEmitter(stdout, events.NewSyncID(), cf.pipelineID)
	if err := em.Emit(events.KindEngineInfo, "", events.EngineInfo{
		Version: buildinfo.Version, Command: "sync",
	}); err != nil {
		return err
	}

	c, typ, err := openConnector(ctx, cf)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()

	if cf.catalog == "" {
		return fmt.Errorf("--catalog is required for sync")
	}
	var cat model.Catalog
	if err := config.LoadJSON(cf.catalog, &cat); err != nil {
		return err
	}

	if cf.destination == "" {
		return fmt.Errorf("--destination is required for sync")
	}
	destCfg, err := loadDestination(cf.destination)
	if err != nil {
		return err
	}
	w, err := syncrun.OpenWriter(destCfg)
	if err != nil {
		return err
	}
	defer func() { _ = w.Close(ctx) }()

	st, err := loadState(cf.state)
	if err != nil {
		return err
	}

	return syncrun.Run(ctx, syncrun.Options{
		Connector:       c,
		Writer:          w,
		Catalog:         cat,
		State:           st,
		Emitter:         em,
		ConnectorType:   typ,
		DestinationType: destCfg.Type,
	})
}

// runVerify re-checks source vs destination current state for every selected
// stream, emitting a verify_result per stream. Exit code is non-zero on any
// mismatch so make verify / CI can gate on it.
func runVerify(ctx context.Context, args []string, stdout io.Writer) error {
	cf, err := parseCommon("verify", args)
	if err != nil {
		return err
	}
	em := events.NewEmitter(stdout, events.NewSyncID(), cf.pipelineID)
	if err := em.Emit(events.KindEngineInfo, "", events.EngineInfo{Version: buildinfo.Version, Command: "verify"}); err != nil {
		return err
	}
	c, _, err := openConnector(ctx, cf)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close(ctx) }()
	fl, ok := c.(sdk.FullLoader)
	if !ok {
		return fmt.Errorf("connector does not support full load; verify needs it to re-read the source")
	}
	if cf.catalog == "" || cf.destination == "" {
		return fmt.Errorf("verify requires --catalog and --destination")
	}
	var cat model.Catalog
	if err := config.LoadJSON(cf.catalog, &cat); err != nil {
		return err
	}
	destCfg, err := loadDestination(cf.destination)
	if err != nil {
		return err
	}
	reader, err := syncrun.OpenReader(destCfg)
	if err != nil {
		return err
	}
	defer func() { _ = reader.Close(ctx) }()

	allMatch := true
	for _, sel := range cat.Selected {
		stream, ok := cat.Stream(sel.ID())
		if !ok {
			return fmt.Errorf("selected stream %s not in catalog", sel.ID())
		}
		sr, err := reader.OpenRead(ctx, stream, sel.DestinationTable)
		if err != nil {
			return err
		}
		res, err := verify.VerifyStream(ctx, verify.StreamInput{
			Stream: stream, Source: fl, DestReader: sr,
		})
		_ = sr.Close(ctx)
		if err != nil {
			return err
		}
		if err := em.Emit(events.KindVerifyResult, stream.ID(), res); err != nil {
			return err
		}
		if !res.Match {
			allMatch = false
		}
	}
	if !allMatch {
		return fmt.Errorf("verify: one or more streams did not match source")
	}
	return nil
}

// loadDestination reads and decodes a destination config document from disk.
func loadDestination(path string) (syncrun.DestinationConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return syncrun.DestinationConfig{}, fmt.Errorf("read destination config: %w", err)
	}
	return syncrun.LoadDestinationConfig(raw)
}

// parseCommon parses the shared data-path flags and enforces the config path.
func parseCommon(name string, args []string) (commonFlags, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	var cf commonFlags
	cf.register(fs)
	if err := fs.Parse(args); err != nil {
		return cf, fmt.Errorf("parse flags: %w", err)
	}
	if cf.config == "" {
		return cf, fmt.Errorf("--config is required")
	}
	if _, err := os.Stat(cf.config); err != nil {
		return cf, fmt.Errorf("config file: %w", err)
	}
	return cf, nil
}

// openConnector resolves the connector type (explicit flag or the config's
// "type" field), instantiates it from the default registry, and connects.
func openConnector(ctx context.Context, cf commonFlags) (sdk.Connector, string, error) {
	raw, err := os.ReadFile(cf.config)
	if err != nil {
		return nil, "", fmt.Errorf("read config: %w", err)
	}
	typ := cf.connector
	if typ == "" {
		var probe struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(raw, &probe) // best-effort; validated below
		typ = probe.Type
	}
	if typ == "" {
		return nil, "", fmt.Errorf("connector type required: pass --connector or set a \"type\" field in the source config")
	}
	c, err := connectors.Default().New(typ)
	if err != nil {
		return nil, "", err
	}
	if err := c.Setup(ctx, raw); err != nil {
		_ = c.Close(ctx)
		return nil, "", fmt.Errorf("setup %s: %w", typ, err)
	}
	return c, typ, nil
}

// loadState opens the state file for resumable progress, or an in-memory
// document when no path is given (a fresh, non-resumable run).
func loadState(path string) (*state.Document, error) {
	if path == "" {
		return state.NewInMemory(), nil
	}
	return state.Load(path)
}

// writeJSON encodes v as an indented JSON document.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("encode output: %w", err)
	}
	return nil
}

// runStub is the placeholder for data-path commands whose full implementation
// lands in a later Phase 2 milestone (backfill → 2.8, verify → 2.7). It
// validates flags, emits engine_info, and reports not-implemented.
func runStub(_ context.Context, name string, args []string, stdout io.Writer) error {
	cf, err := parseCommon(name, args)
	if err != nil {
		return err
	}
	em := events.NewEmitter(stdout, events.NewSyncID(), cf.pipelineID)
	if err := em.Emit(events.KindEngineInfo, "", events.EngineInfo{
		Version: buildinfo.Version,
		Command: name,
	}); err != nil {
		return err
	}
	return fmt.Errorf("%s is not implemented yet (Phase 2 milestone pending)", name)
}

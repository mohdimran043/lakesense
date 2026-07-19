// Package cli implements lsengine's command surface:
// spec | check | discover | sync | backfill | verify | version.
//
// File-based I/O contract (docs/analysis/engine-protocol.md §1): configs and
// catalogs come in as JSON file paths; progress and results are emitted as
// JSONL events on stdout; state is rewritten atomically as sync progresses.
package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/lakesense/lakesense/engine/internal/buildinfo"
	"github.com/lakesense/lakesense/engine/internal/events"
)

// commonFlags holds the flags shared by data-path commands.
type commonFlags struct {
	config      string
	destination string
	catalog     string
	state       string
	pipelineID  string
}

func (c *commonFlags) register(fs *flag.FlagSet) {
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
		err = runStub(ctx, "check", rest, stdout)
	case "discover":
		err = runStub(ctx, "discover", rest, stdout)
	case "sync":
		err = runStub(ctx, "sync", rest, stdout)
	case "backfill":
		err = runStub(ctx, "backfill", rest, stdout)
	case "verify":
		err = runStub(ctx, "verify", rest, stdout)
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

// runSpec emits connector config schemas. Connector registry lands in Phase
// 2.2; until then it reports the engine's own capabilities envelope.
func runSpec(_ context.Context, args []string, _ io.Writer) error {
	fs := flag.NewFlagSet("spec", flag.ContinueOnError)
	connector := fs.String("connector", "", "connector type (required)")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if *connector == "" {
		return fmt.Errorf("--connector is required")
	}
	return fmt.Errorf("connector %q is not registered yet (connector SDK arrives in the next milestone)", *connector)
}

// runStub is the Phase 2.1 placeholder for data-path commands: it validates
// flags, emits the engine_info event, and reports not-implemented. Each
// command is replaced by its real implementation in later Phase 2 milestones.
func runStub(_ context.Context, name string, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	var cf commonFlags
	cf.register(fs)
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	if cf.config == "" {
		return fmt.Errorf("--config is required")
	}
	if _, err := os.Stat(cf.config); err != nil {
		return fmt.Errorf("config file: %w", err)
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

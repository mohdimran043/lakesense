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

// Backfill runs `lsengine backfill ...` for a bounded slice, streaming the JSONL
// event log (which ends with a verify_result) to out.
func (e execEngine) Backfill(ctx context.Context, p SyncPaths, pipelineID int64, o BackfillOpts, out io.Writer) error {
	args := []string{"backfill",
		"--config", p.Source, "--destination", p.Destination,
		"--catalog", p.Catalog, "--state", p.State,
		"--stream", o.Stream, "--pipeline-id", fmt.Sprint(pipelineID)}
	if o.SinceField != "" {
		args = append(args, "--since", o.SinceField+"="+o.SinceValue)
	} else {
		if o.PKMin != "" {
			args = append(args, "--pk-min", o.PKMin)
		}
		if o.PKMax != "" {
			args = append(args, "--pk-max", o.PKMax)
		}
	}
	cmd := exec.CommandContext(ctx, e.Path, args...)
	cmd.Stdout = out
	var errb bytes.Buffer
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("lsengine backfill exited: %w: %s", err, errb.String())
	}
	return nil
}

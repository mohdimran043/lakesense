package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestRunDispatch(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`{}`), 0o600))

	tests := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string
		wantErr  string
	}{
		{name: "no args", args: nil, wantCode: 2, wantErr: "Usage"},
		{name: "unknown command", args: []string{"frobnicate"}, wantCode: 2, wantErr: "unknown command"},
		{name: "version", args: []string{"version"}, wantCode: 0, wantOut: "dev"},
		{name: "help", args: []string{"help"}, wantCode: 0, wantOut: "Commands"},
		{name: "spec requires connector", args: []string{"spec"}, wantCode: 1, wantErr: "--connector is required"},
		{name: "spec unknown connector", args: []string{"spec", "--connector", "nope"}, wantCode: 1, wantErr: "unknown connector"},
		{name: "sync requires config", args: []string{"sync"}, wantCode: 1, wantErr: "--config is required"},
		{name: "sync missing config file", args: []string{"sync", "--config", "/nonexistent.json"}, wantCode: 1, wantErr: "config file"},
		{name: "sync needs connector type", args: []string{"sync", "--config", cfgPath}, wantCode: 1, wantErr: "connector type required"},
		{name: "discover needs connector type", args: []string{"discover", "--config", cfgPath}, wantCode: 1, wantErr: "connector type required"},
		{name: "backfill needs stream", args: []string{"backfill", "--config", cfgPath}, wantCode: 1, wantErr: "backfill requires"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Run(tt.args, &stdout, &stderr)
			assert.Equal(t, tt.wantCode, code)
			if tt.wantOut != "" {
				assert.Contains(t, stdout.String(), tt.wantOut)
			}
			if tt.wantErr != "" {
				assert.Contains(t, stderr.String(), tt.wantErr)
			}
		})
	}
}

// TestSpecEmitsConnectorSchema proves spec prints a real connector's Spec as a
// JSON document (not events) and that it works without connecting.
func TestSpecEmitsConnectorSchema(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Run([]string{"spec", "--connector", "postgres"}, &stdout, &stderr)
	require.Equal(t, 0, code, "stderr: %s", stderr.String())

	var spec sdk.Spec
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &spec))
	assert.Equal(t, "postgres", spec.Type)
	assert.Contains(t, spec.Capabilities, sdk.CapFullLoad)
	assert.NotEmpty(t, spec.ConfigSchema)
}

// TestSyncEmitsEngineInfoEvent proves sync emits engine_info as its first line
// even when the run later fails (here: a bare config with no connector type),
// so a run is always identifiable in the event stream.
func TestSyncEmitsEngineInfoEvent(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`{}`), 0o600))

	var stdout, stderr bytes.Buffer
	code := Run([]string{"sync", "--config", cfgPath, "--pipeline-id", "p1"}, &stdout, &stderr)
	require.Equal(t, 1, code) // no connector type resolvable from {}

	first := strings.SplitN(stdout.String(), "\n", 2)[0]
	var e events.Event
	require.NoError(t, json.Unmarshal([]byte(first), &e))
	assert.Equal(t, events.KindEngineInfo, e.Kind)
	assert.Equal(t, "p1", e.Pipeline)
	assert.NotEmpty(t, e.SyncID)
}

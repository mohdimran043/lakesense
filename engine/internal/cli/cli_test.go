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
		{name: "sync requires config", args: []string{"sync"}, wantCode: 1, wantErr: "--config is required"},
		{name: "sync missing config file", args: []string{"sync", "--config", "/nonexistent.json"}, wantCode: 1, wantErr: "config file"},
		{name: "sync stub reports pending", args: []string{"sync", "--config", cfgPath}, wantCode: 1, wantErr: "not implemented"},
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

func TestSyncStubEmitsEngineInfoEvent(t *testing.T) {
	cfgPath := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(cfgPath, []byte(`{}`), 0o600))

	var stdout, stderr bytes.Buffer
	Run([]string{"sync", "--config", cfgPath, "--pipeline-id", "p1"}, &stdout, &stderr)

	first := strings.SplitN(stdout.String(), "\n", 2)[0]
	var e events.Event
	require.NoError(t, json.Unmarshal([]byte(first), &e))
	assert.Equal(t, events.KindEngineInfo, e.Kind)
	assert.Equal(t, "p1", e.Pipeline)
	assert.NotEmpty(t, e.SyncID)
}

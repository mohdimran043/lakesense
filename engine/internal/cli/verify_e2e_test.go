package cli

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
)

// seedSQLite creates a throwaway .db with a known items table and returns its path.
func seedSQLite(t *testing.T, rows map[int64]string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "src.db")
	db, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, err = db.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, v TEXT NOT NULL)`)
	require.NoError(t, err)
	for id, v := range rows {
		_, err = db.Exec(`INSERT INTO items (id, v) VALUES (?, ?)`, id, v)
		require.NoError(t, err)
	}
	return path
}

// writeJSONFile marshals v to a temp file and returns its path.
func writeJSONFile(t *testing.T, dir, name string, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, b, 0o600))
	return path
}

// itemsCatalog builds a full-load catalog for the seeded items table.
func itemsCatalog() model.Catalog {
	stream := model.Stream{
		Namespace: "main", Name: "items",
		Schema: model.Schema{Columns: []model.Column{
			{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
			{Name: "v", Type: model.TypeString},
		}},
		SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental},
	}
	return model.Catalog{
		Streams:  []model.Stream{stream},
		Selected: []model.SelectedStream{{Namespace: "main", Name: "items", Mode: model.ModeFullLoad}},
	}
}

// TestVerifyMatchesAfterParquetSync proves the whole engine correctness loop
// through the CLI: sync a sqlite table to Parquet, then verify reports match.
func TestVerifyMatchesAfterParquetSync(t *testing.T) {
	work := t.TempDir()
	dbPath := seedSQLite(t, map[int64]string{1: "a", 2: "b", 3: "c"})
	outDir := filepath.Join(work, "out")

	srcPath := writeJSONFile(t, work, "src.json", map[string]any{"type": "sqlite", "path": dbPath})
	destPath := writeJSONFile(t, work, "dest.json", map[string]any{"type": "parquet", "path": outDir})
	catPath := writeJSONFile(t, work, "catalog.json", itemsCatalog())
	statePath := filepath.Join(work, "state.json")

	// sync
	var so, se bytes.Buffer
	code := Run([]string{"sync", "--config", srcPath, "--destination", destPath, "--catalog", catPath, "--state", statePath}, &so, &se)
	require.Equal(t, 0, code, "sync stderr: %s", se.String())

	// verify → match, exit 0
	var vo, ve bytes.Buffer
	code = Run([]string{"verify", "--config", srcPath, "--destination", destPath, "--catalog", catPath}, &vo, &ve)
	require.Equal(t, 0, code, "verify stderr: %s", ve.String())

	// The stream emitted a verify_result with match:true.
	var found bool
	for _, line := range strings.Split(strings.TrimSpace(vo.String()), "\n") {
		var e events.Event
		if json.Unmarshal([]byte(line), &e) != nil || e.Kind != events.KindVerifyResult {
			continue
		}
		payload, _ := json.Marshal(e.Payload)
		var vr events.VerifyResult
		require.NoError(t, json.Unmarshal(payload, &vr))
		require.True(t, vr.Match)
		require.Equal(t, int64(3), vr.SourceRows)
		found = true
	}
	require.True(t, found, "expected a verify_result event")
}

// verifyCode runs verify and returns its exit code.
func verifyCode(t *testing.T, src, dest, cat string) int {
	t.Helper()
	var o, e bytes.Buffer
	return Run([]string{"verify", "--config", src, "--destination", dest, "--catalog", cat}, &o, &e)
}

// TestBackfillRestoresCorruptedWindow proves the full repair loop: sync, corrupt
// the destination (drop a part-file), verify fails, backfill the range, verify
// passes — and a state file handed to backfill is untouched (state-safety).
func TestBackfillRestoresCorruptedWindow(t *testing.T) {
	work := t.TempDir()
	dbPath := seedSQLite(t, map[int64]string{1: "a", 2: "b", 3: "c"})
	outDir := filepath.Join(work, "out")

	srcPath := writeJSONFile(t, work, "src.json", map[string]any{"type": "sqlite", "path": dbPath})
	destPath := writeJSONFile(t, work, "dest.json", map[string]any{"type": "parquet", "path": outDir})
	catPath := writeJSONFile(t, work, "catalog.json", itemsCatalog())
	statePath := filepath.Join(work, "state.json")

	// Sync, then confirm it verifies clean.
	var so, se bytes.Buffer
	require.Equal(t, 0, Run([]string{"sync", "--config", srcPath, "--destination", destPath, "--catalog", catPath, "--state", statePath}, &so, &se), se.String())
	require.Equal(t, 0, verifyCode(t, srcPath, destPath, catPath))

	// Corrupt the destination: delete its part-files (data loss).
	parts, _ := filepath.Glob(filepath.Join(outDir, "main.items", "*.parquet"))
	require.NotEmpty(t, parts)
	for _, p := range parts {
		require.NoError(t, os.Remove(p))
	}
	require.Equal(t, 1, verifyCode(t, srcPath, destPath, catPath), "verify must fail on a corrupted destination")

	// A state file backfill must not touch (state-safety proof).
	bfState := filepath.Join(work, "bf-state.json")
	require.NoError(t, os.WriteFile(bfState, []byte(`{"version":1,"global":{"position":{"lsn":"0/DEADBEEF"}}}`), 0o600))
	before, err := os.ReadFile(bfState)
	require.NoError(t, err)

	// Backfill the whole rowid range and verify it repairs the destination.
	var bo, be bytes.Buffer
	code := Run([]string{"backfill", "--config", srcPath, "--destination", destPath, "--catalog", catPath,
		"--stream", "main.items", "--state", bfState}, &bo, &be)
	require.Equal(t, 0, code, "backfill stderr: %s", be.String())
	require.Equal(t, 0, verifyCode(t, srcPath, destPath, catPath), "verify must pass after backfill")

	after, err := os.ReadFile(bfState)
	require.NoError(t, err)
	require.Equal(t, before, after, "backfill must never mutate sync/CDC state")
}

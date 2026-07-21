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
func (s *countingSink) UpsertDiffRun(_ context.Context, _ int64, _, _ string, d collector.DiffRun) error {
	s.diffs++
	s.matched = d.Match
	return nil
}
func (s *countingSink) RecordLineage(context.Context, int64, string, collector.ColumnMapping, string) error {
	return nil
}
func (s *countingSink) RecordColumnStats(context.Context, int64, string, string, time.Time, []collector.ColumnStat) error {
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

	// Build lsengine from the sibling engine module.
	bin := filepath.Join(work, "lsengine")
	build := exec.Command("go", "build", "-o", bin, "./cmd/lsengine")
	build.Dir = engineDir(t)
	out, err := build.CombinedOutput()
	require.NoError(t, err, string(out))

	// Seed a sqlite source with a known table.
	dbPath := filepath.Join(work, "src.db")
	sq, err := sql.Open("sqlite", "file:"+dbPath)
	require.NoError(t, err)
	_, err = sq.Exec(`CREATE TABLE items(id INTEGER PRIMARY KEY, v TEXT NOT NULL)`)
	require.NoError(t, err)
	_, err = sq.Exec(`INSERT INTO items(id,v) VALUES (1,'a'),(2,'b'),(3,'c')`)
	require.NoError(t, err)
	require.NoError(t, sq.Close())

	sink := &countingSink{}
	ingest := collector.NewIngester(sink).Ingest
	loader := fixedLoader{cfg: runner.PipelineConfig{
		SourceType:        "sqlite",
		SourceConfig:      []byte(`{"type":"sqlite","settings":{"path":"` + dbPath + `"}}`),
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

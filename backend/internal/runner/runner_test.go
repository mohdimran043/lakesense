package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
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
	backfillOpts BackfillOpts
}

func (f *fakeEngine) Discover(_ context.Context, sourceConfigPath string) ([]byte, error) {
	f.discoverPath = sourceConfigPath
	return []byte(`{"streams":[{"namespace":"main","name":"items","schema":{"columns":[{"name":"id","type":"int64"}]}}]}`), nil
}

func (f *fakeEngine) Sync(_ context.Context, p SyncPaths, _ int64, out io.Writer) error {
	// Record the catalog the runner built, then emit the canned stream.
	b, _ := os.ReadFile(p.Catalog)
	f.syncCatalog = string(b)
	_, _ = io.WriteString(out, f.jsonl)
	return f.syncErr
}

func (f *fakeEngine) Backfill(_ context.Context, _ SyncPaths, _ int64, o BackfillOpts, out io.Writer) error {
	f.backfillOpts = o
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

func countingIngest(total *int) Ingest {
	return func(_ context.Context, _ int64, r io.Reader) (int, error) {
		b, _ := io.ReadAll(r)
		n := 0
		for _, line := range strings.Split(string(b), "\n") {
			if strings.TrimSpace(line) != "" {
				n++
			}
		}
		*total = n
		return n, nil
	}
}

func TestRunOrchestratesDiscoverThenSyncAndIngests(t *testing.T) {
	eng := &fakeEngine{jsonl: cannedJSONL()}
	var ingested int
	loader := fakeLoader{ok: true, cfg: PipelineConfig{
		SourceConfig:      []byte(`{"type":"sqlite","settings":{"path":"/tmp/x.db"}}`),
		DestinationConfig: []byte(`{"type":"parquet","settings":{"path":"/tmp/out"}}`),
		Selections:        []StreamSelection{{Namespace: "main", Name: "items", Mode: "full_load"}},
	}}
	r := New(eng, countingIngest(&ingested), loader, t.TempDir(), func() time.Time { return time.Unix(0, 0) })

	res, err := r.Run(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, 4, res.Events)
	require.Contains(t, eng.syncCatalog, "selected_streams")
	require.Contains(t, eng.discoverPath, "source.json")
}

func TestRunIngestsEvenWhenSyncFails(t *testing.T) {
	eng := &fakeEngine{jsonl: cannedJSONL(), syncErr: fmt.Errorf("exit 1")}
	var ingested int
	loader := fakeLoader{ok: true, cfg: PipelineConfig{
		SourceConfig:      []byte(`{"type":"sqlite","settings":{}}`),
		DestinationConfig: []byte(`{"type":"ndjson","settings":{"path":"/tmp/o"}}`),
		Selections:        []StreamSelection{{Namespace: "main", Name: "items", Mode: "full_load"}},
	}}
	r := New(eng, countingIngest(&ingested), loader, t.TempDir(), func() time.Time { return time.Unix(0, 0) })

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

func TestBackfillPassesOptsAndIngests(t *testing.T) {
	eng := &fakeEngine{jsonl: cannedJSONL()}
	var ingested int
	loader := fakeLoader{ok: true, cfg: PipelineConfig{
		SourceConfig:      []byte(`{"type":"sqlite","settings":{}}`),
		DestinationConfig: []byte(`{"type":"parquet","settings":{"path":"/tmp/out"}}`),
		Selections:        []StreamSelection{{Namespace: "main", Name: "items", Mode: "full_load"}},
	}}
	r := New(eng, countingIngest(&ingested), loader, t.TempDir(), func() time.Time { return time.Unix(0, 0) })

	res, err := r.Backfill(context.Background(), 7, BackfillOpts{Stream: "main.items", PKMin: "1", PKMax: "100"})
	require.NoError(t, err)
	require.Equal(t, 4, res.Events)
	require.Equal(t, "main.items", eng.backfillOpts.Stream)
	require.Equal(t, "1", eng.backfillOpts.PKMin)
}

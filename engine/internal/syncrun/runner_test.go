package syncrun

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// fakeConnector is an in-memory source implementing all three read facets, so
// the orchestrator can be exercised end-to-end without a live database. It is
// deliberately simple: integer-id rows, chunk ranges parsed as int bounds.
type fakeConnector struct {
	stream model.Stream
	rows   []sdk.Row // sorted by id ascending

	// failOnChunkCall, when >0, returns an error at the start of the Nth
	// ReadChunk call of the process, then disarms — used to simulate a mid-load
	// crash for the resume test.
	chunkCalls      int
	failOnChunkCall int

	changes  []sdk.Change
	cdcFinal map[string]string
}

func (f *fakeConnector) Spec() sdk.Spec {
	return sdk.Spec{
		Type:         "fake",
		Capabilities: []sdk.Capability{sdk.CapFullLoad, sdk.CapIncremental, sdk.CapCDC},
		Maturity:     sdk.MaturityBeta,
	}
}
func (f *fakeConnector) Setup(context.Context, json.RawMessage) error { return nil }
func (f *fakeConnector) Check(context.Context) error                  { return nil }
func (f *fakeConnector) Close(context.Context) error                  { return nil }
func (f *fakeConnector) Discover(context.Context) ([]model.Stream, error) {
	return []model.Stream{f.stream}, nil
}

// SplitChunks partitions the id space into three fixed ranges.
func (f *fakeConnector) SplitChunks(context.Context, model.Stream) ([]state.Chunk, error) {
	return []state.Chunk{
		{Min: "", Max: "3"},
		{Min: "3", Max: "5"},
		{Min: "5", Max: ""},
	}, nil
}

func (f *fakeConnector) ReadChunk(ctx context.Context, _ model.Stream, chunk state.Chunk, emit sdk.RowFunc) error {
	f.chunkCalls++
	if f.failOnChunkCall != 0 && f.chunkCalls == f.failOnChunkCall {
		f.failOnChunkCall = 0 // disarm so the resumed run succeeds
		return errors.New("injected mid-load failure")
	}
	lo := parseBound(chunk.Min, -1<<62)
	hi := parseBound(chunk.Max, 1<<62)
	for _, row := range f.rows {
		id, _ := row["id"].(int64)
		if id >= lo && id < hi {
			if err := emit(ctx, cloneRow(row)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *fakeConnector) MaxCursor(context.Context, model.Stream, string) (string, error) {
	last, _ := f.rows[len(f.rows)-1]["id"].(int64)
	return strconv.FormatInt(last, 10), nil
}

func (f *fakeConnector) ReadIncrement(ctx context.Context, _ model.Stream, field, since string, emit sdk.RowFunc) (string, error) {
	sinceN := parseBound(since, -1<<62)
	max := since
	for _, row := range f.rows {
		id, _ := row[field].(int64)
		if id > sinceN {
			if err := emit(ctx, cloneRow(row)); err != nil {
				return "", err
			}
			max = strconv.FormatInt(id, 10)
		}
	}
	return max, nil
}

func (f *fakeConnector) PrepareCDC(context.Context, []model.Stream) (map[string]string, error) {
	return map[string]string{"lsn": "0"}, nil
}

func (f *fakeConnector) StreamChanges(ctx context.Context, _ []model.Stream, _ map[string]string, emit sdk.ChangeFunc) (map[string]string, error) {
	for _, ch := range f.changes {
		if err := emit(ctx, ch); err != nil {
			return nil, err
		}
	}
	return f.cdcFinal, nil
}

func parseBound(s string, def int64) int64 {
	if s == "" {
		return def
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func cloneRow(r sdk.Row) sdk.Row {
	c := make(sdk.Row, len(r))
	for k, v := range r {
		c[k] = v
	}
	return c
}

// --- test fixtures ---

func idStream() model.Stream {
	return model.Stream{
		Namespace: "public",
		Name:      "widgets",
		Schema: model.Schema{Columns: []model.Column{
			{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
			{Name: "val", Type: model.TypeString, Nullable: true},
		}},
		SupportedSyncModes: []model.SyncMode{model.ModeFullLoad, model.ModeIncremental, model.ModeCDC},
	}
}

func idRows(n int) []sdk.Row {
	rows := make([]sdk.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = sdk.Row{"id": int64(i + 1), "val": "v" + strconv.Itoa(i+1)}
	}
	return rows
}

func catalogWith(sel model.SelectedStream) model.Catalog {
	return model.Catalog{Streams: []model.Stream{idStream()}, Selected: []model.SelectedStream{sel}}
}

// checksums extracts source/destination Checksum payloads from the event log.
func checksums(t *testing.T, log []byte) (source, dest events.Checksum, finished events.SyncFinished) {
	t.Helper()
	sc := bufio.NewScanner(bytes.NewReader(log))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		var e struct {
			Kind    events.Kind     `json:"event"`
			Payload json.RawMessage `json:"payload"`
		}
		require.NoError(t, json.Unmarshal(sc.Bytes(), &e))
		switch e.Kind {
		case events.KindChecksumComputed:
			var c events.Checksum
			require.NoError(t, json.Unmarshal(e.Payload, &c))
			if c.Side == "source" {
				source = c
			} else {
				dest = c
			}
		case events.KindSyncFinished:
			require.NoError(t, json.Unmarshal(e.Payload, &finished))
		}
	}
	require.NoError(t, sc.Err())
	return source, dest, finished
}

// ndjsonLines reads and decodes every row written to a stream file.
func ndjsonLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var m map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &m))
		out = append(out, m)
	}
	require.NoError(t, sc.Err())
	return out
}

func fixedClock() func() time.Time {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return func() time.Time { return base }
}

func newRun(t *testing.T, fc *fakeConnector, cat model.Catalog, st *state.Document, dir string) *bytes.Buffer {
	t.Helper()
	var log bytes.Buffer
	w, err := OpenWriter(DestinationConfig{Type: "ndjson", Path: dir})
	require.NoError(t, err)
	em := events.NewEmitter(&log, "sync-test", "pipe1")
	err = Run(context.Background(), Options{
		Connector:       fc,
		Writer:          w,
		Catalog:         cat,
		State:           st,
		Emitter:         em,
		ConnectorType:   "fake",
		DestinationType: "ndjson",
		Now:             fixedClock(),
	})
	require.NoError(t, w.Close(context.Background()))
	if err != nil {
		// return the log to the caller with the error surfaced via t.Fatal-free
		// path; callers that expect failure inspect via runExpectErr instead.
		t.Fatalf("Run failed: %v", err)
	}
	return &log
}

// --- tests ---

func TestFullLoadWritesEveryRowWithMatchingChecksums(t *testing.T) {
	dir := t.TempDir()
	fc := &fakeConnector{stream: idStream(), rows: idRows(6)}
	cat := catalogWith(model.SelectedStream{Namespace: "public", Name: "widgets", Mode: model.ModeFullLoad})

	log := newRun(t, fc, cat, state.NewInMemory(), dir)

	src, dst, fin := checksums(t, log.Bytes())
	assert.Equal(t, int64(6), src.Rows)
	assert.Equal(t, int64(6), dst.Rows)
	assert.Equal(t, src.Checksum, dst.Checksum, "source and destination checksums must match")
	assert.Equal(t, int64(6), fin.RowsWritten)
	assert.Equal(t, int64(6), fin.RowsRead, "run-level rows_read must count full-load rows, not stay 0")

	rows := ndjsonLines(t, filepath.Join(dir, "public.widgets.ndjson"))
	require.Len(t, rows, 6)
	// Engine metadata injected on every row.
	assert.Equal(t, "r", rows[0][model.ColOpType])
	assert.NotEmpty(t, rows[0][model.ColRecordID])
	assert.NotEmpty(t, rows[0][model.ColIngestedAt])
}

func TestFullLoadResumesAfterCrashWithoutDuplicatesOrLoss(t *testing.T) {
	dir := t.TempDir()
	st := state.NewInMemory()
	cat := catalogWith(model.SelectedStream{Namespace: "public", Name: "widgets", Mode: model.ModeFullLoad})

	// First run crashes at the start of the 2nd chunk: chunk 1 is durably
	// committed, chunks 2 and 3 are not.
	fc1 := &fakeConnector{stream: idStream(), rows: idRows(6), failOnChunkCall: 2}
	var log1 bytes.Buffer
	w1, err := OpenWriter(DestinationConfig{Type: "ndjson", Path: dir})
	require.NoError(t, err)
	em1 := events.NewEmitter(&log1, "sync-1", "pipe1")
	err = Run(context.Background(), Options{
		Connector: fc1, Writer: w1, Catalog: cat, State: st, Emitter: em1,
		ConnectorType: "fake", DestinationType: "ndjson", Now: fixedClock(),
	})
	require.Error(t, err, "first run must fail")
	require.NoError(t, w1.Close(context.Background()))

	// Only chunk 1's rows (ids 1,2) reached the destination.
	rows := ndjsonLines(t, filepath.Join(dir, "public.widgets.ndjson"))
	require.Len(t, rows, 2)

	// Resume with the SAME state document: remaining chunks are re-read and
	// appended; nothing is re-read from the committed chunk.
	fc2 := &fakeConnector{stream: idStream(), rows: idRows(6)}
	log2 := newRun(t, fc2, cat, st, dir)

	rows = ndjsonLines(t, filepath.Join(dir, "public.widgets.ndjson"))
	require.Len(t, rows, 6, "every row present exactly once after resume")
	ids := map[int64]bool{}
	for _, r := range rows {
		idf, _ := r["id"].(float64)
		ids[int64(idf)] = true
	}
	assert.Len(t, ids, 6)

	// The resumed run reports itself as resumed.
	assert.Contains(t, log2.String(), `"resumed":true`)
}

func TestIncrementalOnlyReadsPastTheCursor(t *testing.T) {
	dir := t.TempDir()
	st := state.NewInMemory()
	cat := catalogWith(model.SelectedStream{
		Namespace: "public", Name: "widgets", Mode: model.ModeIncremental, CursorField: "id",
	})

	// First increment: no cursor yet → all 4 rows.
	fc1 := &fakeConnector{stream: idStream(), rows: idRows(4)}
	log1 := newRun(t, fc1, cat, st, dir)
	_, dst1, _ := checksums(t, log1.Bytes())
	assert.Equal(t, int64(4), dst1.Rows)
	cur, ok := st.Cursor("public", "widgets", "id")
	require.True(t, ok)
	assert.Equal(t, "4", cur)

	// Two new rows arrive (ids 5,6). Second increment reads only those.
	fc2 := &fakeConnector{stream: idStream(), rows: idRows(6)}
	log2 := newRun(t, fc2, cat, st, dir)
	_, dst2, _ := checksums(t, log2.Bytes())
	assert.Equal(t, int64(2), dst2.Rows, "only rows past cursor are re-read")
	cur, _ = st.Cursor("public", "widgets", "id")
	assert.Equal(t, "6", cur)
}

func TestCDCBackfillThenStreamsChangesWithOpTypes(t *testing.T) {
	dir := t.TempDir()
	st := state.NewInMemory()
	cat := catalogWith(model.SelectedStream{Namespace: "public", Name: "widgets", Mode: model.ModeCDC})

	fc := &fakeConnector{
		stream: idStream(),
		rows:   idRows(3), // backfill snapshot
		changes: []sdk.Change{
			{StreamID: "public.widgets", Kind: sdk.ChangeInsert, Data: sdk.Row{"id": int64(4), "val": "v4"}, Timestamp: time.Unix(10, 0)},
			{StreamID: "public.widgets", Kind: sdk.ChangeUpdate, Data: sdk.Row{"id": int64(1), "val": "v1b"}, Timestamp: time.Unix(11, 0)},
			{StreamID: "public.widgets", Kind: sdk.ChangeDelete, Data: sdk.Row{"id": int64(2), "val": "v2"}, Timestamp: time.Unix(12, 0)},
		},
		cdcFinal: map[string]string{"lsn": "12"},
	}

	newRun(t, fc, cat, st, dir)

	// Anchor persisted and advanced to the final position; stream marked
	// backfilled under the anchor.
	pos, ok := st.GlobalPosition()
	require.True(t, ok)
	assert.Equal(t, "12", pos["lsn"])
	assert.True(t, st.BackfilledGlobally("public.widgets"))

	// Destination holds 3 backfilled rows (op "r") plus 3 CDC rows with their
	// op types.
	rows := ndjsonLines(t, filepath.Join(dir, "public.widgets.ndjson"))
	require.Len(t, rows, 6)
	ops := map[string]int{}
	for _, r := range rows {
		op, _ := r[model.ColOpType].(string)
		ops[op]++
	}
	assert.Equal(t, 3, ops["r"])
	assert.Equal(t, 1, ops["i"])
	assert.Equal(t, 1, ops["u"])
	assert.Equal(t, 1, ops["d"])
}

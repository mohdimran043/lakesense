package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/events"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/lakesense/lakesense/engine/internal/syncrun"
)

// fakeConn is a minimal FullLoader connector replaying a fixed row set.
type fakeConn struct{ rows []sdk.Row }

func (c *fakeConn) Spec() sdk.Spec                                   { return sdk.Spec{Type: "fake"} }
func (c *fakeConn) Setup(context.Context, json.RawMessage) error     { return nil }
func (c *fakeConn) Check(context.Context) error                      { return nil }
func (c *fakeConn) Discover(context.Context) ([]model.Stream, error) { return nil, nil }
func (c *fakeConn) Close(context.Context) error                      { return nil }
func (c *fakeConn) SplitChunks(context.Context, model.Stream) ([]state.Chunk, error) {
	return []state.Chunk{{}}, nil
}
func (c *fakeConn) ReadChunk(ctx context.Context, _ model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	for _, r := range c.rows {
		clone := sdk.Row{}
		for k, v := range r {
			clone[k] = v
		}
		if err := emit(ctx, clone); err != nil {
			return err
		}
	}
	return nil
}

func timeAt(rfc3339 string) time.Time {
	t, _ := time.Parse(time.RFC3339, rfc3339)
	return t
}

func testStream() model.Stream {
	return model.Stream{Namespace: "public", Name: "t", Schema: model.Schema{Columns: []model.Column{
		{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "v", Type: model.TypeString},
	}}}
}

// TestBackfillCorrectsAndVerifies seeds the destination with a wrong value for
// id=2, then backfills from a source that holds the correct value and asserts
// the closing verify passes (merge-on-read resolved the correction).
func TestBackfillCorrectsAndVerifies(t *testing.T) {
	dir := t.TempDir()
	stream := testStream()
	sel := model.SelectedStream{Namespace: "public", Name: "t", Mode: model.ModeFullLoad}

	// Seed destination via a normal write with a stale/wrong id=2 value.
	w, _ := syncrun.OpenWriter(syncrun.DestinationConfig{Type: "parquet", Path: dir})
	sw, _ := w.Open(context.Background(), stream, "", true)
	for _, r := range []sdk.Row{{"id": int64(1), "v": "a"}, {"id": int64(2), "v": "WRONG"}} {
		syncrun.InjectMetadata(r, []string{"id"}, "r", timeAt("2026-07-21T00:00:00Z"))
		_ = sw.WriteRow(context.Background(), r)
	}
	_, _ = sw.Close(context.Background())
	_ = w.Close(context.Background())

	// Source now holds the corrected value for id=2 (plus the unchanged id=1).
	source := &fakeConn{rows: []sdk.Row{{"id": int64(1), "v": "a"}, {"id": int64(2), "v": "corrected"}}}
	writer, _ := syncrun.OpenWriter(syncrun.DestinationConfig{Type: "parquet", Path: dir})
	reader, _ := syncrun.OpenReader(syncrun.DestinationConfig{Type: "parquet", Path: dir})

	res, err := Run(context.Background(), Options{
		Connector: source, Writer: writer, Reader: reader,
		Stream: stream, Selection: sel,
		Emitter: events.NewEmitter(&bytes.Buffer{}, "sync1", ""),
		Now:     func() time.Time { return timeAt("2026-07-21T01:00:00Z") }, // newer than the seed
	})
	require.NoError(t, err)
	require.True(t, res.Match, "backfill should reconcile the destination with the source")
}

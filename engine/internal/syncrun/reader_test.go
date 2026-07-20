package syncrun

import (
	"context"
	"testing"

	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/stretchr/testify/require"
)

func TestResolveCurrentStateLatestWinsDeletesDrop(t *testing.T) {
	rows := []sdk.Row{
		{"_ls_id": "a", "_ls_ingested_at": "t1", "_ls_op": "r", "v": int64(1)},
		{"_ls_id": "a", "_ls_ingested_at": "t2", "_ls_op": "u", "v": int64(2)},
		{"_ls_id": "b", "_ls_ingested_at": "t1", "_ls_op": "r", "v": int64(9)},
		{"_ls_id": "b", "_ls_ingested_at": "t3", "_ls_op": "d", "v": int64(9)},
	}
	got := resolveCurrentState(rows)
	require.Len(t, got, 1)
	require.Equal(t, "a", got[0]["_ls_id"])
	require.Equal(t, int64(2), got[0]["v"])
}

func TestParquetReaderReadsBackWhatWriterWrote(t *testing.T) {
	dir := t.TempDir()
	w, err := newParquetWriter(dir)
	require.NoError(t, err)
	stream := testStream()
	sw, _ := w.Open(context.Background(), stream, "", true)
	_ = sw.WriteRow(context.Background(), sdk.Row{"id": int64(1), "amount": "10.00", "note": "a", "ok": true, "_ls_id": "1", "_ls_ingested_at": "t1", "_ls_op": "r"})
	_ = sw.WriteRow(context.Background(), sdk.Row{"id": int64(2), "amount": "20.00", "note": nil, "ok": false, "_ls_id": "2", "_ls_ingested_at": "t1", "_ls_op": "r"})
	_, _ = sw.Close(context.Background())
	_ = w.Close(context.Background())

	r, err := OpenReader(DestinationConfig{Type: "parquet", Path: dir})
	require.NoError(t, err)
	sr, err := r.OpenRead(context.Background(), stream, "")
	require.NoError(t, err)
	var got []sdk.Row
	require.NoError(t, sr.Read(context.Background(), nil, func(_ context.Context, row sdk.Row) error {
		got = append(got, row)
		return nil
	}))
	require.Len(t, got, 2)
}

func TestParquetResumeAppendsWithoutLoss(t *testing.T) {
	dir := t.TempDir()
	stream := testStream()

	// First run: write one flushed part-file, then simulate a crash (no Close).
	w1, _ := newParquetWriter(dir)
	sw1, _ := w1.Open(context.Background(), stream, "", true)
	_ = sw1.WriteRow(context.Background(), sdk.Row{"id": int64(1), "amount": "1", "note": "a", "ok": true, "_ls_id": "1", "_ls_ingested_at": "t1", "_ls_op": "r"})
	require.NoError(t, sw1.Flush(context.Background())) // durable part-file
	// crash: do NOT Close sw1; drop the writer.

	// Resume: append (truncate=false) the second chunk.
	w2, _ := newParquetWriter(dir)
	sw2, _ := w2.Open(context.Background(), stream, "", false)
	_ = sw2.WriteRow(context.Background(), sdk.Row{"id": int64(2), "amount": "2", "note": "b", "ok": false, "_ls_id": "2", "_ls_ingested_at": "t2", "_ls_op": "r"})
	_, _ = sw2.Close(context.Background())
	_ = w2.Close(context.Background())

	r, _ := OpenReader(DestinationConfig{Type: "parquet", Path: dir})
	sr, _ := r.OpenRead(context.Background(), stream, "")
	ids := map[string]bool{}
	_ = sr.Read(context.Background(), nil, func(_ context.Context, row sdk.Row) error {
		ids[row["_ls_id"].(string)] = true
		return nil
	})
	require.True(t, ids["1"], "row from before the crash must survive")
	require.True(t, ids["2"], "row from the resumed run must be present")
}

package syncrun

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func TestParquetWriterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w, err := newParquetWriter(dir)
	require.NoError(t, err)
	stream := testStream()

	sw, err := w.Open(context.Background(), stream, "", true)
	require.NoError(t, err)

	rows := []sdk.Row{
		{"id": int64(1), "amount": "10.00", "note": "a", "ok": true, "_ls_id": "1", "_ls_ingested_at": "t1", "_ls_op": "r"},
		{"id": int64(2), "amount": "20.00", "note": nil, "ok": false, "_ls_id": "2", "_ls_ingested_at": "t1", "_ls_op": "r"},
	}
	for _, r := range rows {
		require.NoError(t, sw.WriteRow(context.Background(), r))
	}
	require.NoError(t, sw.Flush(context.Background()))
	res, err := sw.Close(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), res.Rows)
	require.NoError(t, w.Close(context.Background()))

	// The written part-file(s) are valid Parquet with 2 rows total.
	parts, _ := filepath.Glob(filepath.Join(dir, "public.orders", "*.parquet"))
	require.NotEmpty(t, parts)
	var total int64
	for _, p := range parts {
		f, err := os.Open(p)
		require.NoError(t, err)
		st, _ := f.Stat()
		pf, err := parquet.OpenFile(f, st.Size())
		require.NoError(t, err)
		total += pf.NumRows()
		_ = f.Close()
	}
	require.Equal(t, int64(2), total)
}

package verify

import (
	"context"
	"testing"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
	"github.com/lakesense/lakesense/engine/internal/syncrun"
	"github.com/stretchr/testify/require"
)

// fakeFullLoader replays a fixed row set; SplitChunks returns one unbounded chunk.
type fakeFullLoader struct{ rows []sdk.Row }

func (f *fakeFullLoader) SplitChunks(context.Context, model.Stream) ([]state.Chunk, error) {
	return []state.Chunk{{}}, nil
}

func (f *fakeFullLoader) ReadChunk(ctx context.Context, _ model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	for _, r := range f.rows {
		if err := emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

// sliceReader is a StreamReader over an in-memory current-state slice.
type sliceReader struct{ rows []sdk.Row }

func (s *sliceReader) Read(ctx context.Context, _ *syncrun.PKRange, emit sdk.RowFunc) error {
	for _, r := range s.rows {
		if err := emit(ctx, r); err != nil {
			return err
		}
	}
	return nil
}

func (s *sliceReader) Close(context.Context) error { return nil }

func testStream() model.Stream {
	return model.Stream{Namespace: "public", Name: "t", Schema: model.Schema{Columns: []model.Column{
		{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "v", Type: model.TypeString},
	}}}
}

// destRow copies a source data row and stamps the _ls_id the writer would have.
func destRow(data sdk.Row) sdk.Row {
	out := sdk.Row{}
	for k, v := range data {
		out[k] = v
	}
	out[model.ColRecordID] = syncrun.RecordID(data, []string{"id"})
	return out
}

func TestVerifyMatch(t *testing.T) {
	src := []sdk.Row{{"id": int64(1), "v": "a"}, {"id": int64(2), "v": "b"}}
	dst := []sdk.Row{destRow(sdk.Row{"id": int64(2), "v": "b"}), destRow(sdk.Row{"id": int64(1), "v": "a"})}

	res, err := VerifyStream(context.Background(), StreamInput{
		Stream: testStream(), Source: &fakeFullLoader{rows: src}, DestReader: &sliceReader{rows: dst},
	})
	require.NoError(t, err)
	require.True(t, res.Match)
	require.Equal(t, int64(2), res.SourceRows)
	require.Equal(t, int64(2), res.DestinationRows)
	require.Empty(t, res.SamplePKs)
}

func TestVerifyDetectsDroppedRow(t *testing.T) {
	src := []sdk.Row{
		{"id": int64(1), "v": "a"},
		{"id": int64(2), "v": "b"},
		{"id": int64(3), "v": "c"},
	}
	dst := []sdk.Row{ // id=2 dropped on the destination
		destRow(sdk.Row{"id": int64(1), "v": "a"}),
		destRow(sdk.Row{"id": int64(3), "v": "c"}),
	}
	res, err := VerifyStream(context.Background(), StreamInput{
		Stream: testStream(), Source: &fakeFullLoader{rows: src}, DestReader: &sliceReader{rows: dst},
	})
	require.NoError(t, err)
	require.False(t, res.Match)
	require.Equal(t, int64(3), res.SourceRows)
	require.Equal(t, int64(2), res.DestinationRows)
	require.Contains(t, res.SamplePKs, "2")
	require.NotEmpty(t, res.MismatchedRanges)
}

func TestVerifyDetectsCorruptedValue(t *testing.T) {
	src := []sdk.Row{{"id": int64(1), "v": "a"}, {"id": int64(2), "v": "b"}}
	dst := []sdk.Row{
		destRow(sdk.Row{"id": int64(1), "v": "a"}),
		destRow(sdk.Row{"id": int64(2), "v": "CORRUPT"}),
	}
	res, err := VerifyStream(context.Background(), StreamInput{
		Stream: testStream(), Source: &fakeFullLoader{rows: src}, DestReader: &sliceReader{rows: dst},
	})
	require.NoError(t, err)
	require.False(t, res.Match)
	require.Contains(t, res.SamplePKs, "2")
}

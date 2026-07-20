package syncrun

import (
	"testing"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/stretchr/testify/require"
)

func testStream() model.Stream {
	return model.Stream{
		Namespace: "public", Name: "orders",
		Schema: model.Schema{Columns: []model.Column{
			{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
			{Name: "amount", Type: model.TypeDecimal},
			{Name: "note", Type: model.TypeString, Nullable: true},
			{Name: "ok", Type: model.TypeBool},
		}},
	}
}

func TestBuildParquetSchemaRoundTrip(t *testing.T) {
	stream := testStream()
	schema, names, colType := buildParquetSchema(stream)
	require.NotNil(t, schema)
	require.ElementsMatch(t, []string{"id", "amount", "note", "ok",
		"_ls_id", "_ls_ingested_at", "_ls_op", "_ls_cdc_timestamp"}, names)

	colIndex := map[string]int{}
	for i, n := range names {
		colIndex[n] = i
	}

	row := sdk.Row{"id": int64(7), "amount": "12.34", "note": nil, "ok": true,
		"_ls_id": "abc", "_ls_ingested_at": "2026-07-21T00:00:00Z", "_ls_op": "r"}
	pr := rowToParquet(row, colIndex, colType, len(names))
	back := parquetToRow(pr, names)

	require.Equal(t, int64(7), back["id"])
	require.Equal(t, "12.34", back["amount"])
	require.Nil(t, back["note"])
	require.Equal(t, true, back["ok"])
	require.Equal(t, "abc", back["_ls_id"])
}

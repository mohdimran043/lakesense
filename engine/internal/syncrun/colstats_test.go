package syncrun

import (
	"testing"

	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/stretchr/testify/require"
)

func TestColStatsAccumulation(t *testing.T) {
	cs := newStreamColStats()
	cols := []string{"id", "name"}
	cs.observe(sdk.Row{"id": int64(1), "name": "a"}, cols)
	cs.observe(sdk.Row{"id": int64(2), "name": nil}, cols) // name null
	cs.observe(sdk.Row{"id": int64(3), "name": "a"}, cols) // duplicate name
	cs.observe(sdk.Row{"id": int64(4)}, cols)              // name absent → null

	res := cs.result()
	byCol := map[string]int{}
	for i, c := range res.Columns {
		byCol[c.Column] = i
	}

	id := res.Columns[byCol["id"]]
	require.EqualValues(t, 4, id.Rows)
	require.EqualValues(t, 0, id.Nulls)
	require.EqualValues(t, 4, id.Distinct)
	require.Equal(t, "1", id.Min)
	require.Equal(t, "4", id.Max)

	name := res.Columns[byCol["name"]]
	require.EqualValues(t, 4, name.Rows)
	require.EqualValues(t, 2, name.Nulls, "one explicit nil + one absent")
	require.EqualValues(t, 1, name.Distinct, `only "a"`)
}

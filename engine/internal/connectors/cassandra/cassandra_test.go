package cassandra

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestMapType(t *testing.T) {
	cases := map[string]model.DataType{
		"int": model.TypeInt32, "smallint": model.TypeInt32, "tinyint": model.TypeInt32,
		"bigint": model.TypeInt64, "counter": model.TypeInt64, "varint": model.TypeInt64,
		"boolean": model.TypeBool,
		"float":   model.TypeFloat32, "double": model.TypeFloat64,
		"decimal": model.TypeDecimal,
		"date":    model.TypeDate, "timestamp": model.TypeTimestamp,
		"blob": model.TypeBinary,
		"text": model.TypeString, "uuid": model.TypeString, "timeuuid": model.TypeString, "inet": model.TypeString,
		"list<text>": model.TypeJSON, "map<text, int>": model.TypeJSON, "set<int>": model.TypeJSON,
		"frozen<list<text>>": model.TypeJSON,
	}
	for in, want := range cases {
		require.Equalf(t, want, mapType(in), "mapType(%q)", in)
	}
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "cassandra", spec.Type)
	require.Equal(t, sdk.MaturityBeta, spec.Maturity)
}

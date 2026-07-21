package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestMapType(t *testing.T) {
	cases := map[string]model.DataType{
		"Int32": model.TypeInt32, "UInt16": model.TypeInt32,
		"Int64": model.TypeInt64, "UInt64": model.TypeInt64,
		"Float32": model.TypeFloat32, "Float64": model.TypeFloat64,
		"Decimal(10, 2)": model.TypeDecimal,
		"Bool":           model.TypeBool,
		"Date":           model.TypeDate, "DateTime": model.TypeTimestamp, "DateTime64(3)": model.TypeTimestamp,
		"Array(String)": model.TypeJSON, "Map(String, Int32)": model.TypeJSON,
		"String": model.TypeString, "FixedString(4)": model.TypeString, "UUID": model.TypeString,
		"Nullable(Int32)":                 model.TypeInt32,
		"LowCardinality(String)":          model.TypeString,
		"Nullable(DateTime)":              model.TypeTimestamp,
		"LowCardinality(Nullable(Int64))": model.TypeInt64,
	}
	for in, want := range cases {
		require.Equalf(t, want, mapType(in), "mapType(%q)", in)
	}
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "clickhouse", spec.Type)
	require.Equal(t, sdk.MaturityBeta, spec.Maturity)
}

package mysql

import (
	"testing"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/stretchr/testify/require"
)

func TestMapType(t *testing.T) {
	cases := map[string]model.DataType{
		"int": model.TypeInt32, "integer": model.TypeInt32, "tinyint": model.TypeInt32, "year": model.TypeInt32,
		"bigint":  model.TypeInt64,
		"decimal": model.TypeDecimal, "numeric": model.TypeDecimal,
		"float": model.TypeFloat32, "double": model.TypeFloat64, "real": model.TypeFloat64,
		"bool": model.TypeBool, "boolean": model.TypeBool, "bit": model.TypeBool,
		"date": model.TypeDate, "datetime": model.TypeTimestamp, "timestamp": model.TypeTimestamp,
		"json": model.TypeJSON,
		"blob": model.TypeBinary, "varbinary": model.TypeBinary,
		"varchar": model.TypeString, "text": model.TypeString, "enum": model.TypeString, "time": model.TypeString,
	}
	for in, want := range cases {
		require.Equalf(t, want, mapType(in), "mapType(%q)", in)
	}
}

func TestSpecCapabilitiesAreImplemented(t *testing.T) {
	// The connector must implement exactly the facets it declares — a Stable
	// MySQL is full-load + incremental (binlog CDC joins once its battery passes).
	require.NoError(t, sdk.ValidateCapabilities(New()))
	spec := New().Spec()
	require.Equal(t, "mysql", spec.Type)
	require.Contains(t, spec.Capabilities, sdk.CapFullLoad)
	require.Contains(t, spec.Capabilities, sdk.CapIncremental)
}

func TestQuoteIdent(t *testing.T) {
	require.Equal(t, "`orders`", quoteIdent("orders"))
	require.Equal(t, "`we``ird`", quoteIdent("we`ird"))
}

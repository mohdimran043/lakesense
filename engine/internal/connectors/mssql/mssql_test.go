package mssql

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

func TestMapType(t *testing.T) {
	cases := map[string]model.DataType{
		"int": model.TypeInt32, "smallint": model.TypeInt32, "tinyint": model.TypeInt32,
		"bigint":  model.TypeInt64,
		"bit":     model.TypeBool,
		"decimal": model.TypeDecimal, "numeric": model.TypeDecimal, "money": model.TypeDecimal,
		"real": model.TypeFloat32, "float": model.TypeFloat64,
		"date": model.TypeDate, "datetime2": model.TypeTimestamp, "datetime": model.TypeTimestamp,
		"varbinary": model.TypeBinary, "image": model.TypeBinary,
		"nvarchar": model.TypeString, "uniqueidentifier": model.TypeString, "xml": model.TypeString,
	}
	for in, want := range cases {
		require.Equalf(t, want, mapType(in), "mapType(%q)", in)
	}
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	require.Equal(t, "sqlserver", New().Spec().Type)
}

func TestQuoteIdent(t *testing.T) {
	require.Equal(t, "[orders]", quoteIdent("orders"))
	require.Equal(t, "[we]]ird]", quoteIdent("we]ird"))
}

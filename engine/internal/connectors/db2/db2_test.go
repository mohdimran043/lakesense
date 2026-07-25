//go:build db2

package db2

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

func TestMapType(t *testing.T) {
	cases := map[string]model.DataType{
		"SMALLINT":         model.TypeInt64,
		"INTEGER":          model.TypeInt64,
		"BIGINT":           model.TypeInt64,
		"DECIMAL":          model.TypeDecimal,
		"NUMERIC":          model.TypeDecimal,
		"DECFLOAT":         model.TypeDecimal,
		"REAL":             model.TypeFloat64,
		"FLOAT":            model.TypeFloat64,
		"DOUBLE":           model.TypeFloat64,
		"DOUBLE PRECISION": model.TypeFloat64,
		"CHARACTER":        model.TypeString,
		"VARCHAR":          model.TypeString,
		"CLOB":             model.TypeString,
		"GRAPHIC":          model.TypeString,
		"VARGRAPHIC":       model.TypeString,
		"DBCLOB":           model.TypeString,
		"XML":              model.TypeString,
		"DATE":             model.TypeTimestamp,
		"TIME":             model.TypeTimestamp,
		"TIMESTAMP":        model.TypeTimestamp,
		"BLOB":             model.TypeString,
	}
	for in, want := range cases {
		require.Equalf(t, want, mapType(in), "mapType(%q)", in)
		require.Equalf(t, want, mapType("  "+in+"  "), "mapType trims %q", in)
	}
}

func TestDSN(t *testing.T) {
	c := Config{Host: "localhost", Port: 50000, Database: "testdb", User: "db2inst1", Password: "Test1234"}
	require.Equal(t,
		"HOSTNAME=localhost;DATABASE=testdb;PORT=50000;UID=db2inst1;PWD=Test1234",
		c.dsn())
}

func TestValidateDefaults(t *testing.T) {
	c := Config{Host: "h", Database: "d", User: "u"}
	require.NoError(t, c.validate())
	require.Equal(t, 50000, c.Port)
	require.Equal(t, int64(50_000), c.ChunkRows)

	require.Error(t, (&Config{Database: "d", User: "u"}).validate(), "missing host")
	require.Error(t, (&Config{Host: "h", User: "u"}).validate(), "missing database")
	require.Error(t, (&Config{Host: "h", Database: "d"}).validate(), "missing user")
	require.Error(t, (&Config{Type: "mysql", Host: "h", Database: "d", User: "u"}).validate(), "wrong type")
}

func TestQuoteIdent(t *testing.T) {
	require.Equal(t, `"ORDERS"`, quoteIdent("ORDERS"))
	require.Equal(t, `"we""ird"`, quoteIdent(`we"ird`))
}

func streamWithIntPK() model.Stream {
	return model.Stream{
		Namespace: "DB2INST1",
		Name:      "ITEMS",
		Schema: model.Schema{Columns: []model.Column{
			{Name: "ID", Type: model.TypeInt64, PrimaryKey: true},
			{Name: "LABEL", Type: model.TypeString},
		}},
	}
}

func TestChunkQuery(t *testing.T) {
	st := streamWithIntPK()

	q, args := chunkQuery(st, state.Chunk{Max: "100"})
	require.Equal(t, `SELECT * FROM "DB2INST1"."ITEMS" WHERE "ID" < ?`, q)
	require.Equal(t, []any{"100"}, args)

	q, args = chunkQuery(st, state.Chunk{Min: "100", Max: "200"})
	require.Equal(t, `SELECT * FROM "DB2INST1"."ITEMS" WHERE "ID" >= ? AND "ID" < ?`, q)
	require.Equal(t, []any{"100", "200"}, args)

	q, args = chunkQuery(st, state.Chunk{Min: "200"})
	require.Equal(t, `SELECT * FROM "DB2INST1"."ITEMS" WHERE "ID" >= ?`, q)
	require.Equal(t, []any{"200"}, args)

	// No integer PK → single unbounded scan, no binds.
	noPK := st
	noPK.Schema = model.Schema{Columns: []model.Column{{Name: "LABEL", Type: model.TypeString}}}
	q, args = chunkQuery(noPK, state.Chunk{})
	require.Equal(t, `SELECT * FROM "DB2INST1"."ITEMS"`, q)
	require.Nil(t, args)
}

func TestSuggestCursor(t *testing.T) {
	ts := model.Schema{Columns: []model.Column{
		{Name: "ID", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "UPDATED_AT", Type: model.TypeTimestamp},
	}}
	require.Equal(t, "UPDATED_AT", suggestCursor(ts))

	pkOnly := model.Schema{Columns: []model.Column{{Name: "ID", Type: model.TypeInt64, PrimaryKey: true}}}
	require.Equal(t, "ID", suggestCursor(pkOnly))

	none := model.Schema{Columns: []model.Column{{Name: "LABEL", Type: model.TypeString}}}
	require.Equal(t, "", suggestCursor(none))
}

func TestCursorBind(t *testing.T) {
	stream := model.Stream{Schema: model.Schema{Columns: []model.Column{
		{Name: "ID", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "CREATED_AT", Type: model.TypeTimestamp},
		{Name: "LABEL", Type: model.TypeString},
	}}}

	// Timestamp cursor: RFC3339Nano string parses back to a UTC time.Time so
	// go_ibm_db binds a native Db2 timestamp instead of a rejected string cast.
	got := cursorBind(stream, "CREATED_AT", "2026-07-25T10:30:00.5Z")
	tv, ok := got.(time.Time)
	require.True(t, ok, "timestamp cursor must bind as time.Time, got %T", got)
	require.True(t, tv.Equal(time.Date(2026, 7, 25, 10, 30, 0, 500_000_000, time.UTC)))

	// Numeric cursor stays a string.
	require.Equal(t, "42", cursorBind(stream, "ID", "42"))

	// Unparseable timestamp value falls back to the raw string, no panic.
	require.Equal(t, "not-a-date", cursorBind(stream, "CREATED_AT", "not-a-date"))
}

func TestNormalizeValue(t *testing.T) {
	require.Nil(t, normalizeValue(nil, model.TypeString, false))
	require.Equal(t, int64(42), normalizeValue("42", model.TypeInt64, false))
	require.Equal(t, int64(7), normalizeValue([]byte("7"), model.TypeInt64, false))
	require.Equal(t, "12.34", normalizeValue("12.34", model.TypeDecimal, false))
	require.Equal(t, "deadbeef", normalizeValue([]byte{0xde, 0xad, 0xbe, 0xef}, model.TypeString, true))
	require.Equal(t, "hello", normalizeValue([]byte("hello"), model.TypeString, false))
	require.Equal(t, "2026-07-25T10:30:00Z",
		normalizeValue(time.Date(2026, 7, 25, 10, 30, 0, 0, time.UTC), model.TypeTimestamp, false))
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	require.Equal(t, "db2", New().Spec().Type)
	require.Equal(t, sdk.MaturityBeta, New().Spec().Maturity)
	require.Equal(t, "IBM Db2", New().Spec().DisplayName)
}

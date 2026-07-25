package oracle

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

func num(precision, scale int64, validP, validS bool) (sql.NullInt64, sql.NullInt64) {
	return sql.NullInt64{Int64: precision, Valid: validP}, sql.NullInt64{Int64: scale, Valid: validS}
}

func TestMapType(t *testing.T) {
	// NUMBER variants (precision/scale sensitive).
	p, s := num(10, 0, true, true)
	require.Equal(t, model.TypeInt64, mapType("NUMBER", p, s), "NUMBER(10,0) → int64")

	p, s = num(0, 0, false, true)
	require.Equal(t, model.TypeInt64, mapType("NUMBER", p, s), "INTEGER / NUMBER(*,0) → int64")

	p, s = num(10, 2, true, true)
	require.Equal(t, model.TypeDecimal, mapType("NUMBER", p, s), "NUMBER(10,2) → decimal")

	p, s = num(38, 0, true, true)
	require.Equal(t, model.TypeDecimal, mapType("NUMBER", p, s), "NUMBER(38,0) → decimal (overflows int64)")

	p, s = num(0, 0, false, false)
	require.Equal(t, model.TypeDecimal, mapType("NUMBER", p, s), "plain NUMBER → decimal")

	// Scalar types (precision/scale ignored).
	np, ns := num(0, 0, false, false)
	cases := map[string]model.DataType{
		"FLOAT":                       model.TypeDecimal,
		"BINARY_FLOAT":                model.TypeFloat64,
		"BINARY_DOUBLE":               model.TypeFloat64,
		"DATE":                        model.TypeTimestamp,
		"TIMESTAMP(6)":                model.TypeTimestamp,
		"TIMESTAMP(6) WITH TIME ZONE": model.TypeTimestamp,
		"VARCHAR2":                    model.TypeString,
		"NVARCHAR2":                   model.TypeString,
		"CHAR":                        model.TypeString,
		"NCHAR":                       model.TypeString,
		"CLOB":                        model.TypeString,
		"NCLOB":                       model.TypeString,
		"RAW":                         model.TypeString,
		"BLOB":                        model.TypeString,
		"ROWID":                       model.TypeString,
	}
	for in, want := range cases {
		require.Equalf(t, want, mapType(in, np, ns), "mapType(%q)", in)
	}
}

func TestDSN(t *testing.T) {
	c := Config{Host: "localhost", Port: 1521, Service: "FREEPDB1", User: "system", Password: "Test1234"}
	dsn := c.dsn()
	require.Contains(t, dsn, "oracle://")
	require.Contains(t, dsn, "localhost:1521")
	require.Contains(t, dsn, "FREEPDB1")
}

func TestValidateDefaults(t *testing.T) {
	c := Config{Host: "h", Service: "FREEPDB1", User: "u"}
	require.NoError(t, c.validate())
	require.Equal(t, 1521, c.Port)
	require.Equal(t, int64(50_000), c.ChunkRows)

	require.Error(t, (&Config{Service: "s", User: "u"}).validate(), "missing host")
	require.Error(t, (&Config{Host: "h", User: "u"}).validate(), "missing service")
	require.Error(t, (&Config{Host: "h", Service: "s"}).validate(), "missing user")
	require.Error(t, (&Config{Type: "mysql", Host: "h", Service: "s", User: "u"}).validate(), "wrong type")
}

func TestQuoteIdent(t *testing.T) {
	require.Equal(t, `"ORDERS"`, quoteIdent("ORDERS"))
	require.Equal(t, `"we""ird"`, quoteIdent(`we"ird`))
}

func streamWithIntPK() model.Stream {
	return model.Stream{
		Namespace: "SYSTEM",
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
	require.Equal(t, `SELECT * FROM "SYSTEM"."ITEMS" WHERE "ID" < :1`, q)
	require.Equal(t, []any{"100"}, args)

	q, args = chunkQuery(st, state.Chunk{Min: "100", Max: "200"})
	require.Equal(t, `SELECT * FROM "SYSTEM"."ITEMS" WHERE "ID" >= :1 AND "ID" < :2`, q)
	require.Equal(t, []any{"100", "200"}, args)

	q, args = chunkQuery(st, state.Chunk{Min: "200"})
	require.Equal(t, `SELECT * FROM "SYSTEM"."ITEMS" WHERE "ID" >= :1`, q)
	require.Equal(t, []any{"200"}, args)

	// No integer PK → single unbounded scan, no binds.
	noPK := st
	noPK.Schema = model.Schema{Columns: []model.Column{{Name: "LABEL", Type: model.TypeString}}}
	q, args = chunkQuery(noPK, state.Chunk{})
	require.Equal(t, `SELECT * FROM "SYSTEM"."ITEMS"`, q)
	require.Nil(t, args)
}

func TestSuggestCursor(t *testing.T) {
	ts := model.Schema{Columns: []model.Column{
		{Name: "ID", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "UPDATED_AT", Type: model.TypeTimestamp},
	}}
	require.Equal(t, "UPDATED_AT", suggestCursor(ts))

	// With BOTH an integer PK and a timestamp column, the timestamp column is
	// preferred as the default cursor — this is the path ReadIncrement must bind
	// as a native date, not a raw RFC3339 string (else ORA-01858/ORA-01861).
	tsOverPK := model.Schema{Columns: []model.Column{
		{Name: "ID", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "PAYLOAD", Type: model.TypeString},
		{Name: "CREATED_AT", Type: model.TypeTimestamp},
	}}
	require.Equal(t, "CREATED_AT", suggestCursor(tsOverPK))

	pkOnly := model.Schema{Columns: []model.Column{{Name: "ID", Type: model.TypeInt64, PrimaryKey: true}}}
	require.Equal(t, "ID", suggestCursor(pkOnly))
}

func TestCursorBind(t *testing.T) {
	stream := model.Stream{Schema: model.Schema{Columns: []model.Column{
		{Name: "ID", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "CREATED_AT", Type: model.TypeTimestamp},
		{Name: "LABEL", Type: model.TypeString},
	}}}

	// Timestamp cursor: RFC3339Nano string parses back to a UTC time.Time so
	// go-ora binds a native Oracle date instead of forcing an NLS string cast.
	got := cursorBind(stream, "CREATED_AT", "2026-07-25T10:30:00.5Z")
	tv, ok := got.(time.Time)
	require.True(t, ok, "timestamp cursor must bind as time.Time, got %T", got)
	require.True(t, tv.Equal(time.Date(2026, 7, 25, 10, 30, 0, 500_000_000, time.UTC)))

	// Numeric cursor stays a string (parsed as int64 in the WHERE by Oracle).
	require.Equal(t, "42", cursorBind(stream, "ID", "42"))

	// Unparseable timestamp value falls back to the raw string, no panic.
	require.Equal(t, "not-a-date", cursorBind(stream, "CREATED_AT", "not-a-date"))
}

func TestSpecCapabilities(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
	require.Equal(t, "oracle", New().Spec().Type)
	require.Equal(t, sdk.MaturityBeta, New().Spec().Maturity)
}

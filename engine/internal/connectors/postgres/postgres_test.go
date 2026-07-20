package postgres

import (
	"math/big"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

func TestCapabilityDeclarationMatchesImplementation(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{name: "valid minimal", cfg: Config{Host: "h", Database: "d", User: "u"}},
		{name: "wrong type", cfg: Config{Type: "mysql", Host: "h", Database: "d", User: "u"}, wantErr: "not \"postgres\""},
		{name: "missing host", cfg: Config{Database: "d", User: "u"}, wantErr: "host"},
		{name: "missing database", cfg: Config{Host: "h", User: "u"}, wantErr: "database"},
		{name: "bad strategy", cfg: Config{Host: "h", Database: "d", User: "u", ChunkStrategy: "zigzag"}, wantErr: "chunk_strategy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				assert.Equal(t, 5432, tt.cfg.Port, "defaults applied")
				assert.Equal(t, "prefer", tt.cfg.SSLMode)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestDSNEscapesCredentials(t *testing.T) {
	cfg := Config{Host: "db.local", Database: "app", User: "u@ser", Password: "p@ss/word"}
	require.NoError(t, cfg.validate())
	dsn := cfg.dsn()
	assert.Contains(t, dsn, "u%40ser")
	assert.Contains(t, dsn, "p%40ss%2Fword")
	assert.Contains(t, dsn, "db.local:5432")
	assert.Contains(t, dsn, "sslmode=prefer")
}

func TestMapType(t *testing.T) {
	tests := []struct {
		pg   string
		want model.DataType
	}{
		{"integer", model.TypeInt32},
		{"bigint", model.TypeInt64},
		{"smallint", model.TypeInt32},
		{"boolean", model.TypeBool},
		{"real", model.TypeFloat32},
		{"double precision", model.TypeFloat64},
		{"numeric(10,2)", model.TypeDecimal},
		{"numeric", model.TypeDecimal},
		{"character varying(255)", model.TypeString},
		{"text", model.TypeString},
		{"uuid", model.TypeString},
		{"date", model.TypeDate},
		{"timestamp without time zone", model.TypeTimestamp},
		{"timestamp with time zone", model.TypeTimestamp},
		{"jsonb", model.TypeJSON},
		{"bytea", model.TypeBinary},
		{"integer[]", model.TypeArray},
		{"text[]", model.TypeArray},
		{"tsvector", model.TypeString},
		{"some_custom_enum", model.TypeString},
	}
	for _, tt := range tests {
		t.Run(tt.pg, func(t *testing.T) {
			assert.Equal(t, tt.want, MapType(tt.pg))
		})
	}
}

func TestNormalizeValue(t *testing.T) {
	ts := time.Date(2026, 7, 19, 10, 0, 0, 0, time.UTC)
	num := pgtype.Numeric{Int: big.NewInt(123456), Exp: -2, Valid: true}
	negNum := pgtype.Numeric{Int: big.NewInt(-5), Exp: -3, Valid: true}
	intNum := pgtype.Numeric{Int: big.NewInt(42), Exp: 2, Valid: true}

	tests := []struct {
		name string
		in   any
		want any
	}{
		{"nil", nil, nil},
		{"int16 widens", int16(3), int32(3)},
		{"int64 passes", int64(9), int64(9)},
		{"time passes", ts, ts},
		{"numeric to decimal string", num, "1234.56"},
		{"negative small numeric", negNum, "-0.005"},
		{"positive exponent numeric", intNum, "4200"},
		{"uuid bytes", [16]byte{0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00}, "550e8400-e29b-41d4-a716-446655440000"},
		{"nested array", []any{int16(1), int16(2)}, []any{int32(1), int32(2)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, NormalizeValue(tt.in))
		})
	}
}

func intPKStream() model.Stream {
	return model.Stream{
		Namespace: "public", Name: "users",
		Schema: model.Schema{Columns: []model.Column{
			{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
			{Name: "updated_at", Type: model.TypeTimestamp},
		}},
	}
}

func TestChunkQuery(t *testing.T) {
	stream := intPKStream()
	tests := []struct {
		name     string
		strategy string
		chunk    state.Chunk
		wantSQL  string
		wantArgs []any
		wantErr  string
	}{
		{
			name: "ctid range", strategy: "ctid",
			chunk:   state.Chunk{Min: "0", Max: "131072"},
			wantSQL: `SELECT * FROM "public"."users" WHERE ctid >= '(0,0)'::tid AND ctid < '(131072,0)'::tid`,
		},
		{
			name: "ctid open", strategy: "ctid",
			chunk:   state.Chunk{},
			wantSQL: `SELECT * FROM "public"."users"`,
		},
		{
			name: "keyset bounded", strategy: "keyset",
			chunk:    state.Chunk{Min: "100", Max: "200"},
			wantSQL:  `SELECT * FROM "public"."users" WHERE "id" >= $1 AND "id" < $2`,
			wantArgs: []any{int64(100), int64(200)},
		},
		{
			name: "keyset leading open", strategy: "keyset",
			chunk:    state.Chunk{Max: "100"},
			wantSQL:  `SELECT * FROM "public"."users" WHERE "id" < $1`,
			wantArgs: []any{int64(100)},
		},
		{
			name: "keyset trailing open", strategy: "keyset",
			chunk:    state.Chunk{Min: "200"},
			wantSQL:  `SELECT * FROM "public"."users" WHERE "id" >= $1`,
			wantArgs: []any{int64(200)},
		},
		{
			name: "keyset bad bound", strategy: "keyset",
			chunk:   state.Chunk{Min: "abc"},
			wantErr: "bad keyset chunk bound",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Connector{cfg: Config{ChunkStrategy: tt.strategy}}
			sql, args, err := c.chunkQuery(stream, tt.chunk)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, sql)
			assert.Equal(t, tt.wantArgs, args)
		})
	}
}

func TestCursorRoundTrip(t *testing.T) {
	ts := time.Date(2026, 7, 19, 10, 30, 0, 123456000, time.UTC)
	tests := []struct {
		name string
		typ  model.DataType
		val  any
	}{
		{"timestamp", model.TypeTimestamp, ts},
		{"int64", model.TypeInt64, int64(42)},
		{"int32", model.TypeInt32, int32(7)},
		{"string", model.TypeString, "abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := formatCursor(tt.typ, tt.val)
			require.NoError(t, err)
			back, err := parseCursor(tt.typ, s)
			require.NoError(t, err)
			switch want := tt.val.(type) {
			case time.Time:
				gotTime, ok := back.(time.Time)
				require.True(t, ok)
				assert.True(t, want.Equal(gotTime))
			case int32:
				assert.Equal(t, int64(want), back)
			default:
				assert.Equal(t, tt.val, back)
			}
		})
	}

	_, err := formatCursor(model.TypeJSON, "{}")
	require.Error(t, err, "json cannot be a cursor")
}

func TestSuggestCursor(t *testing.T) {
	assert.Equal(t, "updated_at", suggestCursor(model.Schema{Columns: []model.Column{
		{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
		{Name: "updated_at", Type: model.TypeTimestamp},
	}}))
	assert.Equal(t, "id", suggestCursor(model.Schema{Columns: []model.Column{
		{Name: "id", Type: model.TypeInt64, PrimaryKey: true},
	}}))
	assert.Equal(t, "", suggestCursor(model.Schema{Columns: []model.Column{
		{Name: "name", Type: model.TypeString},
	}}))
}

package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// seedDB writes a fresh SQLite file with a widgets table and n rows, returning
// its path. Rows have id (INTEGER PK), name (TEXT), price (REAL), updated_at
// (TEXT, ISO-8601).
func seedDB(t *testing.T, n int) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "demo.db")
	db, err := sql.Open("sqlite", "file:"+path)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	_, err = db.Exec(`CREATE TABLE widgets (
		id INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		price REAL,
		updated_at TEXT
	)`)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		_, err := db.Exec(`INSERT INTO widgets(id, name, price, updated_at) VALUES(?,?,?,?)`,
			i, fmt.Sprintf("w%d", i), float64(i)+0.5, fmt.Sprintf("2026-01-%02dT00:00:00Z", (i%28)+1))
		require.NoError(t, err)
	}
	return path
}

func setup(t *testing.T, path string) *Connector {
	t.Helper()
	c, ok := New().(*Connector)
	require.True(t, ok)
	raw, _ := json.Marshal(Config{Type: "sqlite", Path: path, ChunkRows: 3})
	require.NoError(t, c.Setup(context.Background(), raw))
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestCapabilityDeclarationMatchesImplementation(t *testing.T) {
	require.NoError(t, sdk.ValidateCapabilities(New()))
}

func TestCheckAndDiscover(t *testing.T) {
	c := setup(t, seedDB(t, 5))
	require.NoError(t, c.Check(context.Background()))

	streams, err := c.Discover(context.Background())
	require.NoError(t, err)
	require.Len(t, streams, 1)
	s := streams[0]
	assert.Equal(t, "main", s.Namespace)
	assert.Equal(t, "widgets", s.Name)
	assert.Equal(t, []model.SyncMode{model.ModeFullLoad, model.ModeIncremental}, s.SupportedSyncModes)

	byName := map[string]model.Column{}
	for _, col := range s.Schema.Columns {
		byName[col.Name] = col
	}
	assert.Equal(t, model.TypeInt64, byName["id"].Type)
	assert.True(t, byName["id"].PrimaryKey)
	assert.Equal(t, model.TypeString, byName["name"].Type)
	assert.False(t, byName["name"].Nullable)
	assert.Equal(t, model.TypeFloat64, byName["price"].Type)
	// updated_at is declared TEXT (not a timestamp type), so suggestCursor
	// falls through to the single integer PK.
	assert.Equal(t, "id", s.DefaultCursorField)
}

func TestFullLoadReadsEveryRowAcrossChunks(t *testing.T) {
	c := setup(t, seedDB(t, 10)) // ChunkRows=3 → multiple chunks
	streams, err := c.Discover(context.Background())
	require.NoError(t, err)
	stream := streams[0]

	chunks, err := c.SplitChunks(context.Background(), stream)
	require.NoError(t, err)
	require.Greater(t, len(chunks), 1, "10 rows at chunk_rows=3 must split into several chunks")

	seen := map[int64]int{}
	for _, ch := range chunks {
		err := c.ReadChunk(context.Background(), stream, ch, func(_ context.Context, row sdk.Row) error {
			id, _ := row["id"].(int64)
			seen[id]++
			assert.IsType(t, "", row["name"], "TEXT columns arrive as strings, not []byte")
			return nil
		})
		require.NoError(t, err)
	}
	require.Len(t, seen, 10, "every row read exactly once across chunk ranges")
	for id, n := range seen {
		assert.Equal(t, 1, n, "row %d read exactly once", id)
	}
}

func TestIncrementalReadsOnlyPastTheCursor(t *testing.T) {
	c := setup(t, seedDB(t, 6))
	streams, err := c.Discover(context.Background())
	require.NoError(t, err)
	stream := streams[0]

	// Cursor on the integer PK.
	max, err := c.MaxCursor(context.Background(), stream, "id")
	require.NoError(t, err)
	assert.Equal(t, "6", max)

	var got []int64
	newCur, err := c.ReadIncrement(context.Background(), stream, "id", "4", func(_ context.Context, row sdk.Row) error {
		id, _ := row["id"].(int64)
		got = append(got, id)
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []int64{5, 6}, got, "only rows with id > 4")
	assert.Equal(t, "6", newCur)
}

func TestChunkQueryBounds(t *testing.T) {
	stream := model.Stream{Namespace: "main", Name: "widgets"}
	tests := []struct {
		name       string
		chunk      state.Chunk
		wantSQL    string
		wantArgLen int
	}{
		{"open-open", state.Chunk{}, `SELECT * FROM "widgets"`, 0},
		{"open-max", state.Chunk{Max: "5"}, `SELECT * FROM "widgets" WHERE rowid < ?`, 1},
		{"min-open", state.Chunk{Min: "5"}, `SELECT * FROM "widgets" WHERE rowid >= ?`, 1},
		{"min-max", state.Chunk{Min: "5", Max: "9"}, `SELECT * FROM "widgets" WHERE rowid >= ? AND rowid < ?`, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, args, err := chunkQuery(stream, tt.chunk)
			require.NoError(t, err)
			assert.Equal(t, tt.wantSQL, q)
			assert.Len(t, args, tt.wantArgLen)
		})
	}
}

func TestMapTypeAffinity(t *testing.T) {
	cases := map[string]model.DataType{
		"INTEGER": model.TypeInt64, "BIGINT": model.TypeInt64,
		"TEXT": model.TypeString, "VARCHAR(10)": model.TypeString,
		"REAL": model.TypeFloat64, "DOUBLE": model.TypeFloat64,
		"BLOB": model.TypeBinary, "": model.TypeString,
		"BOOLEAN": model.TypeBool, "DECIMAL(10,2)": model.TypeDecimal,
		"DATE": model.TypeDate, "DATETIME": model.TypeTimestamp, "JSON": model.TypeJSON,
	}
	for declared, want := range cases {
		assert.Equalf(t, want, mapType(declared), "mapType(%q)", declared)
	}
}

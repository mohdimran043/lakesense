package mysql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/connectors/mysql"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// mysqlIT connects to a throwaway MySQL. Set LAKESENSE_MYSQL_IT to the config
// JSON (e.g. {"type":"mysql","host":"127.0.0.1","port":43306,"database":"shop",
// "user":"root","password":"pw"}) to run; otherwise the test skips.
func mysqlConfig(t *testing.T) json.RawMessage {
	t.Helper()
	cfg := os.Getenv("LAKESENSE_MYSQL_IT")
	if cfg == "" {
		t.Skip("set LAKESENSE_MYSQL_IT to a MySQL config JSON to run the connector integration test")
	}
	return json.RawMessage(cfg)
}

func TestMySQLFullLoadAndIncremental(t *testing.T) {
	ctx := context.Background()
	raw := mysqlConfig(t)

	// Fresh table via a plain driver connection derived from the config.
	var probe struct {
		Host, Database, User, Password string
		Port                           int
	}
	require.NoError(t, json.Unmarshal(raw, &probe))
	db, err := sql.Open("mysql", probe.User+":"+probe.Password+"@tcp("+probe.Host+":"+itoa(probe.Port)+")/"+probe.Database)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS it_orders")
	_, err = db.ExecContext(ctx, `CREATE TABLE it_orders(id INT PRIMARY KEY AUTO_INCREMENT, v VARCHAR(32) NOT NULL)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO it_orders(v) VALUES ('a'),('b'),('c')`)
	require.NoError(t, err)

	c := mysql.New()
	require.NoError(t, c.Setup(ctx, raw))
	defer func() { _ = c.Close(ctx) }()
	require.NoError(t, c.Check(ctx))

	streams, err := c.Discover(ctx)
	require.NoError(t, err)
	stream, ok := findStream(streams, "it_orders")
	require.True(t, ok, "discovered it_orders")
	require.Equal(t, []string{"id"}, stream.Schema.PrimaryKey())

	// Full load: chunk plan + read every chunk.
	fl, ok := c.(sdk.FullLoader)
	require.True(t, ok)
	chunks, err := fl.SplitChunks(ctx, stream)
	require.NoError(t, err)
	var full []sdk.Row
	for _, ch := range chunks {
		require.NoError(t, fl.ReadChunk(ctx, stream, ch, func(_ context.Context, r sdk.Row) error {
			full = append(full, r)
			return nil
		}))
	}
	require.Len(t, full, 3, "full load returns all rows exactly once")

	// Incremental on id: first read all, then only rows past the cursor.
	ir, ok := c.(sdk.IncrementalReader)
	require.True(t, ok)
	var first []sdk.Row
	cursor, err := ir.ReadIncrement(ctx, stream, "id", "", func(_ context.Context, r sdk.Row) error {
		first = append(first, r)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, first, 3)

	_, err = db.ExecContext(ctx, `INSERT INTO it_orders(v) VALUES ('d'),('e')`)
	require.NoError(t, err)

	var second []sdk.Row
	_, err = ir.ReadIncrement(ctx, stream, "id", cursor, func(_ context.Context, r sdk.Row) error {
		second = append(second, r)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, second, 2, "incremental returns only rows past the cursor")
}

func findStream(streams []model.Stream, name string) (model.Stream, bool) {
	for _, s := range streams {
		if s.Name == name {
			return s, true
		}
	}
	return model.Stream{}, false
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	if neg {
		b = append([]byte{'-'}, b...)
	}
	return string(b)
}

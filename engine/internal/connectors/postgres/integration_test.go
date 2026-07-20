package postgres

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Integration tests run only when LAKESENSE_PG_IT=1, against a PostgreSQL
// with wal_level=logical. Connection defaults target the throwaway container
// documented in deploy/test-compose.yml; override via PG* env vars.
//
//	docker run -d --name pg -e POSTGRES_PASSWORD=lakesense -e POSTGRES_DB=cdctest \
//	  -p 15433:5432 postgres:16-alpine \
//	  -c wal_level=logical -c max_replication_slots=8 -c max_wal_senders=8
//	LAKESENSE_PG_IT=1 go test ./internal/connectors/postgres/ -run Integration

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func itConfig() Config {
	port := 15433
	if p := os.Getenv("PGPORT"); p != "" {
		if n, err := strconv.Atoi(p); err == nil {
			port = n
		}
	}
	return Config{
		Host:     envOr("PGHOST", "localhost"),
		Port:     port,
		Database: envOr("PGDATABASE", "cdctest"),
		User:     envOr("PGUSER", "postgres"),
		Password: envOr("PGPASSWORD", "lakesense"),
		SSLMode:  "disable",
	}
}

// itConnector builds and sets up a connector, registering cleanup.
func itConnector(t *testing.T, cfg Config) *Connector {
	t.Helper()
	c, ok := New().(*Connector)
	require.True(t, ok)
	raw, err := json.Marshal(cfg)
	require.NoError(t, err)
	require.NoError(t, c.Setup(context.Background(), raw))
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func requireIT(t *testing.T) {
	t.Helper()
	if os.Getenv("LAKESENSE_PG_IT") != "1" {
		t.Skip("set LAKESENSE_PG_IT=1 to run PostgreSQL integration tests")
	}
}

// setupTable drops and recreates a uniquely-named table so parallel runs and
// re-runs stay isolated, and cleans up its slot/publication afterward.
func setupTable(t *testing.T, c *Connector, ddl, table string) {
	t.Helper()
	ctx := context.Background()
	_, err := c.pool.Exec(ctx, "DROP TABLE IF EXISTS "+table+" CASCADE")
	require.NoError(t, err)
	_, err = c.pool.Exec(ctx, ddl)
	require.NoError(t, err)
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = c.pool.Exec(bg, "DROP TABLE IF EXISTS "+table+" CASCADE")
		_, _ = c.pool.Exec(bg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1", c.cdcSlotName())
		_, _ = c.pool.Exec(bg, "DROP PUBLICATION IF EXISTS "+c.cdcPublicationName())
	})
}

func TestIntegrationCheckAndDiscover(t *testing.T) {
	requireIT(t)
	c := itConnector(t, itConfig())
	ctx := context.Background()

	require.NoError(t, c.Check(ctx))

	setupTable(t, c, `CREATE TABLE public.disc (id bigint PRIMARY KEY, name text, updated_at timestamptz)`, "public.disc")
	streams, err := c.Discover(ctx)
	require.NoError(t, err)

	var found *model.Stream
	for i := range streams {
		if streams[i].ID() == "public.disc" {
			found = &streams[i]
		}
	}
	require.NotNil(t, found, "discover must find the created table")
	assert.Equal(t, []string{"id"}, found.Schema.PrimaryKey())
	assert.Equal(t, "updated_at", found.DefaultCursorField)
	idCol, _ := found.Schema.Column("id")
	assert.Equal(t, model.TypeInt64, idCol.Type)
}

func TestIntegrationFullLoadCTID(t *testing.T) {
	requireIT(t)
	c := itConnector(t, itConfig())
	ctx := context.Background()
	setupTable(t, c, `CREATE TABLE public.load (id bigint PRIMARY KEY, name text)`, "public.load")

	const n = 5000
	_, err := c.pool.Exec(ctx, `INSERT INTO public.load SELECT g, 'row-'||g FROM generate_series(1,$1) g`, n)
	require.NoError(t, err)

	stream := mustStream(t, c, "public.load")
	chunks, err := c.SplitChunks(ctx, stream)
	require.NoError(t, err)
	require.NotEmpty(t, chunks)

	seen := map[int64]bool{}
	for _, chunk := range chunks {
		require.NoError(t, c.ReadChunk(ctx, stream, chunk, func(_ context.Context, row sdk.Row) error {
			id, ok := row["id"].(int64)
			require.True(t, ok, "id should be int64, got %T", row["id"])
			assert.False(t, seen[id], "row %d read twice across chunks", id)
			seen[id] = true
			return nil
		}))
	}
	assert.Len(t, seen, n, "every row read exactly once across all CTID chunks")
}

func TestIntegrationIncremental(t *testing.T) {
	requireIT(t)
	c := itConnector(t, itConfig())
	ctx := context.Background()
	setupTable(t, c, `CREATE TABLE public.inc (id bigint PRIMARY KEY, name text)`, "public.inc")
	stream := mustStream(t, c, "public.inc")

	_, err := c.pool.Exec(ctx, `INSERT INTO public.inc VALUES (1,'a'),(2,'b'),(3,'c')`)
	require.NoError(t, err)

	var count int
	cursor, err := c.ReadIncrement(ctx, stream, "id", "", func(context.Context, sdk.Row) error { count++; return nil })
	require.NoError(t, err)
	assert.Equal(t, 3, count)
	assert.Equal(t, "3", cursor)

	_, err = c.pool.Exec(ctx, `INSERT INTO public.inc VALUES (4,'d'),(5,'e')`)
	require.NoError(t, err)

	count = 0
	cursor, err = c.ReadIncrement(ctx, stream, "id", cursor, func(context.Context, sdk.Row) error { count++; return nil })
	require.NoError(t, err)
	assert.Equal(t, 2, count, "only rows past the previous cursor are re-read")
	assert.Equal(t, "5", cursor)
}

// TestIntegrationCDCEndToEnd is the credibility test: anchor CDC before
// mutations, apply insert/update/delete, and prove the decoded change stream
// matches — including the ack-before-state slot advance.
func TestIntegrationCDCEndToEnd(t *testing.T) {
	requireIT(t)
	c := itConnector(t, itConfig())
	ctx := context.Background()
	setupTable(t, c, `CREATE TABLE public.cdc (id bigint PRIMARY KEY, name text)`, "public.cdc")
	// REPLICA IDENTITY FULL so deletes/updates carry identity columns.
	_, err := c.pool.Exec(ctx, `ALTER TABLE public.cdc REPLICA IDENTITY FULL`)
	require.NoError(t, err)

	stream := mustStream(t, c, "public.cdc")

	// Anchor BEFORE any change — the DBLog discipline.
	pos, err := c.PrepareCDC(ctx, []model.Stream{stream})
	require.NoError(t, err)
	require.NotEmpty(t, pos["lsn"])

	// Mutations after the anchor.
	_, err = c.pool.Exec(ctx, `INSERT INTO public.cdc VALUES (1,'alice'),(2,'bob')`)
	require.NoError(t, err)
	_, err = c.pool.Exec(ctx, `UPDATE public.cdc SET name='robert' WHERE id=2`)
	require.NoError(t, err)
	_, err = c.pool.Exec(ctx, `DELETE FROM public.cdc WHERE id=1`)
	require.NoError(t, err)

	var changes []sdk.Change
	final, err := c.StreamChanges(ctx, []model.Stream{stream}, pos, func(_ context.Context, ch sdk.Change) error {
		changes = append(changes, ch)
		return nil
	})
	require.NoError(t, err)

	require.Len(t, changes, 4, "2 inserts + 1 update + 1 delete")
	assert.Equal(t, sdk.ChangeInsert, changes[0].Kind)
	assert.Equal(t, int32(1), toInt(changes[0].Data["id"]))
	assert.Equal(t, "alice", changes[0].Data["name"])
	assert.Equal(t, sdk.ChangeInsert, changes[1].Kind)
	assert.Equal(t, "bob", changes[1].Data["name"])
	assert.Equal(t, sdk.ChangeUpdate, changes[2].Kind)
	assert.Equal(t, "robert", changes[2].Data["name"])
	assert.Equal(t, sdk.ChangeDelete, changes[3].Kind)
	assert.Equal(t, int32(1), toInt(changes[3].Data["id"]), "delete carries identity columns under REPLICA IDENTITY FULL")

	require.NotEmpty(t, final["lsn"])

	// A second bounded run with no new changes returns promptly and does not
	// duplicate — the slot advanced past the consumed changes.
	changes = nil
	_, err = c.StreamChanges(ctx, []model.Stream{stream}, final, func(_ context.Context, ch sdk.Change) error {
		changes = append(changes, ch)
		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, changes, "no changes replayed after the slot advanced")
}

// TestIntegrationCDCResumeAfterAnchor proves a fresh StreamChanges from the
// stored anchor replays only post-anchor changes (resume correctness).
func TestIntegrationCDCResumeContinues(t *testing.T) {
	requireIT(t)
	c := itConnector(t, itConfig())
	ctx := context.Background()
	setupTable(t, c, `CREATE TABLE public.cdcr (id bigint PRIMARY KEY, n int)`, "public.cdcr")
	stream := mustStream(t, c, "public.cdcr")

	pos, err := c.PrepareCDC(ctx, []model.Stream{stream})
	require.NoError(t, err)

	_, err = c.pool.Exec(ctx, `INSERT INTO public.cdcr VALUES (1,10)`)
	require.NoError(t, err)

	var batch1 []sdk.Change
	mid, err := c.StreamChanges(ctx, []model.Stream{stream}, pos, collect(&batch1))
	require.NoError(t, err)
	require.Len(t, batch1, 1)

	_, err = c.pool.Exec(ctx, `INSERT INTO public.cdcr VALUES (2,20),(3,30)`)
	require.NoError(t, err)

	var batch2 []sdk.Change
	_, err = c.StreamChanges(ctx, []model.Stream{stream}, mid, collect(&batch2))
	require.NoError(t, err)
	require.Len(t, batch2, 2, "resume from mid position replays only the new inserts")
	assert.Equal(t, int32(2), toInt(batch2[0].Data["id"]))
	assert.Equal(t, int32(3), toInt(batch2[1].Data["id"]))
}

func collect(dst *[]sdk.Change) sdk.ChangeFunc {
	return func(_ context.Context, ch sdk.Change) error { *dst = append(*dst, ch); return nil }
}

func mustStream(t *testing.T, c *Connector, id string) model.Stream {
	t.Helper()
	// short retry: table stats may lag right after creation
	deadline := time.Now().Add(2 * time.Second)
	for {
		streams, err := c.Discover(context.Background())
		require.NoError(t, err)
		for _, s := range streams {
			if s.ID() == id {
				return s
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %s not discovered", id)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func toInt(v any) int32 {
	switch n := v.(type) {
	case int32:
		return n
	case int64:
		return int32(n)
	default:
		return -1
	}
}

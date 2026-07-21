package mongodb_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/engine/internal/connectors/mongodb"
	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// Set LAKESENSE_MONGO_IT to a MongoDB config JSON (replica set, e.g.
// {"type":"mongodb","host":"127.0.0.1","port":47017,"database":"itdb"}) to run.
func mongoConfig(t *testing.T) []byte {
	t.Helper()
	cfg := os.Getenv("LAKESENSE_MONGO_IT")
	if cfg == "" {
		t.Skip("set LAKESENSE_MONGO_IT to a MongoDB config JSON to run the connector integration test")
	}
	return []byte(cfg)
}

func TestMongoFullIncrementalCDC(t *testing.T) {
	ctx := context.Background()
	raw := mongoConfig(t)

	c := mongodb.New()
	require.NoError(t, c.Setup(ctx, raw))
	defer func() { _ = c.Close(ctx) }()
	require.NoError(t, c.Check(ctx))

	// Seed a fresh collection through the connector's own client would require an
	// exported handle; instead drive everything through the SDK against a
	// collection the caller pre-seeds. Discover picks up whatever exists.
	streams, err := c.Discover(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, streams, "the itdb database must contain at least one seeded collection")

	// Full load returns rows for the first collection.
	stream := streams[0]
	fl, ok := c.(sdk.FullLoader)
	require.True(t, ok)
	chunks, err := fl.SplitChunks(ctx, stream)
	require.NoError(t, err)
	var n int
	for _, ch := range chunks {
		require.NoError(t, fl.ReadChunk(ctx, stream, ch, func(context.Context, sdk.Row) error {
			n++
			return nil
		}))
	}
	require.Positive(t, n, "full load returns the seeded rows")

	// CDC: anchor then replay whatever changes the caller makes; here we assert
	// the change-stream machinery round-trips a resume token without error.
	cs, ok := c.(sdk.ChangeStreamer)
	require.True(t, ok)
	pos, err := cs.PrepareCDC(ctx, []model.Stream{stream})
	require.NoError(t, err)
	final, err := cs.StreamChanges(ctx, []model.Stream{stream}, pos, func(context.Context, sdk.Change) error { return nil })
	require.NoError(t, err)
	require.NotNil(t, final)
}

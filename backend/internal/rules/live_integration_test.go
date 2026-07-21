package rules_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/collector"
	"github.com/lakesense/lakesense/backend/internal/rules"
	"github.com/lakesense/lakesense/backend/internal/store"
)

// liveTestPool connects to LAKESENSE_TEST_DB, migrates, and truncates, or skips.
func liveTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LAKESENSE_TEST_DB")
	if dsn == "" {
		t.Skip("set LAKESENSE_TEST_DB to run the live rules integration test")
	}
	require.NoError(t, store.Migrate(dsn))
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(context.Background(),
		`TRUNCATE pipelines, environments, rules, incidents, alerts, events, channels RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	return pool
}

// TestLiveEventOpensIncidentAndDedups proves the wired path: an ingested
// sync_failed event matches a rule, opens exactly one incident, and a second
// identical event dedups into that same incident (one incident = one thread).
func TestLiveEventOpensIncidentAndDedups(t *testing.T) {
	pool := liveTestPool(t)
	ctx := context.Background()

	// Minimal fixtures: an environment, a pipeline, and a rule on sync_failed.
	var envID, pipeID int64
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO environments (name, slug, kind) VALUES ('Dev','dev','dev') RETURNING id`).Scan(&envID))
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO pipelines (environment_id, name, slug, source_type, destination_type, status, current_version)
		 VALUES ($1,'P','p','postgres','parquet','active',1) RETURNING id`, envID).Scan(&pipeID))
	_, err := pool.Exec(ctx,
		`INSERT INTO rules (pipeline_id, name, condition, severity, channel_ids, enabled)
		 VALUES ($1, 'fail alert', '{"event":"sync_failed"}', 'critical', '{}', true)`, pipeID)
	require.NoError(t, err)

	// Wire the live processor exactly as serve does (no channels → incident only).
	engine := rules.NewEngine(rules.NewPgStore(pool), nil, nil)
	loader := rules.NewPgLoader(pool)
	process := func(ctx context.Context, pipelineID int64, e collector.Event) {
		rs, lerr := loader.LoadRules(ctx, pipelineID)
		require.NoError(t, lerr)
		require.NoError(t, engine.Evaluate(ctx, pipelineID, e, rs))
	}
	ing := collector.NewIngester(collector.NewPgSink(pool), collector.WithProcessor(process))

	stream := `{"v":1,"event":"sync_failed","sync_id":"s1","stream":"public.orders","payload":{"code":"boom"}}` + "\n"

	// First failure opens an incident.
	_, err = ing.Ingest(ctx, pipeID, strings.NewReader(stream))
	require.NoError(t, err)
	var count, eventCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM incidents WHERE pipeline_id=$1`, pipeID).Scan(&count))
	require.Equal(t, 1, count, "one incident opened")

	// Second identical failure dedups into the same incident, bumping its count.
	_, err = ing.Ingest(ctx, pipeID, strings.NewReader(stream))
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM incidents WHERE pipeline_id=$1`, pipeID).Scan(&count))
	require.Equal(t, 1, count, "still one incident (deduped)")
	require.NoError(t, pool.QueryRow(ctx, `SELECT event_count FROM incidents WHERE pipeline_id=$1`, pipeID).Scan(&eventCount))
	require.Equal(t, 2, eventCount, "incident event_count bumped by the dedup")
}

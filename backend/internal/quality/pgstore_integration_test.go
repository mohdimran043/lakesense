package quality_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/quality"
	"github.com/lakesense/lakesense/backend/internal/store"
)

func qTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LAKESENSE_TEST_DB")
	if dsn == "" {
		t.Skip("set LAKESENSE_TEST_DB to run the quality integration test")
	}
	require.NoError(t, store.Migrate(dsn))
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(context.Background(),
		`TRUNCATE pipelines, environments, column_stats, quality_monitors, quality_results RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	return pool
}

// TestQualityWorkerTripsNullRate proves the full evaluation path over real
// Postgres: a persisted column_stats with a high null rate trips a null-rate
// monitor whose baseline is low → a result is recorded and a breach is emitted.
func TestQualityWorkerTripsNullRate(t *testing.T) {
	pool := qTestPool(t)
	ctx := context.Background()

	var envID, pipeID int64
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO environments (name, slug, kind) VALUES ('Dev','dev','dev') RETURNING id`).Scan(&envID))
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO pipelines (environment_id, name, slug, source_type, destination_type, status, current_version)
		 VALUES ($1,'P','p','postgres','parquet','active',1) RETURNING id`, envID).Scan(&pipeID))

	// A column that has started coming back mostly null.
	_, err := pool.Exec(ctx,
		`INSERT INTO column_stats (pipeline_id, stream, column_name, sync_id, ts, row_count, null_count)
		 VALUES ($1,'public.users','email','s1', now(), 1000, 600)`, pipeID) // 60% null
	require.NoError(t, err)

	// A monitor whose baseline null rate is 2% and tolerates +10 points.
	_, err = pool.Exec(ctx,
		`INSERT INTO quality_monitors (pipeline_id, stream, column_name, kind, config, baseline, enabled)
		 VALUES ($1,'public.users','email','null_rate','{"max_increase":0.1}','{"null_rate":0.02}', true)`, pipeID)
	require.NoError(t, err)

	var breaches int
	emit := func(context.Context, int64, quality.Monitor, quality.Result) error {
		breaches++
		return nil
	}
	w := quality.NewWorker(quality.NewPgStore(pool), emit, nil)
	require.NoError(t, w.Tick(ctx))

	require.Equal(t, 1, breaches, "the null-rate spike alerted")

	var recorded, breached int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE breached) FROM quality_results`).Scan(&recorded, &breached))
	require.Equal(t, 1, recorded, "the evaluation was recorded")
	require.Equal(t, 1, breached, "and marked breached")
}

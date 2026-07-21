package pipelines

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/audit"
	"github.com/lakesense/lakesense/backend/internal/store"
)

// testPool connects to LAKESENSE_TEST_DB and migrates it, or skips.
func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LAKESENSE_TEST_DB")
	if dsn == "" {
		t.Skip("set LAKESENSE_TEST_DB to a throwaway Postgres DSN to run pgRepo integration tests")
	}
	require.NoError(t, store.Migrate(dsn))
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	// Clean slate for repeatable runs.
	_, err = pool.Exec(context.Background(), `TRUNCATE pipelines, environments, pipeline_config_versions, audit_log RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	return pool
}

func TestPgCreatePersistsAtomically(t *testing.T) {
	pool := testPool(t)
	svc := NewService(NewPgRepo(pool), audit.NewPgRecorder(pool), fixedNow)

	p, err := svc.Create(context.Background(), "alice", sampleReq())
	require.NoError(t, err)
	require.NotZero(t, p.ID)

	var versions, audits int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pipeline_config_versions WHERE pipeline_id=$1`, p.ID).Scan(&versions))
	require.Equal(t, 1, versions)
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM audit_log WHERE entity_type='pipeline' AND action='pipeline.create'`).Scan(&audits))
	require.Equal(t, 1, audits)
}

func TestPgDuplicateSlugRollsBack(t *testing.T) {
	pool := testPool(t)
	svc := NewService(NewPgRepo(pool), audit.NewPgRecorder(pool), fixedNow)
	_, err := svc.Create(context.Background(), "alice", sampleReq())
	require.NoError(t, err)

	// Same name in the same env → duplicate (environment_id, slug) → error.
	_, err = svc.Create(context.Background(), "alice", sampleReq())
	require.Error(t, err)

	var versions int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM pipeline_config_versions`).Scan(&versions))
	require.Equal(t, 1, versions, "the failed create left no orphan version row")
}

func TestPgUpdateAndHistory(t *testing.T) {
	pool := testPool(t)
	svc := NewService(NewPgRepo(pool), audit.NewPgRecorder(pool), fixedNow)
	p, _ := svc.Create(context.Background(), "alice", sampleReq())
	changed := sampleReq()
	changed.Schedule = "@hourly"
	p2, err := svc.Update(context.Background(), "bob", p.ID, changed)
	require.NoError(t, err)
	require.Equal(t, 2, p2.CurrentVersion)

	var maxV int
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT max(version) FROM pipeline_config_versions WHERE pipeline_id=$1`, p.ID).Scan(&maxV))
	require.Equal(t, 2, maxV)
}

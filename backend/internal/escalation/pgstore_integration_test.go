package escalation_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/escalation"
	"github.com/lakesense/lakesense/backend/internal/rules"
	"github.com/lakesense/lakesense/backend/internal/store"
)

// recordNotifier captures escalation notifications.
type recordNotifier struct{ sent []rules.Notification }

func (n *recordNotifier) Send(_ context.Context, note rules.Notification) error {
	n.sent = append(n.sent, note)
	return nil
}

func escTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("LAKESENSE_TEST_DB")
	if dsn == "" {
		t.Skip("set LAKESENSE_TEST_DB to run the escalation integration test")
	}
	require.NoError(t, store.Migrate(dsn))
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(context.Background(),
		`TRUNCATE incidents, escalation_policies, oncall_schedules, channels RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	return pool
}

// TestEscalationAdvancesDueIncident proves a due, unacked incident is escalated
// (its step advances) and, being a single-step policy, is then exhausted
// (next_escalation_at cleared so it stops recurring).
func TestEscalationAdvancesDueIncident(t *testing.T) {
	pool := escTestPool(t)
	ctx := context.Background()

	var policyID int64
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO escalation_policies (name, steps)
		 VALUES ('p', '[{"after_seconds":0,"channel_ids":[]}]') RETURNING id`).Scan(&policyID))

	// An open incident already due for escalation.
	past := time.Now().Add(-time.Minute)
	var incID int64
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO incidents (title, severity, status, fingerprint, escalation_policy_id, escalation_step, next_escalation_at)
		 VALUES ('boom','critical','open','fp', $1, 0, $2) RETURNING id`, policyID, past).Scan(&incID))

	w := escalation.NewWorker(escalation.NewPgStore(pool), escalation.NewPgPolicies(pool),
		escalation.NewPgSchedules(pool), &recordNotifier{}, nil)
	require.NoError(t, w.Tick(ctx))

	var step int
	var nextAt *time.Time
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT escalation_step, next_escalation_at FROM incidents WHERE id=$1`, incID).Scan(&step, &nextAt))
	require.Equal(t, 1, step, "escalation advanced one step")
	require.Nil(t, nextAt, "single-step policy is exhausted → no further escalation")
}

// TestCreateIncidentSetsNextEscalation proves the rules store schedules the first
// escalation when an incident opens with a policy.
func TestCreateIncidentSetsNextEscalation(t *testing.T) {
	pool := escTestPool(t)
	ctx := context.Background()
	var policyID int64
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO escalation_policies (name, steps)
		 VALUES ('p', '[{"after_seconds":120,"channel_ids":[]}]') RETURNING id`).Scan(&policyID))

	rs := rules.NewPgStore(pool)
	opened := time.Now().UTC().Truncate(time.Second)
	id, err := rs.CreateIncident(ctx, &rules.Incident{
		Title: "t", Severity: rules.SeverityCritical, Status: "open",
		Fingerprint: "fp2", EventCount: 1, PolicyID: policyID, OpenedAt: opened,
	})
	require.NoError(t, err)

	var nextAt *time.Time
	require.NoError(t, pool.QueryRow(ctx, `SELECT next_escalation_at FROM incidents WHERE id=$1`, id).Scan(&nextAt))
	require.NotNil(t, nextAt)
	require.WithinDuration(t, opened.Add(120*time.Second), *nextAt, time.Second)
}

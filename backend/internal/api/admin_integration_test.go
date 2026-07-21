package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/lakesense/lakesense/backend/internal/store"
)

func adminTestHandler(t *testing.T) (http.Handler, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("LAKESENSE_TEST_DB")
	if dsn == "" {
		t.Skip("set LAKESENSE_TEST_DB to run the admin API integration test")
	}
	require.NoError(t, store.Migrate(dsn))
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	_, err = pool.Exec(context.Background(),
		`TRUNCATE pipelines, environments, rules, channels, incidents, acks, audit_log RESTART IDENTITY CASCADE`)
	require.NoError(t, err)
	return New(pool, slog.New(slog.NewTextHandler(os.Stderr, nil)), nil), pool
}

func do(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("X-Actor", "tester")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAdminChannelAndRuleCRUD(t *testing.T) {
	h, pool := adminTestHandler(t)

	// Create a channel.
	rec := do(t, h, http.MethodPost, "/api/v1/channels", map[string]any{
		"name": "ops-slack", "type": "slack", "config": map[string]string{"webhook_url": "https://example/x"},
	})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	// Create a rule referencing it.
	rec = do(t, h, http.MethodPost, "/api/v1/rules", map[string]any{
		"name": "on-fail", "condition": map[string]any{"event": "sync_failed"},
		"severity": "critical", "channel_ids": []int64{1},
	})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var ruleCount, auditCount int
	require.NoError(t, pool.QueryRow(context.Background(), `SELECT count(*) FROM rules`).Scan(&ruleCount))
	require.Equal(t, 1, ruleCount)
	require.NoError(t, pool.QueryRow(context.Background(),
		`SELECT count(*) FROM audit_log WHERE action IN ('rule.create','channel.create')`).Scan(&auditCount))
	require.Equal(t, 2, auditCount, "both mutations were audited")

	// Delete the rule.
	rec = do(t, h, http.MethodDelete, "/api/v1/rules/1", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)
}

func TestAdminIncidentAck(t *testing.T) {
	h, pool := adminTestHandler(t)
	ctx := context.Background()
	// Seed an open incident.
	var incID int64
	require.NoError(t, pool.QueryRow(ctx,
		`INSERT INTO incidents (title, severity, status, fingerprint)
		 VALUES ('boom','critical','open','fp1') RETURNING id`).Scan(&incID))

	rec := do(t, h, http.MethodPost, "/api/v1/incidents/1/ack", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var status, ackedBy string
	require.NoError(t, pool.QueryRow(ctx, `SELECT status, coalesce(acked_by,'') FROM incidents WHERE id=$1`, incID).Scan(&status, &ackedBy))
	require.Equal(t, "acked", status)
	require.Equal(t, "tester", ackedBy)

	var ackCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM acks WHERE incident_id=$1 AND action='ack'`, incID).Scan(&ackCount))
	require.Equal(t, 1, ackCount)

	// Acking an already-acked incident → 404 (not in a valid state).
	rec = do(t, h, http.MethodPost, "/api/v1/incidents/1/ack", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// An acked incident can still be resolved (regression guard: resolve binds
	// only $1, so passing an actor arg would 404).
	rec = do(t, h, http.MethodPost, "/api/v1/incidents/1/resolve", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NoError(t, pool.QueryRow(ctx, `SELECT status FROM incidents WHERE id=$1`, incID).Scan(&status))
	require.Equal(t, "resolved", status)
}

package quality

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PgStore backs the quality worker with Postgres.
type PgStore struct{ pool *pgxpool.Pool }

// NewPgStore builds a Postgres-backed quality Store.
func NewPgStore(pool *pgxpool.Pool) *PgStore { return &PgStore{pool: pool} }

// Monitors returns every enabled monitor with its config/baseline decoded.
func (s *PgStore) Monitors(ctx context.Context) ([]Monitor, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT id, pipeline_id, stream, column_name, kind, config, baseline
		 FROM quality_monitors WHERE enabled`)
	if err != nil {
		return nil, fmt.Errorf("query monitors: %w", err)
	}
	defer rows.Close()
	var out []Monitor
	for rows.Next() {
		var (
			m            Monitor
			cfgRaw, bRaw []byte
		)
		if err := rows.Scan(&m.ID, &m.PipelineID, &m.Stream, &m.Column, &m.Kind, &cfgRaw, &bRaw); err != nil {
			return nil, fmt.Errorf("scan monitor: %w", err)
		}
		m.Config = decodeFloats(cfgRaw)
		m.Baseline = decodeFloats(bRaw)
		out = append(out, m)
	}
	return out, rows.Err()
}

// LatestStat returns the newest column_stats for (pipeline, stream, column); an
// empty column matches the stream's newest stat (table-level monitors).
func (s *PgStore) LatestStat(ctx context.Context, pipelineID int64, stream, column string) (Stat, bool, error) {
	var (
		st  Stat
		err error
	)
	if column == "" {
		err = s.pool.QueryRow(ctx,
			`SELECT row_count, null_count, sync_id, ts FROM column_stats
			 WHERE pipeline_id=$1 AND stream=$2 ORDER BY ts DESC LIMIT 1`,
			pipelineID, stream).Scan(&st.Rows, &st.Nulls, &st.SyncID, &st.TS)
	} else {
		err = s.pool.QueryRow(ctx,
			`SELECT row_count, null_count, sync_id, ts FROM column_stats
			 WHERE pipeline_id=$1 AND stream=$2 AND column_name=$3 ORDER BY ts DESC LIMIT 1`,
			pipelineID, stream, column).Scan(&st.Rows, &st.Nulls, &st.SyncID, &st.TS)
	}
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Stat{}, false, nil
		}
		return Stat{}, false, fmt.Errorf("latest stat: %w", err)
	}
	return st, true, nil
}

// SinceLastSync returns how long ago the pipeline last synced (DB clock).
func (s *PgStore) SinceLastSync(ctx context.Context, pipelineID int64) (time.Duration, bool, error) {
	var secs *float64
	err := s.pool.QueryRow(ctx,
		`SELECT EXTRACT(EPOCH FROM (now() - last_sync_at)) FROM pipelines WHERE id=$1`,
		pipelineID).Scan(&secs)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("since last sync: %w", err)
	}
	if secs == nil {
		return 0, false, nil // never synced
	}
	return time.Duration(*secs * float64(time.Second)), true, nil
}

// RecordResult persists a monitor evaluation.
func (s *PgStore) RecordResult(ctx context.Context, monitorID int64, syncID string, r Result, ts time.Time) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO quality_results (monitor_id, sync_id, ts, value, breached, detail)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		monitorID, syncID, ts, r.Value, r.Breached, r.Detail)
	if err != nil {
		return fmt.Errorf("record quality result: %w", err)
	}
	return nil
}

// decodeFloats parses a JSONB object of numbers into a map (empty on error).
func decodeFloats(raw []byte) map[string]float64 {
	m := map[string]float64{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &m)
	}
	return m
}

package scheduler

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// pgLister reads schedulable pipelines from the store.
type pgLister struct{ pool *pgxpool.Pool }

// NewPgLister builds a Postgres-backed Lister.
func NewPgLister(pool *pgxpool.Pool) Lister { return &pgLister{pool: pool} }

// Active returns active pipelines that have a non-empty schedule.
func (l *pgLister) Active(ctx context.Context) ([]Schedule, error) {
	rows, err := l.pool.Query(ctx,
		`SELECT id, schedule, last_sync_at
		 FROM pipelines
		 WHERE status = 'active' AND schedule <> ''`)
	if err != nil {
		return nil, fmt.Errorf("list active pipelines: %w", err)
	}
	defer rows.Close()
	var out []Schedule
	for rows.Next() {
		var s Schedule
		if err := rows.Scan(&s.PipelineID, &s.Cron, &s.LastSync); err != nil {
			return nil, fmt.Errorf("scan schedule: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

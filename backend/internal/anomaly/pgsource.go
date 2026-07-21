package anomaly

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// allowedMetrics guards the metric column name against injection — the value is
// interpolated into SQL, so it must be an allow-listed column.
var allowedMetrics = map[string]bool{
	"rows_written": true, "rows_read": true, "bytes_written": true, "duration_seconds": true,
}

// PgSource loads metric series from the metrics table.
type PgSource struct{ pool *pgxpool.Pool }

// NewPgSource builds a Postgres-backed MetricsSource.
func NewPgSource(pool *pgxpool.Pool) *PgSource { return &PgSource{pool: pool} }

// RecentSeries returns, per active pipeline, the most-recent values of metric in
// chronological order (oldest→newest), capped at limit per pipeline.
func (s *PgSource) RecentSeries(ctx context.Context, metric string, limit int) ([]Series, error) {
	if !allowedMetrics[metric] {
		return nil, fmt.Errorf("unsupported metric %q", metric)
	}
	// Per pipeline: take the newest `limit` rows, then order ascending for the
	// baseline. window functions keep this to one query.
	q := fmt.Sprintf(`
		SELECT pipeline_id, array_agg(v ORDER BY ts) FROM (
		    SELECT m.pipeline_id, m.ts, m.%s::float8 AS v,
		           row_number() OVER (PARTITION BY m.pipeline_id ORDER BY m.ts DESC) AS rn
		    FROM metrics m
		    JOIN pipelines p ON p.id = m.pipeline_id AND p.status = 'active'
		) t
		WHERE rn <= $1
		GROUP BY pipeline_id`, metric)
	rows, err := s.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("query metric series: %w", err)
	}
	defer rows.Close()
	var out []Series
	for rows.Next() {
		var ser Series
		if err := rows.Scan(&ser.PipelineID, &ser.Values); err != nil {
			return nil, fmt.Errorf("scan series: %w", err)
		}
		out = append(out, ser)
	}
	return out, rows.Err()
}

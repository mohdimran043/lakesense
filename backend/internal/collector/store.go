package collector

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgSink persists collector output to Postgres. It implements Sink.
type PgSink struct {
	pool *pgxpool.Pool
}

// NewPgSink builds a Postgres-backed sink.
func NewPgSink(pool *pgxpool.Pool) *PgSink { return &PgSink{pool: pool} }

// pipelineArg maps a zero pipeline id to SQL NULL (engine runs outside a
// pipeline still land their raw events).
func pipelineArg(id int64) any {
	if id == 0 {
		return nil
	}
	return id
}

func (s *PgSink) InsertEvent(ctx context.Context, pipelineID int64, e Event) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO events (pipeline_id, sync_id, ts, kind, stream, payload)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		pipelineArg(pipelineID), e.SyncID, e.TS, e.Kind, e.Stream, e.Payload)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	return nil
}

func (s *PgSink) RecordMetric(ctx context.Context, pipelineID int64, syncID string, m SyncFinished, ts time.Time) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO metrics (pipeline_id, sync_id, ts, rows_read, rows_written, bytes_written, duration_seconds)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		pipelineID, syncID, ts, m.RowsRead, m.RowsWritten, m.BytesWritten, m.DurationSeconds)
	if err != nil {
		return fmt.Errorf("insert metric: %w", err)
	}
	return nil
}

func (s *PgSink) UpsertDiffRun(ctx context.Context, pipelineID int64, syncID, stream string, d DiffRun) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO diff_runs (pipeline_id, stream, sync_id, kind, source_rows, dest_rows, source_checksum, dest_checksum, match)
		 VALUES ($1, $2, $3, 'sync', $4, $5, $6, $7, $8)`,
		pipelineID, stream, syncID, d.SourceRows, d.DestRows, d.SourceChecksum, d.DestChecksum, d.Match)
	if err != nil {
		return fmt.Errorf("insert diff run: %w", err)
	}
	return nil
}

func (s *PgSink) RecordLineage(ctx context.Context, pipelineID int64, stream string, m ColumnMapping, syncID string) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO lineage_edges (pipeline_id, source_stream, source_column, source_type, dest_table, dest_column, dest_type, sync_id)
		 VALUES ($1, $2, $3, $4, $2, $5, $6, $7)
		 ON CONFLICT (pipeline_id, source_stream, source_column, dest_column)
		 DO UPDATE SET source_type = EXCLUDED.source_type, dest_type = EXCLUDED.dest_type,
		               sync_id = EXCLUDED.sync_id, updated_at = now()`,
		pipelineID, stream, m.SourceColumn, m.SourceType, m.DestColumn, m.DestType, syncID)
	if err != nil {
		return fmt.Errorf("upsert lineage: %w", err)
	}
	return nil
}

func (s *PgSink) RecordColumnStats(ctx context.Context, pipelineID int64, stream, syncID string, ts time.Time, cols []ColumnStat) error {
	for _, c := range cols {
		_, err := s.pool.Exec(ctx,
			`INSERT INTO column_stats (pipeline_id, stream, column_name, sync_id, ts, row_count, null_count, distinct_estimate, min_value, max_value)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			pipelineID, stream, c.Column, syncID, ts, c.Rows, c.Nulls, c.Distinct, c.Min, c.Max)
		if err != nil {
			return fmt.Errorf("insert column stats for %s.%s: %w", stream, c.Column, err)
		}
	}
	return nil
}

func (s *PgSink) MarkSynced(ctx context.Context, pipelineID int64, e Event) error {
	if pipelineID == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx,
		`UPDATE pipelines SET last_sync_at = $2, updated_at = now() WHERE id = $1`,
		pipelineID, e.TS)
	if err != nil {
		return fmt.Errorf("mark synced: %w", err)
	}
	return nil
}

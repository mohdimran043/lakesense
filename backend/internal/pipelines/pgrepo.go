package pipelines

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lakesense/lakesense/backend/internal/configver"
)

// pgRepo is the Postgres-backed Repo. Multi-table mutations run in one
// transaction so a pipeline and its config version are always consistent.
type pgRepo struct{ pool *pgxpool.Pool }

// NewPgRepo builds a Postgres Repo over the pool.
func NewPgRepo(pool *pgxpool.Pool) *pgRepo { return &pgRepo{pool: pool} }

func (r *pgRepo) EnsureEnvironment(ctx context.Context, slug string) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`INSERT INTO environments (name, slug, kind) VALUES ($1, $1, 'dev')
		 ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`, slug).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ensure environment %s: %w", slug, err)
	}
	return id, nil
}

func (r *pgRepo) CreatePipeline(ctx context.Context, envID int64, p PipelineRow, v configver.Version, cfgJSON []byte) (int64, error) {
	var id int64
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO pipelines (environment_id, name, slug, source_type, source_config,
			     destination_type, destination_config, catalog, schedule, status, current_version)
			 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id`,
			envID, p.Name, p.Slug, p.SourceType, p.SourceConfig, p.DestinationType,
			p.DestinationConfig, p.Catalog, p.Schedule, p.Status, p.CurrentVersion).Scan(&id); err != nil {
			return fmt.Errorf("insert pipeline: %w", err)
		}
		return insertVersion(ctx, tx, id, v, cfgJSON)
	})
	if err != nil {
		return 0, err
	}
	return id, nil
}

func (r *pgRepo) GetPipeline(ctx context.Context, id int64) (PipelineRow, bool, error) {
	var p PipelineRow
	err := r.pool.QueryRow(ctx,
		`SELECT name, slug, source_type, destination_type, schedule, status,
		        source_config, destination_config, catalog, current_version
		 FROM pipelines WHERE id = $1`, id).Scan(
		&p.Name, &p.Slug, &p.SourceType, &p.DestinationType, &p.Schedule, &p.Status,
		&p.SourceConfig, &p.DestinationConfig, &p.Catalog, &p.CurrentVersion)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PipelineRow{}, false, nil
		}
		return PipelineRow{}, false, fmt.Errorf("get pipeline %d: %w", id, err)
	}
	return p, true, nil
}

func (r *pgRepo) ConfigHistory(ctx context.Context, id int64) ([]configver.Version, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT version, yaml, note, created_by, created_at
		 FROM pipeline_config_versions WHERE pipeline_id = $1 ORDER BY version`, id)
	if err != nil {
		return nil, fmt.Errorf("config history %d: %w", id, err)
	}
	defer rows.Close()
	var out []configver.Version
	for rows.Next() {
		var v configver.Version
		if err := rows.Scan(&v.Number, &v.YAML, &v.Note, &v.CreatedBy, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan version: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func (r *pgRepo) UpdatePipeline(ctx context.Context, id int64, p PipelineRow, v configver.Version, cfgJSON []byte, newVersion bool) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx,
			`UPDATE pipelines SET name=$2, slug=$3, source_type=$4, source_config=$5,
			     destination_type=$6, destination_config=$7, catalog=$8, schedule=$9,
			     status=$10, current_version=$11, updated_at=now()
			 WHERE id=$1`,
			id, p.Name, p.Slug, p.SourceType, p.SourceConfig, p.DestinationType,
			p.DestinationConfig, p.Catalog, p.Schedule, p.Status, p.CurrentVersion); err != nil {
			return fmt.Errorf("update pipeline: %w", err)
		}
		if newVersion {
			return insertVersion(ctx, tx, id, v, cfgJSON)
		}
		return nil
	})
}

func (r *pgRepo) SetStatus(ctx context.Context, id int64, status string) error {
	_, err := r.pool.Exec(ctx, `UPDATE pipelines SET status=$2, updated_at=now() WHERE id=$1`, id, status)
	if err != nil {
		return fmt.Errorf("set status %d: %w", id, err)
	}
	return nil
}

// insertVersion writes one config version row inside a transaction.
func insertVersion(ctx context.Context, tx pgx.Tx, pipelineID int64, v configver.Version, cfgJSON []byte) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO pipeline_config_versions (pipeline_id, version, yaml, config, note, created_by, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7)`,
		pipelineID, v.Number, v.YAML, cfgJSON, v.Note, v.CreatedBy, v.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert config version %d: %w", v.Number, err)
	}
	return nil
}

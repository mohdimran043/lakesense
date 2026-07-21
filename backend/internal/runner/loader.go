package runner

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgLoader loads a runnable pipeline's engine inputs from the store.
type pgLoader struct{ pool *pgxpool.Pool }

// NewPgLoader builds a Postgres-backed Loader.
func NewPgLoader(pool *pgxpool.Pool) Loader { return &pgLoader{pool: pool} }

// Load returns a pipeline's config, or ok=false when it is absent or archived.
func (l *pgLoader) Load(ctx context.Context, id int64) (PipelineConfig, bool, error) {
	var (
		cfg    PipelineConfig
		status string
		cat    []byte
	)
	err := l.pool.QueryRow(ctx,
		`SELECT source_type, source_config, destination_config, catalog, status
		 FROM pipelines WHERE id = $1`, id).Scan(
		&cfg.SourceType, &cfg.SourceConfig, &cfg.DestinationConfig, &cat, &status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return PipelineConfig{}, false, nil
		}
		return PipelineConfig{}, false, fmt.Errorf("load pipeline %d: %w", id, err)
	}
	if status == "archived" {
		return PipelineConfig{}, false, nil
	}
	sels, err := parseSelections(cat)
	if err != nil {
		return PipelineConfig{}, false, err
	}
	cfg.Selections = sels
	return cfg, true, nil
}

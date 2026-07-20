package seed

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ensureEnvironment upserts an environment by slug, returning its id.
func ensureEnvironment(ctx context.Context, pool *pgxpool.Pool, name, slug, kind string) (int64, error) {
	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO environments (name, slug, kind) VALUES ($1, $2, $3)
		 ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`, name, slug, kind).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ensure environment %s: %w", slug, err)
	}
	return id, nil
}

// ensurePipeline upserts a pipeline (and an initial config version) for the
// given environment, returning its id. Re-running seed is idempotent on the
// pipeline row; event history is appended.
func ensurePipeline(ctx context.Context, pool *pgxpool.Pool, envID int64, spec pipelineSpec) (int64, error) {
	catalog, _ := json.Marshal(map[string]any{"streams": spec.streams})
	var id int64
	err := pool.QueryRow(ctx,
		`INSERT INTO pipelines (environment_id, name, slug, source_type, destination_type, catalog, schedule, status, current_version)
		 VALUES ($1, $2, $3, $4, $5, $6, '@daily', 'active', 1)
		 ON CONFLICT (environment_id, slug)
		 DO UPDATE SET name = EXCLUDED.name, source_type = EXCLUDED.source_type,
		               destination_type = EXCLUDED.destination_type, catalog = EXCLUDED.catalog
		 RETURNING id`,
		envID, spec.name, spec.slug, spec.sourceType, spec.destType, catalog).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("ensure pipeline %s: %w", spec.slug, err)
	}

	// Seed an initial config version (idempotent on (pipeline, version)).
	cfg, _ := json.Marshal(map[string]any{
		"source":      map[string]any{"type": spec.sourceType},
		"destination": map[string]any{"type": spec.destType},
		"streams":     spec.streams,
	})
	yaml := fmt.Sprintf("name: %s\nsource:\n  type: %s\ndestination:\n  type: %s\nstreams:\n", spec.name, spec.sourceType, spec.destType)
	for _, s := range spec.streams {
		yaml += "  - " + s + "\n"
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO pipeline_config_versions (pipeline_id, version, yaml, config, note, created_by)
		 VALUES ($1, 1, $2, $3, 'initial', 'seed')
		 ON CONFLICT (pipeline_id, version) DO NOTHING`,
		id, yaml, cfg)
	if err != nil {
		return 0, fmt.Errorf("seed config version for %s: %w", spec.slug, err)
	}
	return id, nil
}

package audit

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgRecorder writes audit entries to the append-only audit_log table.
type PgRecorder struct {
	pool *pgxpool.Pool
}

// NewPgRecorder builds a Postgres-backed recorder.
func NewPgRecorder(pool *pgxpool.Pool) *PgRecorder { return &PgRecorder{pool: pool} }

// Record appends one entry. There is intentionally no update or delete path.
func (r *PgRecorder) Record(ctx context.Context, e Entry) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO audit_log (actor, action, entity_type, entity_id, before, after, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		e.Actor, e.Action, e.EntityType, e.EntityID, e.Before, e.After, e.At)
	if err != nil {
		return fmt.Errorf("append audit entry: %w", err)
	}
	return nil
}

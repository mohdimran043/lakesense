package channels

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PgResolver loads channels from the channels table. It implements Resolver.
type PgResolver struct{ pool *pgxpool.Pool }

// NewPgResolver builds a Postgres-backed channel resolver.
func NewPgResolver(pool *pgxpool.Pool) *PgResolver { return &PgResolver{pool: pool} }

// Channel loads an enabled channel by id.
func (r *PgResolver) Channel(ctx context.Context, id int64) (Channel, error) {
	var (
		ch      Channel
		cfgRaw  []byte
		enabled bool
	)
	err := r.pool.QueryRow(ctx,
		`SELECT id, name, type, config, enabled FROM channels WHERE id = $1`, id).
		Scan(&ch.ID, &ch.Name, &ch.Type, &cfgRaw, &enabled)
	if err != nil {
		return Channel{}, fmt.Errorf("load channel %d: %w", id, err)
	}
	if !enabled {
		return Channel{}, fmt.Errorf("channel %d is disabled", id)
	}
	if len(cfgRaw) > 0 {
		if err := json.Unmarshal(cfgRaw, &ch.Config); err != nil {
			return Channel{}, fmt.Errorf("decode channel %d config: %w", id, err)
		}
	}
	return ch, nil
}

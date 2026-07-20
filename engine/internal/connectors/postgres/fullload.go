package postgres

import (
	"context"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// ctidMaxPage is the open-ended upper bound for the final CTID chunk,
// catching pages appended after planning.
const ctidMaxPage = int64(1) << 31

// SplitChunks implements sdk.FullLoader. Default: CTID page-range chunks
// (works without a PK). "keyset" strategy: arithmetic ranges over a single
// integer PK. Chunk Min/Max encode either page numbers (ctid) or PK values
// (keyset) as strings; ReadChunk knows which by the configured strategy.
func (c *Connector) SplitChunks(ctx context.Context, stream model.Stream) ([]state.Chunk, error) {
	if c.pool == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	if c.cfg.ChunkStrategy == "keyset" || c.presetForbidsCTID() {
		return c.splitKeyset(ctx, stream)
	}
	return c.splitCTID(ctx, stream)
}

// presetForbidsCTID reports variants without physical CTID addressing.
func (c *Connector) presetForbidsCTID() bool {
	return c.cfg.Preset == "cockroachdb" || c.cfg.Preset == "yugabytedb"
}

func (c *Connector) splitCTID(ctx context.Context, stream model.Stream) ([]state.Chunk, error) {
	var blockSize int
	if err := c.pool.QueryRow(ctx, "SELECT current_setting('block_size')::int").Scan(&blockSize); err != nil {
		return nil, fmt.Errorf("read block_size: %w", err)
	}
	var relpages int64
	var relkind string
	err := c.pool.QueryRow(ctx,
		`SELECT c.relpages, c.relkind
		 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname = $2`,
		stream.Namespace, stream.Name).Scan(&relpages, &relkind)
	if err != nil {
		return nil, fmt.Errorf("read relpages for %s: %w", stream.ID(), err)
	}
	// Partitioned parents report zero pages; their physical pages live in the
	// leaves. v0.1 keeps it simple and honest: fall back to keyset when
	// possible, else one open chunk (single-pass scan).
	if relkind == "p" {
		if pk := stream.Schema.PrimaryKey(); len(pk) == 1 {
			if col, _ := stream.Schema.Column(pk[0]); col.Type == model.TypeInt32 || col.Type == model.TypeInt64 {
				return c.splitKeyset(ctx, stream)
			}
		}
		return []state.Chunk{{}}, nil
	}

	pagesPerChunk := int64(c.cfg.ChunkTargetMB) * 1024 * 1024 / int64(blockSize)
	if pagesPerChunk < 1 {
		pagesPerChunk = 1
	}
	var chunks []state.Chunk
	for page := int64(0); page < relpages; page += pagesPerChunk {
		chunks = append(chunks, state.Chunk{
			Min: strconv.FormatInt(page, 10),
			Max: strconv.FormatInt(page+pagesPerChunk, 10),
		})
	}
	if len(chunks) == 0 {
		// Empty/tiny table: one chunk covering everything.
		return []state.Chunk{{Min: "0", Max: strconv.FormatInt(ctidMaxPage, 10)}}, nil
	}
	// Extend the final chunk to the sentinel so post-planning growth is read.
	chunks[len(chunks)-1].Max = strconv.FormatInt(ctidMaxPage, 10)
	return chunks, nil
}

// splitKeyset produces arithmetic ranges over a single integer PK (or the
// stream's sole PK column). Sparse key spaces produce some empty chunks,
// which read fast and stay correct.
func (c *Connector) splitKeyset(ctx context.Context, stream model.Stream) ([]state.Chunk, error) {
	pk := stream.Schema.PrimaryKey()
	if len(pk) != 1 {
		return nil, fmt.Errorf("keyset chunking for %s requires a single-column primary key (has %d)", stream.ID(), len(pk))
	}
	col, _ := stream.Schema.Column(pk[0])
	if col.Type != model.TypeInt32 && col.Type != model.TypeInt64 {
		return nil, fmt.Errorf("keyset chunking for %s requires an integer primary key, %s is %s", stream.ID(), pk[0], col.Type)
	}

	quotedCol := quoteIdent(pk[0])
	var minV, maxV *int64
	q := fmt.Sprintf("SELECT min(%s)::bigint, max(%s)::bigint FROM %s", quotedCol, quotedCol, qualifiedTable(stream))
	if err := c.pool.QueryRow(ctx, q).Scan(&minV, &maxV); err != nil {
		return nil, fmt.Errorf("min/max of %s.%s: %w", stream.ID(), pk[0], err)
	}
	if minV == nil || maxV == nil {
		return []state.Chunk{{}}, nil // empty table: one open chunk
	}

	// Estimate rows from stats to size ranges; fall back to a fixed span.
	var estRows int64
	err := c.pool.QueryRow(ctx,
		`SELECT c.reltuples::bigint
		 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname = $2`,
		stream.Namespace, stream.Name).Scan(&estRows)
	if err != nil {
		return nil, fmt.Errorf("estimate rows for %s: %w", stream.ID(), err)
	}
	span := *maxV - *minV + 1
	targetRowsPerChunk := int64(c.cfg.ChunkTargetMB) * 1024 * 4 // assume ≥256B/row ⇒ rows per MiB ≈ 4096
	step := span
	if estRows > targetRowsPerChunk {
		step = span * targetRowsPerChunk / estRows
		if step < 1 {
			step = 1
		}
	}

	var chunks []state.Chunk
	// Leading open chunk catches keys below the planned min inserted later.
	chunks = append(chunks, state.Chunk{Max: strconv.FormatInt(*minV, 10)})
	for lo := *minV; lo <= *maxV; lo += step {
		chunks = append(chunks, state.Chunk{
			Min: strconv.FormatInt(lo, 10),
			Max: strconv.FormatInt(lo+step, 10),
		})
	}
	// Trailing open chunk catches keys above the planned max.
	chunks[len(chunks)-1].Max = ""
	return chunks, nil
}

// ReadChunk implements sdk.FullLoader: streams one chunk inside a
// repeatable-read read-only transaction.
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, chunk state.Chunk, emit sdk.RowFunc) error {
	if c.pool == nil {
		return fmt.Errorf("connector not set up")
	}
	query, args, err := c.chunkQuery(stream, chunk)
	if err != nil {
		return err
	}

	tx, err := c.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin chunk tx: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return fmt.Errorf("set chunk tx isolation: %w", err)
	}

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query chunk [%s,%s) of %s: %w", chunk.Min, chunk.Max, stream.ID(), err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return fmt.Errorf("read row values: %w", err)
		}
		row := make(sdk.Row, len(fields))
		for i, f := range fields {
			row[f.Name] = NormalizeValue(values[i])
		}
		if err := emit(ctx, row); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate chunk [%s,%s) of %s: %w", chunk.Min, chunk.Max, stream.ID(), err)
	}
	return nil
}

// chunkQuery builds the SELECT for one chunk under the active strategy.
func (c *Connector) chunkQuery(stream model.Stream, chunk state.Chunk) (string, []any, error) {
	table := qualifiedTable(stream)
	if c.cfg.ChunkStrategy == "keyset" || c.presetForbidsCTID() {
		pk := stream.Schema.PrimaryKey()
		if len(pk) != 1 {
			return "", nil, fmt.Errorf("keyset read for %s requires a single-column primary key", stream.ID())
		}
		col := quoteIdent(pk[0])
		parse := func(s string) (int64, error) {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("bad keyset chunk bound %q: %w", s, err)
			}
			return v, nil
		}
		switch {
		case chunk.Min == "" && chunk.Max == "":
			return fmt.Sprintf("SELECT * FROM %s", table), nil, nil
		case chunk.Min == "":
			maxV, err := parse(chunk.Max)
			if err != nil {
				return "", nil, err
			}
			return fmt.Sprintf("SELECT * FROM %s WHERE %s < $1", table, col), []any{maxV}, nil
		case chunk.Max == "":
			minV, err := parse(chunk.Min)
			if err != nil {
				return "", nil, err
			}
			return fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1", table, col), []any{minV}, nil
		default:
			minV, err := parse(chunk.Min)
			if err != nil {
				return "", nil, err
			}
			maxV, err := parse(chunk.Max)
			if err != nil {
				return "", nil, err
			}
			return fmt.Sprintf("SELECT * FROM %s WHERE %s >= $1 AND %s < $2", table, col, col),
				[]any{minV, maxV}, nil
		}
	}

	// CTID strategy: bounds are page numbers; tid literals address (page, tuple).
	if chunk.Min == "" && chunk.Max == "" {
		return fmt.Sprintf("SELECT * FROM %s", table), nil, nil
	}
	minPage, err := strconv.ParseInt(chunk.Min, 10, 64)
	if err != nil {
		return "", nil, fmt.Errorf("bad ctid chunk min %q: %w", chunk.Min, err)
	}
	maxPage, err := strconv.ParseInt(chunk.Max, 10, 64)
	if err != nil {
		return "", nil, fmt.Errorf("bad ctid chunk max %q: %w", chunk.Max, err)
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE ctid >= '(%d,0)'::tid AND ctid < '(%d,0)'::tid", table, minPage, maxPage), nil, nil
}

// quoteIdent safely quotes a single SQL identifier.
func quoteIdent(name string) string {
	return pgx.Identifier{name}.Sanitize()
}

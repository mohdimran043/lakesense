package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// SplitChunks implements sdk.FullLoader. It partitions the table's implicit
// rowid space into arithmetic ranges, which resume cleanly (chunk bounds are
// rowid values in state). WITHOUT ROWID tables have no rowid; for those the
// rowid probe fails and the connector falls back to a single open chunk (a
// correct, if unpartitioned, full scan).
func (c *Connector) SplitChunks(ctx context.Context, stream model.Stream) ([]state.Chunk, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	var minV, maxV sql.NullInt64
	q := fmt.Sprintf("SELECT min(rowid), max(rowid) FROM %s", quoteIdent(stream.Name))
	if err := c.db.QueryRowContext(ctx, q).Scan(&minV, &maxV); err != nil {
		// No rowid (WITHOUT ROWID) or other probe failure: fall back to a
		// single open chunk — a correct, if unpartitioned, full scan.
		return []state.Chunk{{}}, nil //nolint:nilerr // intentional fallback, not error suppression
	}
	if !minV.Valid || !maxV.Valid {
		return []state.Chunk{{}}, nil // empty table
	}

	step := c.cfg.ChunkRows
	var chunks []state.Chunk
	// Leading open chunk catches rows below the planned min inserted later.
	chunks = append(chunks, state.Chunk{Max: strconv.FormatInt(minV.Int64, 10)})
	for lo := minV.Int64; lo <= maxV.Int64; lo += step {
		chunks = append(chunks, state.Chunk{
			Min: strconv.FormatInt(lo, 10),
			Max: strconv.FormatInt(lo+step, 10),
		})
	}
	// Trailing open chunk catches rows above the planned max.
	chunks[len(chunks)-1].Max = ""
	return chunks, nil
}

// ReadChunk implements sdk.FullLoader: streams one rowid range.
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, chunk state.Chunk, emit sdk.RowFunc) error {
	if c.db == nil {
		return fmt.Errorf("connector not set up")
	}
	query, args, err := chunkQuery(stream, chunk)
	if err != nil {
		return err
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query chunk [%s,%s) of %s: %w", chunk.Min, chunk.Max, stream.ID(), err)
	}
	defer func() { _ = rows.Close() }()
	return scanRows(ctx, rows, stream, emit)
}

// chunkQuery builds the SELECT for one rowid range.
func chunkQuery(stream model.Stream, chunk state.Chunk) (string, []any, error) {
	table := quoteIdent(stream.Name)
	parse := func(s string) (int64, error) {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("bad rowid chunk bound %q: %w", s, err)
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
		return fmt.Sprintf("SELECT * FROM %s WHERE rowid < ?", table), []any{maxV}, nil
	case chunk.Max == "":
		minV, err := parse(chunk.Min)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("SELECT * FROM %s WHERE rowid >= ?", table), []any{minV}, nil
	default:
		minV, err := parse(chunk.Min)
		if err != nil {
			return "", nil, err
		}
		maxV, err := parse(chunk.Max)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("SELECT * FROM %s WHERE rowid >= ? AND rowid < ?", table), []any{minV, maxV}, nil
	}
}

// scanRows maps a *sql.Rows cursor into engine rows, normalizing values by the
// stream's declared lake types.
func scanRows(ctx context.Context, rows *sql.Rows, stream model.Stream, emit sdk.RowFunc) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read columns: %w", err)
	}
	lake := make([]model.DataType, len(cols))
	for i, name := range cols {
		if col, ok := stream.Schema.Column(name); ok {
			lake[i] = col.Type
		} else {
			lake[i] = model.TypeString
		}
	}
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}
		row := make(sdk.Row, len(cols))
		for i, name := range cols {
			row[name] = normalizeValue(cells[i], lake[i])
		}
		if err := emit(ctx, row); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate rows of %s: %w", stream.ID(), err)
	}
	return nil
}

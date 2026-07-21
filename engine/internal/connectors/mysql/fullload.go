package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// SplitChunks implements sdk.FullLoader. When the stream has a single-column
// integer PK it partitions that key's [min,max] range into arithmetic chunks
// that resume cleanly (bounds are PK values in state). Otherwise it falls back
// to one open chunk — a correct, unpartitioned full scan.
func (c *Connector) SplitChunks(ctx context.Context, stream model.Stream) ([]state.Chunk, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	key, ok := integerPK(stream)
	if !ok {
		return []state.Chunk{{}}, nil // no integer PK → single open chunk
	}
	var minV, maxV sql.NullInt64
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quoteIdent(key), quoteIdent(key), qualified(stream))
	if err := c.db.QueryRowContext(ctx, q).Scan(&minV, &maxV); err != nil {
		return []state.Chunk{{}}, nil //nolint:nilerr // probe failure → safe single-chunk fallback
	}
	if !minV.Valid || !maxV.Valid {
		return []state.Chunk{{}}, nil // empty table
	}

	step := c.cfg.ChunkRows
	var chunks []state.Chunk
	// Leading open chunk catches rows below the observed min inserted later.
	chunks = append(chunks, state.Chunk{Max: strconv.FormatInt(minV.Int64, 10)})
	for lo := minV.Int64; lo <= maxV.Int64; lo += step {
		chunks = append(chunks, state.Chunk{
			Min: strconv.FormatInt(lo, 10),
			Max: strconv.FormatInt(lo+step, 10),
		})
	}
	// Trailing open chunk catches rows above the observed max.
	chunks[len(chunks)-1].Max = ""
	return chunks, nil
}

// ReadChunk implements sdk.FullLoader: streams one keyset range (or the whole
// table for the fallback open chunk).
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, chunk state.Chunk, emit sdk.RowFunc) error {
	if c.db == nil {
		return fmt.Errorf("connector not set up")
	}
	query, args := chunkQuery(stream, chunk)
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query chunk [%s,%s) of %s: %w", chunk.Min, chunk.Max, stream.ID(), err)
	}
	defer func() { _ = rows.Close() }()
	return scanRows(ctx, rows, stream, emit)
}

// chunkQuery builds the SELECT for one keyset range over the stream's integer PK.
func chunkQuery(stream model.Stream, chunk state.Chunk) (string, []any) {
	table := qualified(stream)
	key, ok := integerPK(stream)
	if !ok || (chunk.Min == "" && chunk.Max == "") {
		return fmt.Sprintf("SELECT * FROM %s", table), nil
	}
	k := quoteIdent(key)
	switch {
	case chunk.Min == "":
		return fmt.Sprintf("SELECT * FROM %s WHERE %s < ?", table, k), []any{chunk.Max}
	case chunk.Max == "":
		return fmt.Sprintf("SELECT * FROM %s WHERE %s >= ?", table, k), []any{chunk.Min}
	default:
		return fmt.Sprintf("SELECT * FROM %s WHERE %s >= ? AND %s < ?", table, k, k), []any{chunk.Min, chunk.Max}
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

// integerPK returns the single integer primary-key column, if any.
func integerPK(stream model.Stream) (string, bool) {
	pk := stream.Schema.PrimaryKey()
	if len(pk) != 1 {
		return "", false
	}
	col, ok := stream.Schema.Column(pk[0])
	if !ok || (col.Type != model.TypeInt32 && col.Type != model.TypeInt64) {
		return "", false
	}
	return pk[0], true
}

// qualified returns the backtick-quoted `db`.`table` identifier.
func qualified(stream model.Stream) string {
	return quoteIdent(stream.Namespace) + "." + quoteIdent(stream.Name)
}

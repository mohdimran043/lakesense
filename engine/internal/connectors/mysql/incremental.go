package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// normalizeValue converts a go-sql-driver scan value into a JSON-friendly lake
// value. The driver returns int64/float64 for numbers, []byte for text/decimal/
// blob, and time.Time for date/datetime (ParseTime is on). Byte slices for
// non-binary columns become strings so the destination and checksum see stable,
// readable values; genuine binary keeps its bytes.
func normalizeValue(v any, lake model.DataType) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		if lake == model.TypeBinary {
			return x
		}
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	default:
		return v
	}
}

// MaxCursor implements sdk.IncrementalReader: the current maximum cursor value,
// captured before full load so the later increment is gap-free.
func (c *Connector) MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error) {
	if c.db == nil {
		return "", fmt.Errorf("connector not set up")
	}
	q := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(cursorField), qualified(stream))
	var v sql.NullString
	if err := c.db.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return "", fmt.Errorf("max cursor of %s.%s: %w", stream.ID(), cursorField, err)
	}
	if !v.Valid {
		return "", nil
	}
	return v.String, nil
}

// ReadIncrement implements sdk.IncrementalReader: rows with cursor > since (all
// rows when since is empty), returning the new high watermark.
func (c *Connector) ReadIncrement(ctx context.Context, stream model.Stream, cursorField, since string, emit sdk.RowFunc) (string, error) {
	if c.db == nil {
		return "", fmt.Errorf("connector not set up")
	}
	col := quoteIdent(cursorField)
	var (
		query string
		args  []any
	)
	if since == "" {
		query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s", qualified(stream), col)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s", qualified(stream), col, col)
		args = []any{since}
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return "", fmt.Errorf("incremental query of %s: %w", stream.ID(), err)
	}
	defer func() { _ = rows.Close() }()

	newCursor := since
	err = scanRows(ctx, rows, stream, func(ctx context.Context, row sdk.Row) error {
		if v, ok := row[cursorField]; ok && v != nil {
			newCursor = fmt.Sprintf("%v", v)
		}
		return emit(ctx, row)
	})
	if err != nil {
		return "", err
	}
	return newCursor, nil
}

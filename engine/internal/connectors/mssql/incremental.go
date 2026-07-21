package mssql

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// MaxCursor implements sdk.IncrementalReader.
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

// ReadIncrement implements sdk.IncrementalReader.
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
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > @p1 ORDER BY %s", qualified(stream), col, col)
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

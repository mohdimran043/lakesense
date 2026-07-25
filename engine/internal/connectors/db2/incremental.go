//go:build db2

package db2

import (
	"context"
	"fmt"
	"time"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// MaxCursor implements sdk.IncrementalReader. It scans the max value generically
// and runs it through normalizeValue so the high watermark uses the exact same
// string form (RFC3339 for timestamps, int64 for integers) that ReadIncrement
// emits per row — keeping watermarks comparable across runs.
func (c *Connector) MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error) {
	if c.db == nil {
		return "", fmt.Errorf("connector not set up")
	}
	q := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(cursorField), qualified(stream))
	var raw any
	if err := c.db.QueryRowContext(ctx, q).Scan(&raw); err != nil {
		return "", fmt.Errorf("max cursor of %s.%s: %w", stream.ID(), cursorField, err)
	}
	if raw == nil {
		return "", nil
	}
	lake := model.TypeString
	if col, ok := stream.Schema.Column(cursorField); ok {
		lake = col.Type
	}
	v := normalizeValue(raw, lake, false)
	if v == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", v), nil
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
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s", qualified(stream), col, col)
		args = []any{cursorBind(stream, cursorField, since)}
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

// cursorBind converts the engine's opaque cursor string into the value bound
// into WHERE <col> > ?. A DATE/TIME/TIMESTAMP cursor is stored as the RFC3339
// string normalizeValue/MaxCursor emit; binding that raw string forces Db2 into
// a string→timestamp cast that rejects the 'T'/'Z' RFC3339 form (SQL0180N).
// Parsing it back to time.Time lets go_ibm_db bind a native timestamp matching
// the column type. Numeric/string cursors bind as-is; an unparseable timestamp
// falls back to the string (best effort, no panic).
func cursorBind(stream model.Stream, cursorField, since string) any {
	col, ok := stream.Schema.Column(cursorField)
	if !ok || (col.Type != model.TypeTimestamp && col.Type != model.TypeDate) {
		return since
	}
	if t, err := time.Parse(time.RFC3339Nano, since); err == nil {
		return t.UTC()
	}
	return since
}

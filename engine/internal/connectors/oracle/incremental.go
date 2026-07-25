package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"time"

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
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > :1 ORDER BY %s", qualified(stream), col, col)
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
// into WHERE <col> > :1. For a DATE/TIMESTAMP cursor column, the stored value
// is the RFC3339Nano string normalizeValue/MaxCursor emit; binding it as a raw
// string forces Oracle into an implicit string→date conversion against the
// session NLS format (e.g. DD-MON-RR), which rejects RFC3339 and raises
// ORA-01858/ORA-01861. Parsing it back to time.Time lets go-ora bind a native
// Oracle date, matching the column type. Numeric/string cursors bind as-is, and
// an unparseable timestamp falls back to the string (best effort, no panic).
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

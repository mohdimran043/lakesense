package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// MaxCursor implements sdk.IncrementalReader: the current max cursor value as a
// normalized string, captured before full load so the increment is gap-free.
// SQLite is dynamically typed; cursor values round-trip through their string
// form, and comparison is delegated to SQLite via a bound parameter.
func (c *Connector) MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error) {
	if c.db == nil {
		return "", fmt.Errorf("connector not set up")
	}
	if _, ok := stream.Schema.Column(cursorField); !ok {
		return "", fmt.Errorf("cursor field %q not in schema of %s", cursorField, stream.ID())
	}
	q := fmt.Sprintf("SELECT max(%s) FROM %s", quoteIdent(cursorField), quoteIdent(stream.Name))
	var raw sql.NullString
	if err := c.db.QueryRowContext(ctx, q).Scan(&raw); err != nil {
		return "", fmt.Errorf("max(%s) of %s: %w", cursorField, stream.ID(), err)
	}
	if !raw.Valid {
		return "", nil // empty table: no watermark yet
	}
	return raw.String, nil
}

// ReadIncrement implements sdk.IncrementalReader: rows with cursor > since (all
// rows when since is empty), ordered by cursor so the watermark is monotonic.
// Binding `since` as a parameter lets SQLite apply its own affinity-aware
// comparison, which is correct for integer, real, and ISO-8601 text cursors.
func (c *Connector) ReadIncrement(ctx context.Context, stream model.Stream, cursorField, since string, emit sdk.RowFunc) (string, error) {
	if c.db == nil {
		return "", fmt.Errorf("connector not set up")
	}
	if _, ok := stream.Schema.Column(cursorField); !ok {
		return "", fmt.Errorf("cursor field %q not in schema of %s", cursorField, stream.ID())
	}
	table := quoteIdent(stream.Name)
	col := quoteIdent(cursorField)

	var query string
	var args []any
	if since == "" {
		query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s", table, col)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s", table, col, col)
		args = append(args, since)
	}

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return "", fmt.Errorf("incremental query for %s: %w", stream.ID(), err)
	}
	defer func() { _ = rows.Close() }()

	newCursor := since
	err = scanRows(ctx, rows, stream, func(ctx context.Context, row sdk.Row) error {
		if v, ok := row[cursorField]; ok && v != nil {
			newCursor = fmt.Sprintf("%v", v) // rows are cursor-ordered; last wins
		}
		return emit(ctx, row)
	})
	if err != nil {
		return "", err
	}
	return newCursor, nil
}

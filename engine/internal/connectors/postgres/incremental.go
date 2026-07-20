package postgres

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
)

// cursorTimeFormat is the normalized state form for time cursors.
const cursorTimeFormat = time.RFC3339Nano

// MaxCursor implements sdk.IncrementalReader: the current max cursor value,
// captured BEFORE full load so the later increment is gap-free
// (docs/analysis/state-and-recovery.md §3).
func (c *Connector) MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("connector not set up")
	}
	col, ok := stream.Schema.Column(cursorField)
	if !ok {
		return "", fmt.Errorf("cursor field %q not in schema of %s", cursorField, stream.ID())
	}
	q := fmt.Sprintf("SELECT max(%s) FROM %s", quoteIdent(cursorField), qualifiedTable(stream))
	var raw any
	if err := c.pool.QueryRow(ctx, q).Scan(&raw); err != nil {
		return "", fmt.Errorf("max(%s) of %s: %w", cursorField, stream.ID(), err)
	}
	if raw == nil {
		return "", nil // empty table: no watermark yet
	}
	return formatCursor(col.Type, NormalizeValue(raw))
}

// ReadIncrement implements sdk.IncrementalReader: rows with cursor > since
// (all rows when since is empty), ordered by cursor so progress is monotonic.
func (c *Connector) ReadIncrement(ctx context.Context, stream model.Stream, cursorField, since string, emit sdk.RowFunc) (string, error) {
	if c.pool == nil {
		return "", fmt.Errorf("connector not set up")
	}
	col, ok := stream.Schema.Column(cursorField)
	if !ok {
		return "", fmt.Errorf("cursor field %q not in schema of %s", cursorField, stream.ID())
	}

	table := qualifiedTable(stream)
	quotedCursor := quoteIdent(cursorField)
	var query string
	var args []any
	if since == "" {
		query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s", table, quotedCursor)
	} else {
		arg, err := parseCursor(col.Type, since)
		if err != nil {
			return "", fmt.Errorf("cursor %q for %s.%s: %w", since, stream.ID(), cursorField, err)
		}
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s", table, quotedCursor, quotedCursor)
		args = append(args, arg)
	}

	rows, err := c.pool.Query(ctx, query, args...)
	if err != nil {
		return "", fmt.Errorf("incremental query for %s: %w", stream.ID(), err)
	}
	defer rows.Close()

	fields := rows.FieldDescriptions()
	newCursor := since
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return "", fmt.Errorf("read row values: %w", err)
		}
		row := make(sdk.Row, len(fields))
		for i, f := range fields {
			row[f.Name] = NormalizeValue(values[i])
		}
		if v, ok := row[cursorField]; ok && v != nil {
			formatted, err := formatCursor(col.Type, v)
			if err != nil {
				return "", err
			}
			newCursor = formatted // rows are cursor-ordered; last seen is max
		}
		if err := emit(ctx, row); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate increment of %s: %w", stream.ID(), err)
	}
	return newCursor, nil
}

// formatCursor normalizes a cursor value to its state string form.
func formatCursor(t model.DataType, v any) (string, error) {
	switch t {
	case model.TypeTimestamp, model.TypeDate:
		tv, ok := v.(time.Time)
		if !ok {
			return "", fmt.Errorf("expected time cursor, got %T", v)
		}
		return tv.UTC().Format(cursorTimeFormat), nil
	case model.TypeInt32, model.TypeInt64:
		switch n := v.(type) {
		case int32:
			return strconv.FormatInt(int64(n), 10), nil
		case int64:
			return strconv.FormatInt(n, 10), nil
		default:
			return "", fmt.Errorf("expected integer cursor, got %T", v)
		}
	case model.TypeString, model.TypeDecimal:
		s, ok := v.(string)
		if !ok {
			return fmt.Sprintf("%v", v), nil
		}
		return s, nil
	default:
		return "", fmt.Errorf("type %s cannot be an incremental cursor", t)
	}
}

// parseCursor converts a state string back to a query argument.
func parseCursor(t model.DataType, s string) (any, error) {
	switch t {
	case model.TypeTimestamp, model.TypeDate:
		tv, err := time.Parse(cursorTimeFormat, s)
		if err != nil {
			return nil, fmt.Errorf("parse time cursor: %w", err)
		}
		return tv, nil
	case model.TypeInt32, model.TypeInt64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse integer cursor: %w", err)
		}
		return n, nil
	case model.TypeString, model.TypeDecimal:
		return s, nil
	default:
		return nil, fmt.Errorf("type %s cannot be an incremental cursor", t)
	}
}

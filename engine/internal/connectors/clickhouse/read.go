package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// mapType maps a ClickHouse column type to a lake type, unwrapping Nullable(…)
// and LowCardinality(…) wrappers first.
func mapType(chType string) model.DataType {
	t := unwrap(chType)
	switch {
	case strings.HasPrefix(t, "Int8"), strings.HasPrefix(t, "Int16"), strings.HasPrefix(t, "Int32"),
		strings.HasPrefix(t, "UInt8"), strings.HasPrefix(t, "UInt16"), strings.HasPrefix(t, "UInt32"):
		return model.TypeInt32
	case strings.HasPrefix(t, "Int64"), strings.HasPrefix(t, "UInt64"),
		strings.HasPrefix(t, "Int128"), strings.HasPrefix(t, "UInt128"):
		return model.TypeInt64
	case strings.HasPrefix(t, "Float32"):
		return model.TypeFloat32
	case strings.HasPrefix(t, "Float64"):
		return model.TypeFloat64
	case strings.HasPrefix(t, "Decimal"):
		return model.TypeDecimal
	case t == "Bool", t == "Boolean":
		return model.TypeBool
	case t == "Date", t == "Date32":
		return model.TypeDate
	case strings.HasPrefix(t, "DateTime"):
		return model.TypeTimestamp
	case strings.HasPrefix(t, "Array("), strings.HasPrefix(t, "Map("), strings.HasPrefix(t, "Tuple("), t == "JSON":
		return model.TypeJSON
	default:
		// String, FixedString, UUID, IPv4/6, Enum → string.
		return model.TypeString
	}
}

// unwrap strips Nullable(...) and LowCardinality(...) wrappers.
func unwrap(t string) string {
	for {
		switch {
		case strings.HasPrefix(t, "Nullable(") && strings.HasSuffix(t, ")"):
			t = t[len("Nullable(") : len(t)-1]
		case strings.HasPrefix(t, "LowCardinality(") && strings.HasSuffix(t, ")"):
			t = t[len("LowCardinality(") : len(t)-1]
		default:
			return t
		}
	}
}

// SplitChunks implements sdk.FullLoader: a single open chunk. ClickHouse keys are
// not unique, so keyset partitioning would risk gaps/overlaps; a single ordered
// scan is correct.
func (c *Connector) SplitChunks(_ context.Context, _ model.Stream) ([]state.Chunk, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	return []state.Chunk{{}}, nil
}

// ReadChunk implements sdk.FullLoader: streams the whole table.
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	if c.db == nil {
		return fmt.Errorf("connector not set up")
	}
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s", qualified(stream)))
	if err != nil {
		return fmt.Errorf("query %s: %w", stream.ID(), err)
	}
	defer func() { _ = rows.Close() }()
	return scanRows(ctx, rows, emit)
}

// MaxCursor implements sdk.IncrementalReader.
func (c *Connector) MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error) {
	if c.db == nil {
		return "", fmt.Errorf("connector not set up")
	}
	var v sql.NullString
	q := fmt.Sprintf("SELECT toString(max(%s)) FROM %s", quoteIdent(cursorField), qualified(stream))
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
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s", qualified(stream), col, col)
		args = []any{since}
	}
	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return "", fmt.Errorf("incremental query of %s: %w", stream.ID(), err)
	}
	defer func() { _ = rows.Close() }()

	newCursor := since
	err = scanRows(ctx, rows, func(ctx context.Context, row sdk.Row) error {
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

// scanRows maps a *sql.Rows cursor into engine rows using ClickHouse column types.
func scanRows(ctx context.Context, rows *sql.Rows, emit sdk.RowFunc) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read columns: %w", err)
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
			row[name] = normalizeValue(cells[i])
		}
		if err := emit(ctx, row); err != nil {
			return err
		}
	}
	return rows.Err()
}

// normalizeValue coerces a clickhouse-go scan value into a JSON-friendly value.
func normalizeValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case fmt.Stringer:
		return x.String() // e.g. decimal, UUID, big.Int wrappers
	default:
		return v
	}
}

package cassandra

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// mapType maps a CQL type to a lake type. Collection and tuple types land as JSON.
func mapType(cqlType string) model.DataType {
	t := strings.ToLower(strings.TrimSpace(cqlType))
	switch {
	case t == "tinyint", t == "smallint", t == "int":
		return model.TypeInt32
	case t == "bigint", t == "counter", t == "varint":
		return model.TypeInt64
	case t == "boolean":
		return model.TypeBool
	case t == "float":
		return model.TypeFloat32
	case t == "double":
		return model.TypeFloat64
	case t == "decimal":
		return model.TypeDecimal
	case t == "date":
		return model.TypeDate
	case t == "timestamp", t == "time":
		return model.TypeTimestamp
	case t == "blob":
		return model.TypeBinary
	case strings.HasPrefix(t, "list<"), strings.HasPrefix(t, "set<"), strings.HasPrefix(t, "map<"),
		strings.HasPrefix(t, "tuple<"), strings.HasPrefix(t, "frozen<"):
		return model.TypeJSON
	default:
		// text/varchar/ascii/uuid/timeuuid/inet → string.
		return model.TypeString
	}
}

// SplitChunks implements sdk.FullLoader: a single chunk. gocql pages the result
// set automatically, so one scan covers the whole table (token-range chunking is
// a later refinement).
func (c *Connector) SplitChunks(_ context.Context, _ model.Stream) ([]state.Chunk, error) {
	if c.session == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	return []state.Chunk{{}}, nil
}

// ReadChunk implements sdk.FullLoader: streams the whole table.
func (c *Connector) ReadChunk(ctx context.Context, stream model.Stream, _ state.Chunk, emit sdk.RowFunc) error {
	if c.session == nil {
		return fmt.Errorf("connector not set up")
	}
	iter := c.session.Query(fmt.Sprintf("SELECT * FROM %s", qualified(stream))).WithContext(ctx).Iter()
	return scanIter(ctx, iter, emit)
}

// MaxCursor implements sdk.IncrementalReader.
func (c *Connector) MaxCursor(ctx context.Context, stream model.Stream, cursorField string) (string, error) {
	if c.session == nil {
		return "", fmt.Errorf("connector not set up")
	}
	q := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(cursorField), qualified(stream))
	var v any
	if err := c.session.Query(q).WithContext(ctx).Scan(&v); err != nil {
		return "", nil //nolint:nilerr // empty table / unsupported aggregate → no watermark
	}
	if v == nil {
		return "", nil
	}
	return fmt.Sprintf("%v", normalizeValue(v)), nil
}

// ReadIncrement implements sdk.IncrementalReader. Cassandra cannot range-filter a
// non-clustering column without ALLOW FILTERING, which this uses; on large tables
// prefer a clustering-key cursor.
func (c *Connector) ReadIncrement(ctx context.Context, stream model.Stream, cursorField, since string, emit sdk.RowFunc) (string, error) {
	if c.session == nil {
		return "", fmt.Errorf("connector not set up")
	}
	col := quoteIdent(cursorField)
	var iter *gocql.Iter
	if since == "" {
		iter = c.session.Query(fmt.Sprintf("SELECT * FROM %s", qualified(stream))).WithContext(ctx).Iter()
	} else {
		q := fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ALLOW FILTERING", qualified(stream), col)
		iter = c.session.Query(q, coerceCursor(stream, cursorField, since)).WithContext(ctx).Iter()
	}

	newCursor := since
	err := scanIter(ctx, iter, func(ctx context.Context, row sdk.Row) error {
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

// scanIter maps each CQL row (a column→value map) into an engine row.
func scanIter(ctx context.Context, iter *gocql.Iter, emit sdk.RowFunc) error {
	for {
		raw := map[string]any{}
		if !iter.MapScan(raw) {
			break
		}
		row := make(sdk.Row, len(raw))
		for k, v := range raw {
			row[k] = normalizeValue(v)
		}
		if err := emit(ctx, row); err != nil {
			_ = iter.Close()
			return err
		}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("iterate: %w", err)
	}
	return nil
}

// normalizeValue coerces a gocql value into a JSON-friendly lake value.
func normalizeValue(v any) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return string(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case gocql.UUID:
		return x.String()
	case fmt.Stringer:
		return x.String()
	default:
		return v
	}
}

// coerceCursor converts a stored string watermark into the type gocql expects for
// the cursor column; timestamps parse to time, integers to int64, else string.
func coerceCursor(stream model.Stream, cursorField, since string) any {
	col, ok := stream.Schema.Column(cursorField)
	if !ok {
		return since
	}
	switch col.Type {
	case model.TypeTimestamp, model.TypeDate:
		if ts, err := time.Parse(time.RFC3339Nano, since); err == nil {
			return ts
		}
	case model.TypeInt32, model.TypeInt64:
		var n int64
		if _, err := fmt.Sscan(since, &n); err == nil {
			return n
		}
	}
	return since
}

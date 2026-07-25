package oracle

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// mapType maps an Oracle data type (with NUMBER precision/scale) to a lake type.
//
//	NUMBER, scale 0, precision <= 18 (or unspecified) → int64
//	NUMBER, scale > 0, or precision > 18, or plain NUMBER → decimal (exact string)
//	FLOAT (NUMBER-backed)                               → decimal
//	BINARY_FLOAT / BINARY_DOUBLE                        → float64
//	DATE / TIMESTAMP[(n)][ WITH [LOCAL] TIME ZONE]      → timestamp (RFC3339, UTC)
//	RAW / LONG RAW / BLOB                               → string (hex-encoded)
//	VARCHAR2 / NVARCHAR2 / CHAR / NCHAR / CLOB / NCLOB  → string
//	anything else                                       → string
func mapType(dataType string, precision, scale sql.NullInt64) model.DataType {
	t := strings.ToUpper(strings.TrimSpace(dataType))
	switch {
	case t == "NUMBER":
		switch {
		case !scale.Valid:
			// Plain NUMBER (unspecified scale) can hold fractions → exact decimal.
			return model.TypeDecimal
		case scale.Int64 > 0:
			return model.TypeDecimal
		case precision.Valid && precision.Int64 > 18:
			return model.TypeDecimal
		default:
			// scale 0 with precision <= 18 or unspecified (e.g. INTEGER) → int64.
			return model.TypeInt64
		}
	case t == "FLOAT":
		return model.TypeDecimal
	case t == "BINARY_FLOAT", t == "BINARY_DOUBLE":
		return model.TypeFloat64
	case t == "DATE", strings.HasPrefix(t, "TIMESTAMP"):
		return model.TypeTimestamp
	case t == "RAW", t == "LONG RAW", t == "BLOB":
		return model.TypeString
	default:
		return model.TypeString
	}
}

// SplitChunks implements sdk.FullLoader. With a single numeric PK it partitions
// that key's range into arithmetic chunks; otherwise a single open chunk
// (correct, unpartitioned scan).
func (c *Connector) SplitChunks(ctx context.Context, stream model.Stream) ([]state.Chunk, error) {
	if c.db == nil {
		return nil, fmt.Errorf("connector not set up")
	}
	key, ok := integerPK(stream)
	if !ok {
		return []state.Chunk{{}}, nil
	}
	var minV, maxV sql.NullInt64
	q := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", quoteIdent(key), quoteIdent(key), qualified(stream))
	if err := c.db.QueryRowContext(ctx, q).Scan(&minV, &maxV); err != nil {
		return []state.Chunk{{}}, nil //nolint:nilerr // probe failure → single-chunk fallback
	}
	if !minV.Valid || !maxV.Valid {
		return []state.Chunk{{}}, nil
	}
	step := c.cfg.ChunkRows
	var chunks []state.Chunk
	chunks = append(chunks, state.Chunk{Max: strconv.FormatInt(minV.Int64, 10)})
	for lo := minV.Int64; lo <= maxV.Int64; lo += step {
		chunks = append(chunks, state.Chunk{Min: strconv.FormatInt(lo, 10), Max: strconv.FormatInt(lo+step, 10)})
	}
	chunks[len(chunks)-1].Max = ""
	return chunks, nil
}

// ReadChunk implements sdk.FullLoader: streams one keyset range.
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

// chunkQuery builds the half-open keyset scan with positional binds (:1, :2).
func chunkQuery(stream model.Stream, chunk state.Chunk) (string, []any) {
	table := qualified(stream)
	key, ok := integerPK(stream)
	if !ok || (chunk.Min == "" && chunk.Max == "") {
		return fmt.Sprintf("SELECT * FROM %s", table), nil
	}
	k := quoteIdent(key)
	switch {
	case chunk.Min == "":
		return fmt.Sprintf("SELECT * FROM %s WHERE %s < :1", table, k), []any{chunk.Max}
	case chunk.Max == "":
		return fmt.Sprintf("SELECT * FROM %s WHERE %s >= :1", table, k), []any{chunk.Min}
	default:
		return fmt.Sprintf("SELECT * FROM %s WHERE %s >= :1 AND %s < :2", table, k, k), []any{chunk.Min, chunk.Max}
	}
}

// scanRows maps a *sql.Rows cursor into engine rows, normalizing values.
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
	return rows.Err()
}

// normalizeValue coerces a go-ora scan value into a JSON-friendly lake value.
// Scanned into interface{}, go-ora returns every NUMBER (and text type) as a Go
// string, binary types (RAW/BLOB) as []byte, and DATE/TIMESTAMP as time.Time.
// Integer PKs are parsed to int64; NUMBER decimals keep their exact string form
// so no precision is lost; binary is hex-encoded; timestamps render RFC3339 UTC.
func normalizeValue(v any, lake model.DataType) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		return hex.EncodeToString(x)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case string:
		if lake == model.TypeInt32 || lake == model.TypeInt64 {
			if n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64); err == nil {
				return n
			}
		}
		return x
	case float64:
		if lake == model.TypeDecimal {
			return strconv.FormatFloat(x, 'f', -1, 64)
		}
		return x
	case int64:
		if lake == model.TypeDecimal {
			return strconv.FormatInt(x, 10)
		}
		return x
	default:
		return v
	}
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

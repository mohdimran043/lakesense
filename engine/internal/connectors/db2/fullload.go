//go:build db2

package db2

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

// mapType maps a Db2 SYSCAT.COLUMNS TYPENAME to a lake type.
//
//	SMALLINT / INTEGER / BIGINT                 → int64
//	DECIMAL / NUMERIC / DECFLOAT                → decimal (exact string)
//	REAL / FLOAT / DOUBLE                       → float64
//	CHARACTER / VARCHAR / CLOB / GRAPHIC / …    → string
//	DATE / TIME / TIMESTAMP                     → timestamp (RFC3339, UTC)
//	BLOB                                        → string (hex-encoded)
//	anything else                               → string
func mapType(typeName string) model.DataType {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "SMALLINT", "INTEGER", "INT", "BIGINT":
		return model.TypeInt64
	case "DECIMAL", "NUMERIC", "DECFLOAT":
		return model.TypeDecimal
	case "REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION":
		return model.TypeFloat64
	case "DATE", "TIME", "TIMESTAMP":
		return model.TypeTimestamp
	case "BLOB":
		return model.TypeString
	default:
		// CHARACTER/CHAR/VARCHAR/CLOB/GRAPHIC/VARGRAPHIC/DBCLOB/XML → string.
		return model.TypeString
	}
}

// SplitChunks implements sdk.FullLoader. With a single integer PK it partitions
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

// chunkQuery builds the half-open keyset scan with `?` positional binds.
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

// scanRows maps a *sql.Rows cursor into engine rows, normalizing values.
func scanRows(ctx context.Context, rows *sql.Rows, stream model.Stream, emit sdk.RowFunc) error {
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("read columns: %w", err)
	}
	lake := make([]model.DataType, len(cols))
	hexEnc := make([]bool, len(cols))
	for i, name := range cols {
		if col, ok := stream.Schema.Column(name); ok {
			lake[i] = col.Type
			hexEnc[i] = strings.EqualFold(strings.TrimSpace(col.SourceType), "BLOB")
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
			row[name] = normalizeValue(cells[i], lake[i], hexEnc[i])
		}
		if err := emit(ctx, row); err != nil {
			return err
		}
	}
	return rows.Err()
}

// normalizeValue coerces a go_ibm_db scan value into a JSON-friendly lake value.
// go_ibm_db returns TIMESTAMP/DATE/TIME as time.Time, BLOB as []byte, and most
// numeric/text types as strings. Integer PKs are parsed to int64; DECIMAL keeps
// its exact string so no precision is lost; BLOB is hex-encoded; timestamps
// render RFC3339 UTC.
func normalizeValue(v any, lake model.DataType, hexEnc bool) any {
	switch x := v.(type) {
	case nil:
		return nil
	case []byte:
		if hexEnc {
			return hex.EncodeToString(x)
		}
		s := string(x)
		return coerceString(s, lake)
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano)
	case string:
		return coerceString(x, lake)
	case float64:
		if lake == model.TypeDecimal {
			return strconv.FormatFloat(x, 'f', -1, 64)
		}
		return x
	case float32:
		return float64(x)
	case int64:
		return normInt(x, lake)
	case int32:
		return normInt(int64(x), lake)
	case int16:
		return normInt(int64(x), lake)
	case int:
		return normInt(int64(x), lake)
	default:
		return v
	}
}

// normInt normalizes any Db2 integer width to int64 (or an exact decimal
// string when the column is DECIMAL-typed), so ID/PK columns are always int64.
func normInt(x int64, lake model.DataType) any {
	if lake == model.TypeDecimal {
		return strconv.FormatInt(x, 10)
	}
	return x
}

// coerceString parses integer-typed values to int64, keeping everything else
// (decimals, text) as its exact string form.
func coerceString(s string, lake model.DataType) any {
	if lake == model.TypeInt32 || lake == model.TypeInt64 {
		if n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64); err == nil {
			return n
		}
	}
	return s
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

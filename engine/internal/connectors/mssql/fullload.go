package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/lakesense/lakesense/engine/internal/state"
)

// mapType maps a SQL Server data type to a lake type.
func mapType(dataType string) model.DataType {
	switch strings.ToLower(strings.TrimSpace(dataType)) {
	case "tinyint", "smallint", "int":
		return model.TypeInt32
	case "bigint":
		return model.TypeInt64
	case "bit":
		return model.TypeBool
	case "decimal", "numeric", "money", "smallmoney":
		return model.TypeDecimal
	case "real":
		return model.TypeFloat32
	case "float":
		return model.TypeFloat64
	case "date":
		return model.TypeDate
	case "datetime", "datetime2", "smalldatetime", "datetimeoffset", "time":
		return model.TypeTimestamp
	case "binary", "varbinary", "image", "rowversion", "timestamp":
		return model.TypeBinary
	default:
		// char/varchar/nchar/nvarchar/text/ntext/uniqueidentifier/xml → string.
		return model.TypeString
	}
}

// SplitChunks implements sdk.FullLoader. With a single-column integer PK it
// partitions that key's range into arithmetic chunks; otherwise a single open
// chunk (correct, unpartitioned scan).
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

func chunkQuery(stream model.Stream, chunk state.Chunk) (string, []any) {
	table := qualified(stream)
	key, ok := integerPK(stream)
	if !ok || (chunk.Min == "" && chunk.Max == "") {
		return fmt.Sprintf("SELECT * FROM %s", table), nil
	}
	k := quoteIdent(key)
	switch {
	case chunk.Min == "":
		return fmt.Sprintf("SELECT * FROM %s WHERE %s < @p1", table, k), []any{chunk.Max}
	case chunk.Max == "":
		return fmt.Sprintf("SELECT * FROM %s WHERE %s >= @p1", table, k), []any{chunk.Min}
	default:
		return fmt.Sprintf("SELECT * FROM %s WHERE %s >= @p1 AND %s < @p2", table, k, k), []any{chunk.Min, chunk.Max}
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

// normalizeValue coerces a go-mssqldb scan value into a JSON-friendly lake value.
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

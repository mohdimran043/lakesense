package syncrun

import (
	"encoding/json"

	"github.com/lakesense/lakesense/engine/internal/model"
	"github.com/lakesense/lakesense/engine/internal/sdk"
	"github.com/parquet-go/parquet-go"
)

// buildParquetSchema maps a stream's lake schema (data columns + engine _ls_
// metadata) onto a flat, all-optional Parquet schema. parquet-go orders Group
// fields alphabetically, so the returned names slice — taken from the built
// schema — is the authoritative column order for row assembly.
func buildParquetSchema(stream model.Stream) (*parquet.Schema, []string) {
	group := parquet.Group{}
	for _, c := range stream.Schema.Columns {
		group[c.Name] = parquet.Optional(parquet.Leaf(parquetType(c.Type)))
	}
	schema := parquet.NewSchema(sanitizeName(stream.ID()), group)
	cols := schema.Columns()
	names := make([]string, len(cols))
	for i, path := range cols {
		names[i] = path[len(path)-1]
	}
	return schema, names
}

// sanitizeName keeps a schema's root name well-formed; the stream id "ns.name"
// is fine, but an empty namespace/name would not be.
func sanitizeName(id string) string {
	if id == "" || id == "." {
		return "stream"
	}
	return id
}

// parquetType maps a lake type to a Parquet physical type. Types without a
// native Parquet primitive (decimal, timestamp, date, json, array) are stored
// as UTF8 strings in v0.1 to preserve exactness; documented in the design.
func parquetType(t model.DataType) parquet.Type {
	switch t {
	case model.TypeBool:
		return parquet.BooleanType
	case model.TypeInt32:
		return parquet.Int32Type
	case model.TypeInt64:
		return parquet.Int64Type
	case model.TypeFloat32:
		return parquet.FloatType
	case model.TypeFloat64:
		return parquet.DoubleType
	case model.TypeBinary:
		return parquet.ByteArrayType
	default: // string, decimal, date, timestamp, json, array
		return parquet.ByteArrayType
	}
}

// rowToParquet builds a parquet.Row of length n, placing each column's value at
// its schema index. A nil/absent value becomes a null at definition level 0;
// present values are normalized to the Go kind matching their leaf type.
func rowToParquet(row sdk.Row, colIndex map[string]int, colType map[string]model.DataType, n int) parquet.Row {
	pr := make(parquet.Row, n)
	for name, idx := range colIndex {
		v, present := row[name]
		if !present || v == nil {
			pr[idx] = parquet.NullValue().Level(0, 0, idx)
			continue
		}
		pr[idx] = parquet.ValueOf(normalizeValue(v, colType[name])).Level(0, 1, idx)
	}
	return pr
}

// normalizeValue coerces a Go value read from a connector into the concrete Go
// type the column's Parquet leaf expects, so parquet.ValueOf boxes it into the
// right kind.
func normalizeValue(v any, t model.DataType) any {
	switch t {
	case model.TypeBool:
		b, _ := v.(bool)
		return b
	case model.TypeInt32:
		return int32(toInt64(v))
	case model.TypeInt64:
		return toInt64(v)
	case model.TypeFloat32:
		return float32(toFloat64(v))
	case model.TypeFloat64:
		return toFloat64(v)
	case model.TypeBinary:
		if b, ok := v.([]byte); ok {
			return b
		}
		return []byte(toString(v))
	default:
		return toString(v) // string, decimal, date, timestamp, json, array
	}
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case int32:
		return int64(x)
	case float64:
		return int64(x)
	case json.Number:
		i, _ := x.Int64()
		return i
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	default:
		return 0
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// parquetToRow reconstructs an sdk.Row from a raw parquet.Row using the schema's
// column order. Values are decoded to the Go types the digest and JSON encoder
// expect (int64, float64, bool, string).
func parquetToRow(pr parquet.Row, names []string) sdk.Row {
	row := make(sdk.Row, len(names))
	for _, v := range pr {
		idx := v.Column()
		if idx < 0 || idx >= len(names) {
			continue
		}
		name := names[idx]
		if v.IsNull() {
			row[name] = nil
			continue
		}
		switch v.Kind() {
		case parquet.Boolean:
			row[name] = v.Boolean()
		case parquet.Int32:
			row[name] = int64(v.Int32())
		case parquet.Int64:
			row[name] = v.Int64()
		case parquet.Float:
			row[name] = float64(v.Float())
		case parquet.Double:
			row[name] = v.Double()
		default:
			row[name] = v.String()
		}
	}
	return row
}

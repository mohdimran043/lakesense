package sqlite

import (
	"strings"

	"github.com/lakesense/lakesense/engine/internal/model"
)

// mapType maps a SQLite declared column type to a lake type using SQLite's
// type-affinity rules (https://sqlite.org/datatype3.html §3.1). SQLite is
// dynamically typed, so the declared type is only a hint; the affinity order
// below matches SQLite's own determination.
func mapType(declared string) model.DataType {
	t := strings.ToUpper(strings.TrimSpace(declared))
	switch {
	case t == "":
		// No declared type → BLOB affinity, but in practice untyped columns
		// hold text/ints; string is the safe, lossless lake landing type.
		return model.TypeString
	case strings.Contains(t, "INT"):
		return model.TypeInt64
	case strings.Contains(t, "CHAR"), strings.Contains(t, "CLOB"), strings.Contains(t, "TEXT"):
		return model.TypeString
	case strings.Contains(t, "BLOB"):
		return model.TypeBinary
	case strings.Contains(t, "REAL"), strings.Contains(t, "FLOA"), strings.Contains(t, "DOUB"): //nolint:misspell // "DOUB" is SQLite's REAL-affinity substring rule, not a typo
		return model.TypeFloat64
	case strings.Contains(t, "BOOL"):
		return model.TypeBool
	case strings.Contains(t, "DECIMAL"), strings.Contains(t, "NUMERIC"):
		return model.TypeDecimal
	case t == "DATE":
		return model.TypeDate
	case strings.Contains(t, "TIMESTAMP"), strings.Contains(t, "DATETIME"), strings.Contains(t, "TIME"):
		return model.TypeTimestamp
	case strings.Contains(t, "JSON"):
		return model.TypeJSON
	default:
		return model.TypeString
	}
}

// normalizeValue converts a modernc/sqlite scan value into a JSON-friendly
// lake value. modernc returns int64, float64, string, []byte, or nil. Byte
// slices from TEXT/JSON columns are rendered as strings so the destination
// (and checksum) sees readable, deterministic values; genuine BLOBs keep their
// bytes (JSON-encoded as base64 by the writer).
func normalizeValue(v any, lake model.DataType) any {
	b, ok := v.([]byte)
	if !ok {
		return v
	}
	if lake == model.TypeBinary {
		return b
	}
	return string(b)
}

package postgres

import (
	"fmt"
	"math/big"
	"net/netip"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/lakesense/lakesense/engine/internal/model"
)

// MapType maps a Postgres type name (format_type output) to a lake type.
// Unknown types degrade to string — discovery must never fail on exotica.
// Mapping table documented in docs/analysis/postgres-connector.md §4; unlike
// the reference we map numeric/decimal to TypeDecimal, not lossy float64.
func MapType(pgType string) model.DataType {
	t := strings.ToLower(strings.TrimSpace(pgType))
	// Strip type modifiers: "numeric(10,2)" → "numeric", "varchar(255)" → "varchar".
	if i := strings.IndexByte(t, '('); i > 0 {
		mod := t[i:]
		t = strings.TrimSpace(t[:i])
		// Re-attach nothing; modifiers only matter for decimal precision,
		// which the writer reads from SourceType.
		_ = mod
	}
	if strings.HasSuffix(t, "[]") {
		return model.TypeArray
	}
	switch t {
	case "boolean", "bool":
		return model.TypeBool
	case "smallint", "int2", "integer", "int", "int4", "smallserial", "serial":
		return model.TypeInt32
	case "bigint", "int8", "bigserial":
		return model.TypeInt64
	case "real", "float4":
		return model.TypeFloat32
	case "double precision", "float8":
		return model.TypeFloat64
	case "numeric", "decimal", "money":
		return model.TypeDecimal
	case "date":
		return model.TypeDate
	case "timestamp without time zone", "timestamp with time zone", "timestamp", "timestamptz":
		return model.TypeTimestamp
	case "json", "jsonb":
		return model.TypeJSON
	case "bytea":
		return model.TypeBinary
	default:
		// uuid, text, varchar, char, time, timetz, interval, xml, inet, cidr,
		// macaddr, tsvector, hstore, enums, geometrics, bit, pg_lsn, …
		return model.TypeString
	}
}

// NormalizeValue converts pgx-scanned values into the engine's canonical Go
// representations: nil, bool, int32, int64, float32, float64, string,
// []byte, time.Time, []any, map[string]any. Everything else is stringified.
func NormalizeValue(v any) any {
	switch x := v.(type) {
	case nil, bool, int32, int64, float32, float64, string, []byte, time.Time, map[string]any:
		return x
	case int16:
		return int32(x)
	case int:
		return int64(x)
	case pgtype.Numeric:
		return numericString(x)
	case [16]byte: // uuid
		return fmt.Sprintf("%x-%x-%x-%x-%x", x[0:4], x[4:6], x[6:8], x[8:10], x[10:16])
	case netip.Addr:
		return x.String()
	case netip.Prefix:
		return x.String()
	case pgtype.Time:
		return time.UnixMicro(x.Microseconds).UTC().Format("15:04:05.000000")
	case pgtype.Interval:
		return fmt.Sprintf("%d months %d days %d us", x.Months, x.Days, x.Microseconds)
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			out[i] = NormalizeValue(e)
		}
		return out
	default:
		return fmt.Sprintf("%v", v)
	}
}

// numericString renders a pgtype.Numeric as an exact decimal string.
func numericString(n pgtype.Numeric) any {
	if !n.Valid {
		return nil
	}
	if n.NaN {
		return "NaN"
	}
	if n.InfinityModifier == pgtype.Infinity {
		return "Infinity"
	}
	if n.InfinityModifier == pgtype.NegativeInfinity {
		return "-Infinity"
	}
	if n.Exp >= 0 {
		i := new(big.Int).Set(n.Int)
		i.Mul(i, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(n.Exp)), nil))
		return i.String()
	}
	// Negative exponent: place the decimal point.
	s := new(big.Int).Abs(n.Int).String()
	frac := int(-n.Exp)
	for len(s) <= frac {
		s = "0" + s
	}
	point := len(s) - frac
	out := s[:point] + "." + s[point:]
	if n.Int.Sign() < 0 {
		out = "-" + out
	}
	return out
}

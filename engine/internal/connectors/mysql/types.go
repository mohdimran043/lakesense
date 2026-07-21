package mysql

import (
	"strings"

	"github.com/lakesense/lakesense/engine/internal/model"
)

// mapType maps a MySQL column type (the DATA_TYPE from information_schema, e.g.
// "int", "varchar", "datetime") to a lake type. Matching is by the base type
// name, lowercased; length/precision qualifiers are ignored.
func mapType(dataType string) model.DataType {
	t := strings.ToLower(strings.TrimSpace(dataType))
	switch t {
	case "tinyint", "smallint", "mediumint", "int", "integer", "year":
		return model.TypeInt32
	case "bigint":
		return model.TypeInt64
	case "bit", "bool", "boolean":
		return model.TypeBool
	case "decimal", "numeric", "dec", "fixed":
		return model.TypeDecimal
	case "float":
		return model.TypeFloat32
	case "double", "double precision", "real":
		return model.TypeFloat64
	case "date":
		return model.TypeDate
	case "datetime", "timestamp":
		return model.TypeTimestamp
	case "json":
		return model.TypeJSON
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return model.TypeBinary
	default:
		// char/varchar/text families, enum, set, time, geometry, and anything
		// unrecognized land as string — lossless and safe.
		return model.TypeString
	}
}

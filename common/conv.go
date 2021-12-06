package common

import (
	"database/sql/driver"
	"reflect"
	"strconv"
	"time"
)

// NamedValueToMapString converts the arguments to a statement from the datastruct driver.NamedValue
// defined by sql.Driver to map[string]interface, which is expected by immuclient.
func NamedValueToMapString(args []driver.NamedValue) map[string]interface{} {
	res := make(map[string]interface{})
	for _, namedValue := range args {
		name := namedValue.Name
		if name == "" {
			// Positional arguments get names assigned
			// following the schema "paramORDINAL" with ORDINAL being an integer.
			// See embedded/sql/sql_parser.go and embedded/sql/sql_grammar.y
			// for details how the parameters are parsed and named.
			name = "param" + strconv.Itoa(namedValue.Ordinal)
		}
		res[name] = namedValue.Value
	}
	return res
}

// ColumnTypeScanType returns the type of a go value into which a value of Type SQLValueType can be scanned.
func ColumnTypeScanType(sqlValueType string) reflect.Type {
	switch sqlValueType {
	case "INTEGER":
		return reflect.TypeOf(int64(0))
	case "BOOLEAN":
		return reflect.TypeOf(false)
	case "VARCHAR":
		return reflect.TypeOf("")
	case "BLOB":
		return reflect.TypeOf([]byte{})
	case "TIMESTAMP":
		return reflect.TypeOf(time.Time{})
	// These cases should not be reached.
	// Nevertheless []byte should be safe default for scanning values.
	case "ANY":
		fallthrough
	default:
		return reflect.TypeOf([]byte{})
	}
}

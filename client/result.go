package client

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/tauu/immusql/common"
)

// result contains the data reported by immudb after executing a statement.
type result struct {
	data *schema.SQLExecResult
}

// -- Result interface --

// LastInsertId returns the id of the last row insterted by a statement.
func (r result) LastInsertId() (int64, error) {
	if r.data == nil {
		return -1, nil
	}
	// If there is exactly one auto increment id, that one is returned.
	if len(r.data.LastInsertedPKs) == 1 {
		for _, id := range r.data.LastInsertedPKs {
			if id == nil {
				return -1, nil
			}
			// The primary key should always be an integer.
			return id.GetN(), nil
		}
	}
	// Immudb does not support auto increment columns at the moment,
	// therefore this will never be set.
	return -1, nil
}

// RowsAffected returns the number of rows affected by executing a statement.
func (r result) RowsAffected() (int64, error) {
	if r.data == nil {
		return 0, nil
	}
	return int64(r.data.UpdatedRows), nil
}

// rows contains the rows retrieved by immudb after executing a query.
type rows struct {
	data  *schema.SQLQueryResult
	index int
}

// -- Rows interface --

// Columns returns the name of the columns of the rows.
func (r *rows) Columns() []string {
	// Retrieve the columns in the query result.
	immudbCols := r.data.GetColumns()
	// Create a sting array and insert all names into it.
	columns := make([]string, len(immudbCols))
	for i, col := range immudbCols {
		columns[i] = col.Name
	}
	return columns
}

// Close closes the query result iterator.
func (r *rows) Close() error {
	// Currently there is nothing to close,
	// as immudb will return all data at once.
	return nil
}

// Next returns the next row of the query result.
func (r *rows) Next(dest []driver.Value) error {
	// Get the rows.
	rows := r.data.GetRows()
	// Check if the last row has already been read.
	if r.index >= len(rows) {
		return io.EOF
	}
	// Write value to destination slice.
	row := rows[r.index]
	values := row.GetValues()
	for i, value := range values {
		switch v := value.Value.(type) {
		case *schema.SQLValue_Null:
			dest[i] = v.Null
		case *schema.SQLValue_N:
			dest[i] = v.N
		case *schema.SQLValue_S:
			dest[i] = v.S
		case *schema.SQLValue_B:
			dest[i] = v.B
		case *schema.SQLValue_Bs:
			dest[i] = v.Bs
		}
	}
	// Advance the index.
	r.index = r.index + 1
	return nil
}

// -- RowsColumnTypeDatabaseTypeName interface --

// ColumnTypeDatabaseTypeName returns the type of the index-th column in the result.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	// Retrieve the columns in the query result.
	immudbCols := r.data.GetColumns()
	if index >= len(immudbCols) {
		return ""
	}
	typeName := immudbCols[index].GetType()
	fmt.Printf("typeName: %v\n", typeName)
	return typeName
}

// -- RowsColumnTypeNullable interface --

// ColumnTypeNullable returns if the index-th column in the result is nullable.
func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	r.data.Columns[index].GetType()
	// Immudb does currently not support nullable columns,
	// therefore the result is always false.
	return false, true
}

// -- RowsColumnTypePrecisionScale interface --

// ColumnTypePrecisionScale return the precision and scale of decimal columns.
func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	// Decimal columns are currently not supported by immudb.
	// 0,0,false is the return value for non decimal columns suggested by the sql/driver documentation.
	return 0, 0, false
}

// -- RowsColumnTypeScanType interface --
// ColumnTypeScanType returns the type of a go value into which the value of the index-th column can be scanned.
func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	// Retrieve the columns in the query result.
	immudbCols := r.data.GetColumns()
	if index >= len(immudbCols) {
		return nil
	}
	typeName := immudbCols[index].GetType()
	return common.ColumnTypeScanType(typeName)
}

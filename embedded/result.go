package embedded

import (
	"database/sql/driver"
	"reflect"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/tauu/immusql/common"
)

// result contains the data reported by immudb after executing a statement.
// The embedded engine does not seem to report any useful information,
// after an execution. Therefore this does not store any data at the moment.
type result struct {
	tx          *sql.SQLTx
	committedTx []*sql.SQLTx
}

// -- Result interface --

// LastInsertId returns the id of the last row insterted by a statement.
func (r result) LastInsertId() (int64, error) {
	// If no summary has been set, there is no id available.
	if r.tx == nil {
		return -1, nil
	}
	// If there is exactly one auto increment id, that one is returned.
	pks := r.tx.LastInsertedPKs()
	if len(pks) == 1 {
		for _, id := range pks {
			return id, nil
		}
	}
	// If there are several inserted primary ids,
	// it is not clear, which one should be returned.
	// TODO check if there is way to determine,
	// the very last inserted id.
	return -1, nil
}

// RowsAffected returns the number of rows affected by executing a statement.
func (r result) RowsAffected() (int64, error) {
	// Sum up the updated rows reported by all committed operations.
	count := int64(0)
	for _, tx := range r.committedTx {
		count = count + int64(tx.UpdatedRows())
	}
	// If a new tx is set, also include the updated rows count of it in the total.
	if r.tx != nil {
		count = count + int64(r.tx.UpdatedRows())
	}
	return count, nil
}

// rows contains the rows retrieved by immudb after executing a query.
type rows struct {
	data sql.RowReader
}

// -- Rows interface --

// Columns returns the name of the columns of the rows.
func (r *rows) Columns() []string {
	// Retrieve the columns in the query result.
	immudbCols, err := r.data.Columns()
	// If an error occurred, it cannot be reported using the sql/driver interface.
	// Instead we return an empty array.
	if err != nil {
		return []string{}
	}
	// Create a string array and insert all names into it.
	columns := make([]string, len(immudbCols))
	for i, col := range immudbCols {
		columns[i] = col.Column
	}
	return columns
}

// Close closes the query result iterator.
func (r *rows) Close() error {
	// Currently there is nothing to close,
	// as immudb will return all data at once.
	return r.data.Close()
}

// Next returns the next row of the query result.
func (r *rows) Next(dest []driver.Value) error {
	// Get the rows.
	row, err := r.data.Read()
	if err != nil {
		return err
	}
	// Retrieve the columns in the query result.
	immudbCols, err := r.data.Columns()
	if err != nil {
		return err
	}
	// Write value to destination slice.
	values := row.Values
	for i, col := range immudbCols {
		value := values[col.Selector()]
		switch value.Type() {
		case sql.IntegerType:
			dest[i] = value.Value()
		case sql.VarcharType:
			dest[i] = value.Value()
		case sql.BooleanType:
			dest[i] = value.Value()
		case sql.BLOBType:
			dest[i] = value.Value()
		case sql.TimestampType:
			dest[i] = value.Value()
		case sql.AnyType:
			dest[i] = value.Value()
		}
	}
	return nil
}

// -- RowsColumnTypeDatabaseTypeName interface --

// ColumnTypeDatabaseTypeName returns the type of the index-th column in the result.
func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	// Retrieve the columns in the query result.
	immudbCols, err := r.data.Columns()
	if err != nil {
		return ""
	}
	if index >= len(immudbCols) {
		return ""
	}
	typeName := immudbCols[index].Type
	return typeName
}

// -- RowsColumnTypeNullable interface --

// ColumnTypeNullable returns if the index-th column in the result is nullable.
func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	// Immudb does support nullable columns,
	// but as of now, there does not seem to be a way,
	// to determine if a column can be null from the reader.
	// Therefore the result is always false.
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
	immudbCols, err := r.data.Columns()
	if err != nil {
		return nil
	}
	if index >= len(immudbCols) {
		return nil
	}
	typeName := immudbCols[index].Type
	return common.ColumnTypeScanType(typeName)
}

package embedded

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/tauu/immusql/common"
)

// result contains the data reported by immudb after executing a statement.
// The embedded engine does not seem to report any useful information,
// after an execution. Therefore this does not store any data at the moment.
type result struct {
	previousLastInsertedPKs map[string]int64
	tx                      *sql.SQLTx
	committedTx             []*sql.SQLTx
}

// -- Result interface --

// LastInsertId returns the id of the last row insterted by a statement.
func (r result) LastInsertId() (int64, error) {
	// If no summary has been set, there is no id available.
	if r.tx == nil {
		return -1, nil

	}

	// Load all last inserted PKs.
	// This yields a map, with (table_name => last_inserted_id) pairs.
	// As maps in golang are not ordered in any way, there is no way to determine,
	// which value was inserted last.
	// If the query corresponding to this result is part of a transaction,
	// lastPKs can contain multiple entries. More specifically, the map will
	// contain one value for each table with an autoincrement column, into which
	// a row was inserted during the transaction.
	lastPKs := r.tx.LastInsertedPKs()

	// To determine the one id that was inserted last, we have to consider 2 cases.

	// Case 1: The result does not correspond to a query being part of a transaction.
	// In this there is no stored previous value for lastInsertedPKs.
	// The map should contain one value, if it inserted a value
	// into a table with an autoincrement column.
	if r.previousLastInsertedPKs == nil {
		if len(lastPKs) == 1 {
			// The single value in lastPKs is the last primary key.
			for _, id := range lastPKs {
				return id, nil
			}
		}
		// There is more than one value in LastInsertedPKs().
		// As maps are not ordered in golang, there is no way,
		// to determine which one was inserted last.
		return -1, nil
	}

	// Case 2: The result corresponds to a query that is part of a transaction.
	// Before the query was executed, the value os LastInsertedPKs() was copied
	// into r.previousLastPKs. This value is compared with the current value.
	lastPK := int64(-1)
	foundPK := 0
	for table, id := range lastPKs {
		// Test if a last inserted PK value existed for this table previously.
		previousId, ok := r.previousLastInsertedPKs[table]
		// If no primary key value has been inserted into the table
		// by the query corresponding to this result.
		if !ok {
			lastPK = id
			foundPK++
		}
		// If the current last inserted PK value differs from the
		// previous value for the table, a new primary key has been
		// inserted into the table.
		if ok && previousId != id {
			lastPK = id
			foundPK++
		}
	}
	//fmt.Printf("lastInsertedID: %v, #foundPL: %v", lastPK, foundPK)
	// Only if the comparison above found exactly one possible value
	// for the last inserted PK, we do know its value.
	if foundPK == 1 {
		return lastPK, nil
	}

	// The last inserted PK could not be identified.
	return -1, nil

}

// RowsAffected returns the number of rows affected by executing a statement.
func (r result) RowsAffected() (int64, error) {
	// Sum up the updated rows reported by all committed operations.

	// store previous value of updated rows when the query is executed and then
	// calculate the difference between that and the currento one and return
	// that instance
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
	if errors.Is(err, sql.ErrNoMoreRows) {
		return io.EOF
	}
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

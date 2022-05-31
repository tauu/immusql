package client

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/tauu/immusql/common"
)

// result contains the data reported by immudb after executing a statement.
type result struct {
	data            *schema.SQLExecResult
	previousLastPks map[string]int64
}

// -- Result interface --

// LastInsertId returns the id of the last row insterted by a statement.
func (r result) LastInsertId() (int64, error) {
	fmt.Println("this is data", r.data)
	// if r.data == nil {
	// 	return -1, nil
	// }
	// // Get the last executed transaction.
	// txs := r.data.GetTxs()
	// if len(txs) < 1 {
	// 	return -1, nil
	// }
	// tx := txs[len(txs)-1]
	// // If there was more then one primary key in the last transaction,
	// // it is currently not possible to determine, which was created last.
	// // In this case no PK is returned.
	// lastPks := tx.GetLastInsertedPKs()

	// if r.previousLastPks == nil {
	// 	if len(lastPks) == 1 {
	// 		for _, id := range lastPks {
	// 			return id, nil
	// 		}
	// 	}
	// 	return -1, nil
	// }

	// for table, id := range lastPks {
	// 	previousId, ok := r.previousLastPks[table]
	// 	if !ok {
	// 		return id, nil
	// 	}
	// 	if previousId != id {
	// 		return id, nil
	// 	}
	// }

	// return -1, nil

	// if len(pks) == 1 {
	// 	for _, pk := range pks {
	// 		// If the numeric value of pk is 0, the primary key is not an integer.
	// 		// In this case -1 is returned, as database/sql does not support
	// 		// any other type for primary keys.
	// 		n := pk.GetN()
	// 		if n == 0 {
	// 			return -1, nil
	// 		}
	// 		return n, nil
	// 	}
	// }
	// return -1, nil

	// if r.tx == nil {
	// 	return -1, nil

	// }

	// LastPks := r.tx.LastInsertedPKs()

	// if r.previousLastPks == nil {
	// 	if len(LastPks) == 1 {
	// 		for _, id := range LastPks {
	// 			return id, nil
	// 		}
	// 	}
	// 	return -1, nil
	// }

	// for table, id := range LastPks {
	// 	previousId, ok := r.previousLastPks[table]
	// 	if !ok {
	// 		return id, nil
	// 	}
	// 	if previousId != id {
	// 		return id, nil
	// 	}
	// }

	return -1, nil

}

// RowsAffected returns the number of rows affected by executing a statement.
func (r result) RowsAffected() (int64, error) {
	if r.data == nil {
		return 0, nil
	}
	// Check if there is at least one tx.
	txs := r.data.GetTxs()
	if len(txs) < 1 {
		return 0, nil
	}
	// Create the sum of all affected rows in the txs.
	count := int64(0)
	for _, tx := range txs {
		count = count + int64(tx.GetUpdatedRows())
	}
	return count, nil
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
		name := col.GetName()
		// immudb returns column names enclosed in ( )
		// and also includes the name of the database.
		// database/sql expects only the column name though,
		// and therefore these values have to be stripped.
		name = strings.TrimSuffix(name, ")")
		name = strings.TrimPrefix(name, "(")
		index := strings.LastIndex(name, ".")
		if index > -1 {
			name = name[index+1:]
		}
		columns[i] = name
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
			dest[i] = nil
		case *schema.SQLValue_N:
			dest[i] = v.N
		case *schema.SQLValue_S:
			dest[i] = v.S
		case *schema.SQLValue_B:
			dest[i] = v.B
		case *schema.SQLValue_Bs:
			dest[i] = v.Bs
		case *schema.SQLValue_Ts:
			// Convert micro seconds since epoch into a time value.
			dest[i] = time.UnixMicro(v.Ts)
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
	// Immudb supports nullable columns,
	// but there does not seem to be a way to determine
	// if a column can be nullable using only the result of a query.
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

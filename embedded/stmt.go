package embedded

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/tauu/immusql/common"
)

// Stmt is a prepared SQL statement.
type stmt struct {
	query []sql.SQLStmt
	conn  *immudbEmbedded
}

// -- Stmt interface --

// Close closes the statement.
// This method if required to satisfy the Stmt interface of sql/driver.
func (s *stmt) Close() error {
	// As prepared statements are not really supported
	// right now, there is nothing to do.
	return nil
}

// NumInput is the number of placeholders in the sql query.
// This method if required to satisfy the Stmt interface of sql/driver.
func (s *stmt) NumInput() int {
	// -1 can be returned, if the number of placeholders in unknown.
	// TODO investigate if this number can be determined.
	return -1
}

// Exec executes the statement and returns the result.
// This method if required to satisfy the Stmt interface of sql/driver.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	// This method is deprecated and therefore not implemented.
	return nil, fmt.Errorf("not implemeneted")
}

// Query executes the statement and returns the retrieved rows.
// This method if required to satisfy the Stmt interface of sql/driver.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	// This method is deprecated and therefore not implemented.
	return nil, fmt.Errorf("not implemeneted")
}

// -- StmtExecContext interface --

// ExecContext executes the statement and returns the result.
// This method if required to satisfy the StmtExecContext interface of sql/driver.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Convert arguments to the expected format and execute the query.
	params := common.NamedValueToMapString(args)
	summary, err := s.conn.engine.ExecPreparedStmts(s.query, params, false)
	if err != nil {
		return nil, err
	}
	return result{summary: summary}, nil
}

// -- StmtQueryContext interface --

// QueryContext executes the statement and returns the retrieved rows.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// Convert arguments to the expected format and execute the query.
	params := common.NamedValueToMapString(args)
	if len(s.query) > 1 {
		return nil, ErrQueriedMultipleStatements
	}
	stmt := s.query[0]
	switch q := stmt.(type) {
	case *sql.SelectStmt:
		res, err := s.conn.engine.QueryPreparedStmt(q, params, false)
		if err != nil {
			return nil, err
		}
		return &rows{data: res}, nil
	default:
		return nil, ErrQueriedNonSelectStatement
	}
}

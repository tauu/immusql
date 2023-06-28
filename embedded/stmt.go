package embedded

import (
	"context"
	"database/sql/driver"

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
func (s *stmt) Close() error {
	// As prepared statements are not really supported
	// right now, there is nothing to do.
	return nil
}

// NumInput is the number of placeholders in the sql query.
func (s *stmt) NumInput() int {
	// -1 can be returned, if the number of placeholders in unknown.
	// TODO investigate if this number can be determined.
	return -1
}

// Exec executes the statement and returns the result.
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	// This method is deprecated and therefore not implemented.
	return nil, common.ErrNotImplemented
}

// Query executes the statement and returns the retrieved rows.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	// This method is deprecated and therefore not implemented.
	return nil, common.ErrNotImplemented
}

// -- StmtExecContext interface --

// ExecContext executes the statement and returns the result.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {

	// If the statement is part of a transaction
	// the previous LastInsertedPKs are stored
	// to determine later on, which PKs have been
	// inserted by this statement.
	var previousLastInsertedPKs map[string]int64
	if s.conn.sqlTx != nil {
		lastPKs := s.conn.sqlTx.LastInsertedPKs()
		previousLastInsertedPKs = make(map[string]int64, len(lastPKs))

		for k, v := range lastPKs {
			previousLastInsertedPKs[k] = v
		}
	}

	// Convert arguments to the expected format and execute the query.
	params := common.NamedValueToMapString(args)
	tx, committedTx, err := s.conn.engine.ExecPreparedStmts(context.Background(), s.conn.sqlTx, s.query, params)
	if err != nil {
		return nil, err
	}

	return result{previousLastInsertedPKs: previousLastInsertedPKs, tx: tx, committedTx: committedTx}, nil
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
		res, err := s.conn.engine.QueryPreparedStmt(context.Background(), s.conn.sqlTx, q, params)
		if err != nil {
			return nil, err
		}
		return &rows{data: res}, nil
	default:
		return nil, ErrQueriedNonSelectStatement
	}
}

package client

import (
	"context"
	"database/sql/driver"
	"strconv"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/tauu/immusql/common"
)

// Stmt is a "prepared" SQL statement.
// The native client api does not seem to support prepared statements
// at that the moment, though they seem to be supported using the pgsql interface.
// Therefore currently the query is just stored for later execution.
type stmt struct {
	query string
	conn  *immudbConn
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
	return nil, common.ErrNotImplemented
}

// Query executes the statement and returns the retrieved rows.
// This method if required to satisfy the Stmt interface of sql/driver.
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	// This method is deprecated and therefore not implemented.
	return nil, common.ErrNotImplemented
}

// -- StmtExecContext interface --

// ExecContext executes the statement and returns the result.
// This method if required to satisfy the StmtExecContext interface of sql/driver.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// Convert arguments to the expected format and execute the query.
	params := namedValueToMapString(args)
	// Execute the query as part of the transaction,
	// if there is an active transaction.
	var res *schema.SQLExecResult
	var err error
	if s.conn.tx != nil {
		err = s.conn.tx.SQLExec(ctx, s.query, params)
	} else {
		res, err = s.conn.client.SQLExec(ctx, s.query, params)
	}
	if err != nil {
		return nil, err
	}
	//fmt.Printf("exec response data: %s", res.String())
	return result{data: res}, nil
}

// -- StmtQueryContext interface --

// QueryContext executes the statement and returns the retrieved rows.
// This method if required to satisfy the StmtQueryContext interface of sql/driver.
func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// Convert arguments to the expected format and execute the query.
	params := namedValueToMapString(args)
	// Execute the query as part of the transaction,
	// if there is an active transaction.
	var res *schema.SQLQueryResult
	var err error
	if s.conn.tx != nil {
		res, err = s.conn.tx.SQLQuery(ctx, s.query, params)
	} else {
		res, err = s.conn.client.SQLQuery(ctx, s.query, params, false)
	}
	if err != nil {
		return nil, err
	}
	return &rows{data: res, index: 0}, nil
}

// -- utils --

// namedValueToMapString converts the arguments to a statement from the datastruct driver.NamedValue
// defined by sql.Driver to map[string]interface, which is expected by immuclient.
func namedValueToMapString(args []driver.NamedValue) map[string]interface{} {
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

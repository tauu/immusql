package client

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/codenotary/immudb/pkg/client"
	"github.com/tauu/immusql/common"
)

// immudbConn is a connection to a immudb instance.
type immudbConn struct {
	client client.ImmuClient
	tx     client.Tx
}

// Connect establishes a new connection to an immudb instance.
func Open(ctx context.Context, options *client.Options) (driver.Conn, error) {
	// Connect to immudb.
	c := client.NewClient()
	c = c.WithOptions(options)
	err := c.OpenSession(ctx, []byte(options.Username), []byte(options.Password), options.Database)
	if err != nil {
		return nil, err
	}
	// Create the connection with the just received auth token for the database.
	conn := &immudbConn{
		client: c,
		tx:     nil,
	}
	return conn, nil
}

// -- Conn interface --

// Prepare prepares a sql statement.
func (conn *immudbConn) Prepare(query string) (driver.Stmt, error) {
	return &stmt{query: query}, nil
}

// Begin start a new transaction.
func (conn *immudbConn) Begin() (driver.Tx, error) {
	// This method is not implemented as it is deprecated anyway.
	return nil, common.ErrNotImplemented
}

// Close closes the database connection.
func (conn *immudbConn) Close() error {
	return conn.client.Disconnect()
}

// -- ConnBeginTx interface --
func (conn *immudbConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Abort if another transaction is currently active,
	// as nested transactions are not supported at the moment.
	if conn.tx != nil {
		return nil, common.ErrNestedTxNotSupported
	}
	// Check if the transaction should be read only, which is currently not supported.
	if opts.ReadOnly {
		return nil, common.ErrReadOnlyTxNotSupported
	}
	// Check if an isolation level has been set for the transaction.
	// Immudb does not support setting a specific isolation level,
	// therefore an error has to be returned according to the database/sql documentation.
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, common.ErrIsolationLevelNotSupported
	}
	immuTx, err := conn.client.NewTx(ctx)
	if err != nil {
		return nil, err
	}
	conn.tx = immuTx
	return &tx{conn: conn, ctx: ctx}, nil
}

// -- ConnPrepareContext interface --

// PrepareContext prepares a sql statement.
func (conn *immudbConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return conn.Prepare(query)
}

// -- ExecerContext interface --

// ExecContext executes a statement and returns the result.
func (conn *immudbConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Create a statement and return query result.
	stmt := stmt{
		query: query,
		conn:  conn,
	}
	return stmt.ExecContext(ctx, args)
}

// -- QueryerContext interface --

// QueryContext executes a query and returns the retrieved rows.
// This method if required to satisfy the QueryerContext interface of sql/driver.
func (conn *immudbConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Create a statement and return the query result.
	stmt := stmt{
		query: query,
		conn:  conn,
	}
	return stmt.QueryContext(ctx, args)
}

// -- Pinger interface --

// Ping performs a health check to verify if the connection is still alive.
func (conn *immudbConn) Ping(ctx context.Context) error {
	// Check if the client is connected.
	if !conn.client.IsConnected() {
		return driver.ErrBadConn
	}
	// Perform a health check.
	err := conn.client.HealthCheck(ctx)
	if err != nil {
		return driver.ErrBadConn
	}
	return nil
}

// -- SessionResetter interface --

// ResetSession is called by database/sql before the connection is reused.
// This method if required to satisfy the SessionResetter interface of sql/driver.
func (conn *immudbConn) ResetSession(ctx context.Context) error {
	// Check if the client is connected.
	if !conn.client.IsConnected() {
		return driver.ErrBadConn
	}
	// Currently there is nothing to reset,
	// the connection may immediately be reused.
	return nil
}

// -- ImmuDB custom interface --

// ExistTable checks if a table with the given name exist in the connected database.
func (conn *immudbConn) ExistTable(name string) (bool, error) {
	// Retrieve a list of all tables.
	result, err := conn.client.ListTables(context.Background())
	if err != nil {
		return false, err
	}
	// Check all rows in the result, if one
	// contains the name of queried table.
	rows := result.Rows
	for _, row := range rows {
		if len(row.Values) < 1 {
			return false, nil
		}
		col := row.Values[0]
		if col.GetS() == name {
			return true, nil
		}
	}
	return false, nil
}

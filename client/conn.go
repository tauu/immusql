package client

import (
	"context"
	"database/sql/driver"
	"errors"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/tauu/immusql/common"
	"google.golang.org/grpc/metadata"
)

// immudbConn is a connection to a immudb instance.
type immudbConn struct {
	client client.ImmuClient
	authMD metadata.MD
	tx     *tx
}

// Connect establishes a new connection to an immudb instance.
func Open(ctx context.Context, options *client.Options, dbName string) (driver.Conn, error) {
	// Connect to immudb.
	client, err := client.NewImmuClient(options)
	if err != nil {
		return nil, err
	}
	_, err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}
	loginResponse, err := client.Login(ctx, []byte(options.Username), []byte(options.Password))
	if err != nil {
		return nil, err
	}
	// Create context for selecting a database.
	md := metadata.Pairs("authorization", loginResponse.Token)
	authCtx := metadata.NewOutgoingContext(ctx, md)
	// Select the database.
	db := schema.Database{DatabaseName: dbName}
	dbRes, err := client.UseDatabase(authCtx, &db)
	if err != nil {
		return nil, err
	}
	// Create the connection with the just received auth token for the database.
	conn := &immudbConn{
		client: client,
		authMD: metadata.Pairs("authorization", dbRes.Token),
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
	return nil, errors.New("not implemented")
}

// Close closes the database connection.
func (conn *immudbConn) Close() error {
	return conn.client.Disconnect()
}

// -- ConnBeginTx interface --
func (conn *immudbConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	conn.tx = &tx{conn: conn, parts: []txPart{}}
	return conn.tx, nil
}

// -- ConnPrepareContext interface --

// PrepareContext prepares a sql statement.
func (conn *immudbConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return conn.Prepare(query)
}

// -- ExecerContext interface --

// ExecContext executes a statement and returns the result.
func (conn *immudbConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// If a transaction has been started just append the query.
	if conn.tx != nil {
		conn.tx.add(query, args)
		return nil, nil
	}
	// Execute the query.
	return conn.execContext(ctx, query, args)
}

// -- QueryerContext interface --

// QueryContext executes a query and returns the retrieved rows.
// This method if required to satisfy the QueryerContext interface of sql/driver.
func (conn *immudbConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// If a transaction has been started report and error,
	// as immudb currently only supports transaction with a single request.
	if conn.tx != nil {
		return nil, common.ErrNoQueryInTx
	}
	// Create a statement and return the query result.
	stmt := stmt{
		query:  query,
		client: conn.client,
	}
	return stmt.QueryContext(conn.requestContext(ctx), args)
}

// -- Pinger interface --

// Ping performs a health check to verify if the connection is still alive.
func (conn *immudbConn) Ping(ctx context.Context) error {
	// Check if the client is connected.
	if !conn.client.IsConnected() {
		return driver.ErrBadConn
	}
	// Perform a health check.
	err := conn.client.HealthCheck(conn.requestContext(ctx))
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

// -- util --

// requestContext add authentication data to an existing context.
func (conn *immudbConn) requestContext(ctx context.Context) context.Context {
	return metadata.NewOutgoingContext(ctx, conn.authMD)
}

// execContext executes a statement and returns the result.
// In contrast to the public ExecContext method, this method,
// will exectue the query also if a transaction has been started.
func (conn *immudbConn) execContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Create a statement and return the query result.
	stmt := stmt{
		query:  query,
		client: conn.client,
	}
	return stmt.ExecContext(conn.requestContext(ctx), args)
}

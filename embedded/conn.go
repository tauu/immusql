package embedded

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/tauu/immusql/common"
)

// Config contains the configuration for creating
// an embedded immudb engine.
type Config struct {
	CatalogPath string
	SqlPath     string
}

// immudbEmbedded is a connection to a immudb instance.
type immudbEmbedded struct {
	engine *sql.Engine
	tx     *tx
}

// Connect establishes a new connection to an immudb instance.
func Open(ctx context.Context, config Config, dbName string) (driver.Conn, error) {
	// Open a catalog and data store for the sql engine.
	catalogStore, err := store.Open(config.CatalogPath, store.DefaultOptions())
	if err != nil {
		return nil, err
	}
	dataStore, err := store.Open(config.SqlPath, store.DefaultOptions())
	if err != nil {
		return nil, err
	}
	// Create a sql engine.
	engine, err := sql.NewEngine(catalogStore, dataStore, []byte("sql"))
	if err != nil {
		return nil, err
	}
	// Wait until the engine is ready.
	err = engine.EnsureCatalogReady(make(<-chan struct{}))
	if err != nil {
		return nil, err
	}
	ok, err := engine.ExistDatabase(dbName)
	if err != nil {
		return nil, err
	}
	// Create the database if it does not exist.
	if !ok {
		_, err := engine.ExecStmt("CREATE DATABASE "+dbName, map[string]interface{}{}, false)
		if err != nil {
			return nil, err
		}
	}
	err = engine.UseDatabase(dbName)
	if err != nil {
		return nil, err
	}
	return &immudbEmbedded{engine: engine}, nil
}

// -- Conn interface --

// Prepare prepares a sql statement.
func (conn *immudbEmbedded) Prepare(query string) (driver.Stmt, error) {
	stmts, err := sql.Parse(strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	return &stmt{query: stmts, conn: conn}, nil
}

// Begin start a new transaction.
func (conn *immudbEmbedded) Begin() (driver.Tx, error) {
	// This method is not implemented as it is deprecated anyway.
	return nil, errors.New("not implemented")
}

// Close closes the database connection.
func (conn *immudbEmbedded) Close() error {
	return conn.engine.Close()
}

// -- ConnBeginTx interface --
func (conn *immudbEmbedded) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	conn.tx = &tx{conn: conn, parts: []txPart{}}
	return conn.tx, nil
}

// -- ConnPrepareContext interface --

// PrepareContext prepares a sql statement.
func (conn *immudbEmbedded) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return conn.Prepare(query)
}

// -- ExecerContext interface --

// ExecContext executes a statement and returns the result.
func (conn *immudbEmbedded) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Create a statement and execute it.
	stmts, err := sql.Parse(strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	// If a transaction has been started just append the query.
	if conn.tx != nil {
		conn.tx.add(stmts, args)
		return nil, nil
	}
	stmt := stmt{
		query: stmts,
		conn:  conn,
	}
	// Execute the query.
	return stmt.ExecContext(ctx, args)
}

// -- QueryerContext interface --

// QueryContext executes a query and returns the retrieved rows.
// This method if required to satisfy the QueryerContext interface of sql/driver.
func (conn *immudbEmbedded) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// If a transaction has been started report and error,
	// as immudb currently only supports transactions with a single request.
	if conn.tx != nil {
		return nil, common.ErrNoQueryInTx
	}
	// Create a statement and return the query result.
	stmts, err := sql.Parse(strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	stmt := stmt{
		query: stmts,
		conn:  conn,
	}
	return stmt.QueryContext(ctx, args)
}

// -- Pinger interface --

// Ping performs a health check to verify if the connection is still alive.
func (conn *immudbEmbedded) Ping(ctx context.Context) error {
	// Nothing to do, as there is no database connection.
	return nil
}

// -- SessionResetter interface --

// ResetSession is called by database/sql before the connection is reused.
func (conn *immudbEmbedded) ResetSession(ctx context.Context) error {
	// Currently there is nothing to reset,
	// as there is no session.
	return nil
}

// -- ImmuDB custom interface --

// ExistTable checks if a table with the given name exist in the connected database.
func (conn *immudbEmbedded) ExistTable(name string) (bool, error) {
	db, err := conn.engine.DatabaseInUse()
	if err != nil {
		return false, err
	}
	return db.ExistTable(name), nil
}

// -- util --

// execContext executes a statement and returns the result.
// In contrast to the public ExecContext method, this method,
// will exectue the query also if a transaction has been started.
func (conn *immudbEmbedded) execContext(ctx context.Context, stmts []sql.SQLStmt, args []driver.NamedValue) (driver.Result, error) {
	// Create a statement and return the query result.
	stmt := stmt{
		query: stmts,
		conn:  conn,
	}
	return stmt.ExecContext(ctx, args)
}

package embedded

import (
	"context"
	"database/sql/driver"
	"strings"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/tauu/immusql/common"
)

// immudbEmbedded is a connection to a immudb instance.
type immudbEmbedded struct {
	engine *sql.Engine
	store  *store.ImmuStore
	sqlTx  *sql.SQLTx
}

// Connect establishes a new connection to an immudb instance.
func Open(ctx context.Context, path string, dbName string) (driver.Conn, error) {
	// Open a catalog and data store for the sql engine.
	catalogStore, err := store.Open(path, store.DefaultOptions())
	if err != nil {
		return nil, err
	}
	// dataStore, err := store.Open(config.SqlPath, store.DefaultOptions())
	//if err != nil {
	//	return nil, err
	//}
	// Create a sql engine.
	sqlOpts := sql.DefaultOptions()
	engine, err := sql.NewEngine(catalogStore, sqlOpts)
	if err != nil {
		return nil, err
	}
	// Wait until the engine is ready.
	//err = engine.EnsureCatalogReady(make(<-chan struct{}))
	//if err != nil {
	//	return nil, err
	//}
	catalog, err := engine.Catalog(nil)
	if err != nil {
		return nil, err
	}
	//ok, err := engine.ExistDatabase(dbName)
	ok := catalog.ExistDatabase(dbName)
	// Create the database if it does not exist.
	if !ok {
		_, _, err := engine.Exec("CREATE DATABASE "+dbName, map[string]interface{}{}, nil)
		if err != nil {
			return nil, err
		}
	}
	//err = engine.UseDatabase(dbName)
	err = engine.SetDefaultDatabase(dbName)
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
	return nil, common.ErrNotImplemented
}

// Close closes the database connection.
func (conn *immudbEmbedded) Close() error {
	return conn.store.Close()
}

// -- ConnBeginTx interface --
func (conn *immudbEmbedded) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Abort if another transaction is active,
	// as nested transactions are currently not supported.
	if conn.sqlTx != nil {
		return nil, common.ErrNestedTxNotSupported
	}
	stmt := &sql.BeginTransactionStmt{}
	sqlTx, err := conn.execStmt(stmt)
	if err != nil {
		return nil, err
	}
	conn.sqlTx = sqlTx
	return &tx{conn: conn, ctx: ctx}, nil
}

// -- ConnPrepareContext interface --

// PrepareContext prepares a sql statement.
func (conn *immudbEmbedded) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return conn.Prepare(query)
}

// -- ExecerContext interface --

// ExecContext executes a statement and returns the result.
func (conn *immudbEmbedded) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// Create a statement.
	stmts, err := sql.Parse(strings.NewReader(query))
	if err != nil {
		return nil, err
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
	// Create a statement.
	stmts, err := sql.Parse(strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	stmt := stmt{
		query: stmts,
		conn:  conn,
	}
	// Run it and return the result.
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
	catalog, err := conn.engine.Catalog(nil)
	if err != nil {
		return false, err
	}
	db, err := catalog.GetDatabaseByName(conn.engine.DefaultDatabase())
	if err != nil {
		return false, err
	}
	return db.ExistTable(name), nil
}

// -- util --
// execStmt executes a single statement and returns the new Tx.
func (conn *immudbEmbedded) execStmt(stmt sql.SQLStmt) (*sql.SQLTx, error) {
	stmts := []sql.SQLStmt{stmt}
	sqlTx, _, err := conn.engine.ExecPreparedStmts(stmts, nil, conn.sqlTx)
	return sqlTx, err
}

package embedded

import (
	"context"

	"github.com/codenotary/immudb/embedded/sql"
	"github.com/tauu/immusql/common"
)

// tx
type tx struct {
	conn *immudbEmbedded
	ctx  context.Context
}

// -- Tx interface --

// Commit commits the transaction.
func (t *tx) Commit() error {
	// Verify that the transaction is still active.
	if t.conn.sqlTx == nil {
		return common.ErrTxAlreadyFinished
	}
	// Commit the transaction.
	_, err := t.conn.execStmt(&sql.CommitStmt{})
	return t.finish(err)
}

// Rollback rolls back the transaction.
func (t *tx) Rollback() error {
	// Verify that the transaction is still active.
	if t.conn.sqlTx == nil {
		return common.ErrTxAlreadyFinished
	}
	// Rollback the transaction.
	_, err := t.conn.execStmt(&sql.RollbackStmt{})
	// In any case mark the transaction as finished.
	// Otherwise no further operations can be performed.
	return t.finish(err)
}

// -- helper --

// finish completes the transaction, and informs the connection
// to no longer execute queries in the context of the transaction.
func (t *tx) finish(err error) error {
	// If an error occurred while commiting or rolling back
	// the transaction, it is cancelled to resume regular operation.
	if err != nil {
		err = t.conn.sqlTx.Cancel()
	}
	// If the transaction was completed, remove it from the connection.
	t.conn.sqlTx = nil
	return err
}

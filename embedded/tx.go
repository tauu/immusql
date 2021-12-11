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
	if err == nil {
		t.finish()
	}
	return err
}

// Rollback rolls back the transaction.
func (t *tx) Rollback() error {
	// Verify that the transaction is still active.
	if t.conn.sqlTx == nil {
		return common.ErrTxAlreadyFinished
	}
	// Commit the transaction.
	_, err := t.conn.execStmt(&sql.RollbackStmt{})
	if err == nil {
		t.finish()
	}
	return err
}

// -- helper --

// finish completes the transaction, and informs the connection
// to no longer execute queries in the context of the transaction.
func (t *tx) finish() {
	// If the transaction was completed, remove it from the connection.
	t.conn.sqlTx = nil
}

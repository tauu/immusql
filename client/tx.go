package client

import (
	"context"

	"github.com/tauu/immusql/common"
)

// tx denotes an active transaction.
type tx struct {
	conn *immudbConn
	ctx  context.Context
}

// -- Tx interface --

// Commit commits the transaction.
func (t *tx) Commit() error {
	// Verify that there is an active transaction.
	if t.conn.tx == nil {
		return common.ErrTxAlreadyFinished
	}
	// Commit the transaction.
	_, err := t.conn.tx.Commit(t.ctx)
	t.finish()
	return err
}

// Rollback rolls back the transaction.
func (t *tx) Rollback() error {
	// Verify that there is an active transaction.
	if t.conn.tx == nil {
		return common.ErrTxAlreadyFinished
	}
	// Rollback the transaction.
	err := t.conn.tx.Rollback(t.ctx)
	t.finish()
	return err
}

// finish completes the transaction, and informs the connection
// to no longer execute queries in the context of the transaction.
func (t *tx) finish() {
	// If the transaction was completed, remove it from the connection.
	t.conn.tx = nil
}

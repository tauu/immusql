package client

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
)

// tx
type tx struct {
	conn  *immudbConn
	parts []txPart
}

type txPart struct {
	query string
	args  []driver.NamedValue
}

// -- Tx interface --

// Commit commits the transaction.
func (t *tx) Commit() error {
	// Combine all queries into a single query
	// starting with BEGIN TRANSACTION
	// and ending with COMMIT.
	query := "BEGIN TRANSACTION"
	// The arguments of all parts are also combined.
	args := []driver.NamedValue{}
	argNames := make(map[string]bool)
	ordinalShift := 0
	for _, part := range t.parts {
		// Parts of the transaction are seperated by ';'.
		query = query + " " + part.query + ";"
		for _, arg := range part.args {
			if arg.Name != "" {
				// Check if an argument with this name has been used before.
				if _, ok := argNames[arg.Name]; ok {
					return fmt.Errorf("the named argument %s was specified in two statements, which is currently not possible in a transaction due to limitations of immudb", arg.Name)
				}
				// Store that an argument with the current name exists.
				argNames[arg.Name] = true
			}
			// Shift the ordinal of the argument
			// by the total number of arguments in previous parts
			// so that each argument will have a unique number.
			arg.Ordinal += ordinalShift
			args = append(args, arg)
		}
		ordinalShift = ordinalShift + len(part.args)
	}
	// Remove the last seperator.
	query = strings.TrimSuffix(query, ";")
	// Append the final commit statement.
	query = query + " COMMIT"
	fmt.Printf("transaction: %s\nparams: %v", query, args)
	// Execute the query.
	// The private method has to be used, as the public method would
	// just queue the query.
	_, err := t.conn.execContext(context.Background(), query, args)
	// End the transaction for the connection.
	t.conn.tx = nil
	return err
}

// Rollback rolls back the transaction.
func (t *tx) Rollback() error {
	// Currently immudb does only support
	// submitting the whole transaction
	// in one request to the server.
	// Consequently, rolling it back only means
	// not submit it in the first place and
	// nothing needs to be done here.
	// The tx reference is just reset,
	// to indicate that no transaction is going on.
	t.conn.tx = nil
	return nil
}

// -- helper --

// add add a query to the transaction.
func (t *tx) add(query string, args []driver.NamedValue) {
	t.parts = append(t.parts, txPart{query: query, args: args})
}

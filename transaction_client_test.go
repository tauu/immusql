package immusql

import (
	"database/sql"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func openConn() (*sql.DB, error) {

	// Open a connection to immudb
	dsn := "immudb://immudb:immudb@localhost:3322/defaultdb"
	db, err := sql.Open("immudb", dsn)

	return db, err
}

func TestContainerTransaction(t *testing.T) {

	// Open a connection
	db, err := openClientConnection(t)
	if !assert.NoError(t, err, "An error occurred opening connection") {
		t.Skip()
	}
	defer db.Close()

	// Create a new table in the database
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, age INTEGER, isSingle BOOLEAN, PRIMARY KEY id)")
	if !assert.NoError(t, err, "An error occurred creating a table") {
		t.FailNow()
	}

	// Count the amount of rows before the transaction
	rowsBefore, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred querying before transaction")
	}
	defer rowsBefore.Close()

	var countBefore int

	for rowsBefore.Next() {
		if err := rowsBefore.Scan(&countBefore); err != nil {
			log.Error().Err(err).Msg("An error occurred counting rows before transaction")
		}
	}

	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		log.Error().Err(err).Msg("An error occurred beginning the transaction")
	}

	// Insert data to the database
	_, err = tx.Exec("UPSERT INTO test(id, name, age, isSingle) VALUES(?, ?, ?, ?)", 1, "Maria", 40, false)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while inserting data to the database (1st record)")

	}

	// Insert data to the database
	_, err = tx.Exec("UPSERT INTO tests(id, name, age, isSingle) VALUES(?, ?, ?, ?)", 2, "Marc", 22, true)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while inserting data to the database (2nd record)")

	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Error().Err(err).Msg("An error occurred committing the transaction")
	}

	// Count the amount of rows after the transaction succeeded
	rowsAfter, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred querying after transaction")
	}
	defer rowsAfter.Close()

	var countAfter int

	for rowsAfter.Next() {
		if err := rowsAfter.Scan(&countAfter); err != nil {
			log.Error().Err(err).Msg("An error occurred counting rows after transaction")
		}
	}

	// Test cases
	assert.Equal(t, countBefore, countAfter, "Both values should be the same as there was an intended mistake while inserting in the database")

}
package immusql

import (
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func TestTransactionSuccess(t *testing.T) {

	// Open a connection
	db, err := openConnection()
	if !assert.NoError(t, err, "An error occurred openning connection") {
		t.FailNow()
	}

	// Deletes the test directory
	defer cleanUpEmbedded()

	// Create a new table in the database
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, age INTEGER, isSingle BOOLEAN, PRIMARY KEY id)")
	if !assert.NoError(t, err, "An error occurred creating a table") {
		t.FailNow()
	}

	// Count the amount of rows before the transaction
	rowsBefore, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred quering before transaction")
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
	_, err = tx.Exec("INSERT INTO test(name, age, isSingle) VALUES(?, ?, ?)", "Maria", 40, false)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while inserting data to the database (1st record)")

	}

	// Insert data to the database
	_, err = tx.Exec("INSERT INTO test(name, age, isSingle) VALUES(?, ?, ?)", "Marc", 44, true)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while inserting data to the database (2nd record)")

	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Error().Err(err).Msg("An error occurred commiting the transaction")
	}

	// Count the amount of rows after the transaction succeeded
	rowsAfter, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred quering after transaction")
	}
	defer rowsAfter.Close()

	var countAfter int

	for rowsAfter.Next() {
		if err := rowsAfter.Scan(&countAfter); err != nil {
			log.Error().Err(err).Msg("An error occurred counting rows after transaction")
		}
	}

	// Test cases
	assert.Equal(t, countBefore+2, countAfter, "An error happened in the transaction")

	type user struct {
		id       int
		name     string
		age      int
		isSingle bool
	}

	// Check that the database connection still works after a successful transaction
	rows, err := db.Query("SELECT * from test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while Quering the database")
	}
	defer rows.Close()

	users := []user{}
	for rows.Next() {
		var u user
		err := rows.Scan(&u.id, &u.name, &u.age, &u.isSingle)
		if err != nil {
			log.Error().Err(err).Msg("An error happened scanning rows")
		}
		users = append(users, u)

	}

	// Verify that the length of users array is 2
	assert.Equal(t, 2, len(users), "An error occurred Quering users")

	// Test cases
	assert.Equal(t, 1, users[0].id, "An error happened parsing the db")
	assert.Equal(t, "Maria", users[0].name, "An error happened parsing the db")
	assert.Equal(t, 40, users[0].age, "An error happened parsing the db")
	assert.Equal(t, false, users[0].isSingle, "An error happened parsing the db")

	assert.Equal(t, 2, users[1].id, "An error happened parsing the db")
	assert.Equal(t, "Marc", users[1].name, "An error happened parsing the db")
	assert.Equal(t, 44, users[1].age, "An error happened parsing the db")
	assert.Equal(t, true, users[1].isSingle, "An error happened parsing the db")

}

func TestTransactionFail(t *testing.T) {

	// Open a connection
	db, err := openConnection()
	if !assert.NoError(t, err, "An error occurred openning connection") {
		t.FailNow()
	}

	// Deletes the test directory
	defer cleanUpEmbedded()

	// Create a new table in the database
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, age INTEGER, isSingle BOOLEAN, PRIMARY KEY id)")
	if !assert.NoError(t, err, "An error occurred creating a table") {
		t.FailNow()
	}

	// Count the amount of rows before the transaction
	rowsBefore, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred quering before transaction")
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
	_, err = tx.Exec("INSERT INTO test(name, age, isSingle) VALUES(?, ?, ?)", "Maria", 40, false)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while inserting data to the database (1st record)")
	}

	// Insert data to the database
	_, err = tx.Exec("INSERT INTO tests(name, age, isSingle) VALUES(?, ?, ?)", "Marc", 22, true)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while inserting data to the database (2nd record)")
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Error().Err(err).Msg("An error occurred commiting the transaction")
	}

	// Count the amount of rows after the transaction failed
	rowsAfter, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred quering after transaction")
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

func TestTransactionSimple(t *testing.T) {

	// Open a connection
	db, err := openConnection()
	if !assert.NoError(t, err, "An error occurred openning connection") {
		t.FailNow()
	}

	// Deletes the test directory
	defer cleanUpEmbedded()

	// Create a new table in the database
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, age INTEGER, isSingle BOOLEAN, PRIMARY KEY id)")
	if !assert.NoError(t, err, "An error occurred creating a table") {
		t.FailNow()
	}

	// Count the amount of rows before the transaction
	rowsBefore, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred quering before transaction")
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
	_, err = tx.Exec("INSERT INTO test(name, age, isSingle) VALUES(?, ?, ?)", "Maria", 40, false)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while inserting data to the database")
	}

	// Forced Rollback
	tx.Rollback()

	// Count the amount of rows after the transaction succeeded
	rowsAfter, err := db.Query("SELECT COUNT(*) FROM test")
	if err != nil {
		log.Error().Err(err).Msg("An error occurred quering after transaction")
	}
	defer rowsAfter.Close()

	var countAfter int

	for rowsAfter.Next() {
		if err := rowsAfter.Scan(&countAfter); err != nil {
			log.Error().Err(err).Msg("An error occurred counting rows after transaction")
		}
	}

	// Test cases
	assert.Equal(t, countBefore, countAfter, "An error happened in the transaction")

}

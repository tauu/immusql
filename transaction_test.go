package immusql

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransactionSuccess(t *testing.T) {
	runTest(t, func(t *testing.T, db *sql.DB) {

		// Create a new table in the database
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, age INTEGER, isSingle BOOLEAN, PRIMARY KEY id)")
		if !assert.NoError(t, err, "An error occurred creating a table") {
			t.FailNow()
		}

		// Count the amount of rows before the transaction
		rowsBefore, err := db.Query("SELECT COUNT(*) FROM test")
		if !assert.NoError(t, err, "querying for a transaction should not cause an error") {
			t.FailNow()
		}
		defer rowsBefore.Close()

		var countBefore int

		for rowsBefore.Next() {
			err := rowsBefore.Scan(&countBefore)
			assert.NoError(t, err, "counting rows before a transaction should not cause an error")
		}

		// Start a transaction
		tx, err := db.Begin()
		if !assert.NoError(t, err, "beginning a transaction should not cause and error") {
			t.FailNow()
		}

		// Insert data to the database
		_, err = tx.Exec("INSERT INTO test(name, age, isSingle) VALUES(?, ?, ?)", "Maria", 40, false)
		if !assert.NoError(t, err, "inserting data during a transaction should not cause and error") {
			t.FailNow()
		}

		// Insert data to the database
		_, err = tx.Exec("INSERT INTO test(name, age, isSingle) VALUES(?, ?, ?)", "Marc", 44, true)
		if !assert.NoError(t, err, "inserting data during a transaction should not cause and error") {
			t.FailNow()
		}

		// Commit the transaction
		err = tx.Commit()
		if !assert.NoError(t, err, "committing a transaction should not cause and error") {
			t.FailNow()
		}

		// Count the amount of rows after the transaction succeeded
		rowsAfter, err := db.Query("SELECT COUNT(*) FROM test")
		if !assert.NoError(t, err, "querying after a transaction should not cause and error") {
			t.FailNow()
		}
		defer rowsAfter.Close()

		var countAfter int

		for rowsAfter.Next() {
			err := rowsAfter.Scan(&countAfter)
			assert.NoError(t, err, "scanning rows after a transaction should not cause an error")
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
		if !assert.NoError(t, err, "querying database should not cause an error") {
			t.FailNow()
		}
		defer rows.Close()

		users := []user{}
		for rows.Next() {
			var u user
			err := rows.Scan(&u.id, &u.name, &u.age, &u.isSingle)
			assert.NoError(t, err, "scanning errors should not cause an error")
			users = append(users, u)

		}

		// Verify that the length of users array is 2
		if !assert.Equal(t, 2, len(users), "An error occurred Quering users") {
			t.FailNow()
		}

		// Test cases
		assert.Equal(t, 1, users[0].id, "An error happened parsing the db")
		assert.Equal(t, "Maria", users[0].name, "An error happened parsing the db")
		assert.Equal(t, 40, users[0].age, "An error happened parsing the db")
		assert.Equal(t, false, users[0].isSingle, "An error happened parsing the db")

		assert.Equal(t, 2, users[1].id, "An error happened parsing the db")
		assert.Equal(t, "Marc", users[1].name, "An error happened parsing the db")
		assert.Equal(t, 44, users[1].age, "An error happened parsing the db")
		assert.Equal(t, true, users[1].isSingle, "An error happened parsing the db")
	})
}

func TestTransactionFail(t *testing.T) {
	runTest(t, func(t *testing.T, db *sql.DB) {
		// Create a new table in the database
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, age INTEGER, isSingle BOOLEAN, PRIMARY KEY id)")
		if !assert.NoError(t, err, "An error occurred creating a table") {
			t.FailNow()
		}

		// Count the amount of rows before the transaction
		rowsBefore, err := db.Query("SELECT COUNT(*) FROM test")
		if !assert.NoError(t, err, "querying before a transaction should not cause an error") {
			t.FailNow()
		}
		defer rowsBefore.Close()

		var countBefore int

		for rowsBefore.Next() {
			err := rowsBefore.Scan(&countBefore)
			assert.NoError(t, err, "counting rows before a transaction should not cause an error")
		}

		// Start a transaction
		tx, err := db.Begin()
		if !assert.NoError(t, err, "beginning a transaction should not cause an error") {
			t.FailNow()
		}

		// Insert data to the database
		_, err = tx.Exec("INSERT INTO test(name, age, isSingle) VALUES(?, ?, ?)", "Maria", 40, false)
		assert.NoError(t, err, "inserting data during a transaction should not cause an error")

		// Insert data to the database
		_, err = tx.Exec("INSERT INTO tests(name, age, isSingle) VALUES(?, ?, ?)", "Marc", 22, true)
		assert.Error(t, err, "inserting data into a non existing table should cause an error")

		// Commit the transaction
		err = tx.Commit()
		assert.Error(t, err, "committing a transaction after an error occurred did not cause an error")

		// Count the amount of rows after the transaction failed
		rowsAfter, err := db.Query("SELECT COUNT(*) FROM test")
		if !assert.NoError(t, err, "querying after a transaction should no cause an error") {
			t.FailNow()
		}
		defer rowsAfter.Close()

		var countAfter int

		for rowsAfter.Next() {
			err := rowsAfter.Scan(&countAfter)
			assert.NoError(t, err, "counting rows after a transaction should not cause an error")
		}

		// Test cases
		assert.Equal(t, countBefore, countAfter, "Both values should be the same as there was an intended mistake while inserting in the database")
	})
}

func TestTransactionSimple(t *testing.T) {
	runTest(t, func(t *testing.T, db *sql.DB) {
		// Create a new table in the database
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, age INTEGER, isSingle BOOLEAN, PRIMARY KEY id)")
		if !assert.NoError(t, err, "An error occurred creating a table") {
			t.FailNow()
		}

		// Count the amount of rows before the transaction
		rowsBefore, err := db.Query("SELECT COUNT(*) FROM test")
		if !assert.NoError(t, err, "querying before transaction should not cause an error") {
			t.FailNow()
		}
		defer rowsBefore.Close()

		var countBefore int

		for rowsBefore.Next() {
			err := rowsBefore.Scan(&countBefore)
			assert.NoError(t, err, "scanning rows before a transaction should not cause an error")
		}

		// Start a transaction
		tx, err := db.Begin()
		if !assert.NoError(t, err, "beginning a transaction should not cause an error") {
			t.FailNow()
		}

		// Insert data to the database
		_, err = tx.Exec("INSERT INTO test(name, age, isSingle) VALUES(?, ?, ?)", "Maria", 40, false)
		assert.NoError(t, err, "inserting data to the database should not cause an error")

		// Forced Rollback
		tx.Rollback()

		// Count the amount of rows after the transaction succeeded
		rowsAfter, err := db.Query("SELECT COUNT(*) FROM test")
		if !assert.NoError(t, err, "querying data after a transaction should not cause an error") {
			t.FailNow()
		}
		defer rowsAfter.Close()

		var countAfter int

		for rowsAfter.Next() {
			err := rowsAfter.Scan(&countAfter)
			assert.NoError(t, err, "scanning rows after a transaction should not cause an error")
		}

		// Test cases
		assert.Equal(t, countBefore, countAfter, "An error happened in the transaction")
	})
}

package immusql

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

func openConnection() (*sql.DB, error) {

	// Creates a database object
	// http://foo/asdfadf/test
	// "file:///test.html"
	path, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	// URI to storage location for the database.
	// Example format: immudbe:///folderA/folderB/databaseName
	url := url.URL{
		Scheme: "immudbe",
		Path:   path + "/test/testdb",
	}

	db, err := sql.Open("immudb", url.String())
	if err != nil {
		log.Error().Err(err).Msg("An error occurred while opening connection")
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		log.Error().Err(err).Msg("Ping failed")
		return nil, err
	}

	return db, nil
}

func cleanUpEmbedded() {

	// Deletes the test directory
	removeDirError := os.RemoveAll("test")
	if removeDirError != nil {
		log.Error().Err(removeDirError).Msg("an error occurred while deleting test directory")
	}

}

func TestCreateTable(t *testing.T) {

	// Open a connection
	db, err := openConnection()
	if !assert.NoError(t, err, "An error occurred openning connection") {
		t.FailNow()
	}

	// Deletes the test directory
	defer cleanUpEmbedded()

	// Establish an actual connection to the db
	conn, err := db.Conn(context.Background())
	if !assert.NoError(t, err, "retrieving an actual database connection failed") {
		t.FailNow()
	}

	// Check if table exists before creating it
	err = conn.Raw(func(driverConn interface{}) error {
		if v, ok := driverConn.(ImmuDBconn); ok {
			exists, err := v.ExistTable("test")
			assert.NoError(t, err, "calling ExistTable of the driver failed")
			assert.False(t, exists, "table does exist before creating it")
		} else {
			assert.True(t, ok, "driver object of database connection does not satisfiy ImmuDBconn interface")
		}
		return nil
	})
	assert.NoError(t, err, "Checking if table exists failed")

	// Create a new table in the database
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, surname BLOB, age INTEGER, single BOOLEAN, date TIMESTAMP, PRIMARY KEY id)")
	if !assert.NoError(t, err, "An error occurred creating a new table") {
		t.FailNow()
	}

	// Establish an actual connection to the db
	conn, err = db.Conn(context.Background())
	if !assert.NoError(t, err, "retrieving an actual database connection failed") {
		t.FailNow()
	}

	// Check if table exists after creating it
	err = conn.Raw(func(driverConn interface{}) error {
		if v, ok := driverConn.(ImmuDBconn); ok {
			exists, err := v.ExistTable("test")
			assert.NoError(t, err, "calling ExistTable of the driver failed")
			assert.True(t, exists, "table test does not exist after creating it")
		} else {
			assert.True(t, ok, "driver object of database connection does not satisfiy ImmuDBconn interface")
		}
		return nil
	})
	assert.NoError(t, err, "Table creation failed")

}

func TestInsertValues(t *testing.T) {

	// Open a connection
	db, err := openConnection()
	if !assert.NoError(t, err, "An error occurred openning connection") {
		t.FailNow()
	}

	// Deletes the test directory
	defer cleanUpEmbedded()

	// Create a new table in the database
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, surname BLOB, age INTEGER, single BOOLEAN, date TIMESTAMP, PRIMARY KEY id)")
	if !assert.NoError(t, err, "An error occurred creating a new table") {
		t.FailNow()
	}

	dateBeforeMicro := time.Now().UnixMicro()

	// Define variables to Insert
	nameBefore := "Jose"
	surnameBefore := []byte("Roca")
	ageBefore := 33
	singleBefore := true
	dateBefore := time.UnixMicro(dateBeforeMicro).UTC()

	// Insert data in the database
	res, err := db.Exec("INSERT INTO test(name, surname, age, single, date) VALUES(?,?,?,?,?)", nameBefore, surnameBefore, ageBefore, singleBefore, dateBefore)
	if err != nil {
		log.Error().Err(err).Msg("An error occurred inserting data to the database")
	}

	// Check if the data was inserted in the database
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		log.Error().Err(err).Msg("An error occurred checking rows affected")
	}
	assert.Equal(t, rowsAffected, int64(1), "An error occurred reading the database")

	var (
		nameAfter    string
		surnameAfter []byte
		ageAfter     int
		singleAfter  bool
		dateAfter    time.Time
	)

	// Query data previously inserted
	rows, err := db.Query("SELECT name, surname, age, single, date FROM test WHERE name = ?", nameBefore)
	if err != nil {
		log.Error().Err(err).Msg("An error happened while quering data")
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&nameAfter, &surnameAfter, &ageAfter, &singleAfter, &dateAfter)
		if err != nil {
			log.Error().Err(err).Msg("An error happened scanning rows")
		}
	}

	// Tests cases
	assert.Equal(t, nameBefore, nameAfter, "An error ocurred parsing the database")
	assert.Equal(t, surnameBefore, surnameAfter, "An error ocurred parsing the database")
	assert.Equal(t, ageBefore, ageAfter, "An error ocurred parsing the database")
	assert.Equal(t, singleBefore, singleAfter, "An error ocurred parsing the database")
	assert.Equal(t, dateBefore, dateAfter, "An error ocurred parsing the database")

}

package immusql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/server"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openConnection(t *testing.T) (*sql.DB, error) {
	// URI to storage location for the database.
	// Example format: immudbe:///folderA/folderB/databaseName
	//
	// The temp directory from the testing package is automatically cleared
	// after the test by the testing package itself. So no further cleanup is
	// required.
	url := url.URL{
		Scheme: "immudbe",
		Path:   t.TempDir(),
	}

	// Open a connection.
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

func openClientConnection(t *testing.T) (*sql.DB, error) {
	// Create a test server with default options and a random port.
	// For the data storage a temp directory provided by the testing package
	// is used. This directory is automatically cleaned after the test.
	options := server.DefaultOptions().
		WithMetricsServer(false).
		WithWebServer(false).
		WithPgsqlServer(false).
		WithPort(0).
		WithDir(t.TempDir())

	srv := server.DefaultServer().WithOptions(options).(*server.ImmuServer)
	srv.Initialize()

	// Run the test server.
	go func() {
		srv.Start()
	}()

	// Stop the server during test cleanup.
	t.Cleanup(func() { srv.Stop() })

	// Wait up to 500ms for the server to be active.
	active := false
	for i := 0; i < 5; i++ {
		res, err := srv.Health(context.Background(), nil)
		// If the health request failed, wait for 100ms.
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		// Determine the health status and abort if the server is ready.
		active = res.GetStatus()
		if active {
			break
		}
	}
	// Abort if the server is still not ready.
	if !active {
		return nil, errors.New("immudb server did not start")
	}

	// Extract the port on which the server is running.
	port := srv.Listener.Addr().(*net.TCPAddr).Port

	// Build URI to test server for the database.
	// Example format: immudb://immudb:immudb@localhost:3322/defaultdb
	host := fmt.Sprintf("localhost:%d", port)
	url := url.URL{
		Scheme: "immudb",
		User:   url.UserPassword("immudb", "immudb"),
		Host:   host,
		Path:   "defaultdb",
	}

	// Open a connection.
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

func runTest(t *testing.T, testFunc func(*testing.T, *sql.DB)) {
	// Run the test using an embedded engine.
	t.Run("embedded", func(t *testing.T) {
		// Open a connection
		db, err := openConnection(t)
		if !assert.NoError(t, err, "An error occurred opening connection") {
			t.FailNow()
		}
		defer db.Close()
		testFunc(t, db)
	})
	// Run the test using a client connection.
	t.Run("client", func(t *testing.T) {
		db, err := openClientConnection(t)
		if !assert.NoError(t, err, "An error occurred opening connection") {
			t.FailNow()
		}
		defer db.Close()
		testFunc(t, db)
	})
}

func TestCreateTable(t *testing.T) {
	runTest(t, func(t *testing.T, db *sql.DB) {
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

	})
}

func TestInsertValues(t *testing.T) {
	runTest(t, func(t *testing.T, db *sql.DB) {
		// Create a new table in the database
		_, err := db.Exec("CREATE TABLE IF NOT EXISTS test(id INTEGER AUTO_INCREMENT, name VARCHAR, surname BLOB, age INTEGER, single BOOLEAN, date TIMESTAMP, id2 UUID, PRIMARY KEY id)")
		if !assert.NoError(t, err, "An error occurred creating a new table") {
			t.FailNow()
		}

		// Define variables to Insert
		nameBefore := "Jose"
		surnameBefore := []byte("Roca")
		ageBefore := 33
		singleBefore := true
		dateBefore := time.Now()
		id2Before, err := uuid.NewRandom()
		require.NoError(t, err, "creating a random uuid should not fail")

		// Insert data in the database
		res, err := db.Exec("INSERT INTO test(name, surname, age, single, date, id2) VALUES(?,?,?,?,?,?::UUID)", nameBefore, surnameBefore, ageBefore, singleBefore, dateBefore, id2Before)
		require.NoError(t, err, "An error occurred inserting data to the database")

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
			id2After     uuid.UUID
		)

		// Query data previously inserted
		rows, err := db.Query("SELECT name, surname, age, single, date, id2 FROM test WHERE name = ?", nameBefore)
		if err != nil {
			log.Error().Err(err).Msg("An error happened while querying data")
		}
		defer rows.Close()

		for rows.Next() {
			err := rows.Scan(&nameAfter, &surnameAfter, &ageAfter, &singleAfter, &dateAfter, &id2After)
			if err != nil {
				log.Error().Err(err).Msg("An error happened scanning rows")
			}
		}

		// Tests cases
		assert.Equal(t, nameBefore, nameAfter, "An error ocurred parsing the database")
		assert.Equal(t, surnameBefore, surnameAfter, "An error ocurred parsing the database")
		assert.Equal(t, ageBefore, ageAfter, "An error ocurred parsing the database")
		assert.Equal(t, singleBefore, singleAfter, "An error ocurred parsing the database")
		// Comparing times directly does not work, as timestamps also contains a
		// monotonic clock reading, which is never stored in the database.
		assert.True(t, dateBefore.Equal(dateAfter), "An error ocurred parsing the database")
		assert.Equal(t, time.Local, dateAfter.Location(), "The database should by default always return local times")
		assert.Equal(t, id2Before, id2After, "An error ocurred parsing the database")

	})
}

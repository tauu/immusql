package immusql

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnectionResetClient(t *testing.T) {
	// Open the database.
	db, err := openConn()
	if !assert.NoError(t, err, "An error occurred opening connection") {
		t.Skip()
	}
	// Limit the maximum number of open connections.
	db.SetMaxOpenConns(4)
	// Create more concurrent request than the maximum number of open
	// connections. This will force connections being closed and reset.
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err = db.Ping()
			assert.NoError(t, err, "ping after opening the connection failed")
		}()
	}
	wg.Wait()
}

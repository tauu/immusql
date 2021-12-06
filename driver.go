package immusql

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// Init registers the driver for the immudb database.
func init() {
	driver := &ImmudbDriver{}
	sql.Register("immudb", driver)
}

// ImmudbDriver is the basis of the database/sql driver.
type ImmudbDriver struct{}

// -- Driver interface --

// Open a new connection to immudb.
func (driver *ImmudbDriver) Open(dsn string) (driver.Conn, error) {
	connector, err := driver.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// -- DriverContext interface --

// OpenConnector creates a connector for opening connections to a immudb.
func (driver *ImmudbDriver) OpenConnector(dsn string) (*connector, error) {
	config, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{config: config}, nil
}

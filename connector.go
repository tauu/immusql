package immusql

import (
	"context"
	"database/sql/driver"

	"github.com/codenotary/immudb/pkg/client"
	driverClient "github.com/tauu/immusql/client"
	"github.com/tauu/immusql/embedded"
)

// connector opens connections to a preconfigured immudb instance.
type connector struct {
	config dsnConfig
	driver ImmudbDriver
}

// -- Connector interface --

// Connect establishes a new connection to an immudb instance.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.config.Embedded {
		return c.openEmbedded(ctx)
	}
	return c.openClient(ctx)
}

// Driver returns the driver used by the connector.
func (c *connector) Driver() *ImmudbDriver {
	return &c.driver
}

// openClient opens an immuclient connection to an immudb.
func (c *connector) openClient(ctx context.Context) (driver.Conn, error) {
	// Assemble options for connecting to immudb.
	options := client.DefaultOptions().
		WithAddress(c.config.Host).
		WithPort(c.config.Port).
		WithDatabase(c.config.Name)
	// Set username and password.
	if c.config.User != "" {
		options = options.WithUsername(c.config.User).
			WithPassword(c.config.Pass)
	}
	return driverClient.Open(ctx, options)
}

// openEmbedded creates an embedded immudb engine.
func (c *connector) openEmbedded(ctx context.Context) (driver.Conn, error) {
	// Open an engine for it.
	return embedded.Open(ctx, c.config.Path, c.config.Name)
}

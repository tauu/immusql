package immusql

import (
	"github.com/codenotary/immudb/pkg/client"
	"github.com/tauu/immusql/common"
)

// dsnConfig contains the connection parameters
// parsed from a dsn string.
type dsnConfig struct {
	User     string
	Pass     string
	Host     string
	Port     int
	Path     string
	Name     string
	Embedded bool
}

// mtlsOptions stores registered mtlsOptions
// for ImmuDB client connections.
var mtlsOptions map[string]client.MTLsOptions

// RegisterTLSoptions registers a tls configuration to be sued for a specific domain name.
func RegisterTLSoptions(dns string, options client.MTLsOptions) error {
	// Create map if it does not exist.
	if mtlsOptions == nil {
		mtlsOptions = make(map[string]client.MTLsOptions)
	}
	// Check if a configuration with this name was already registered.
	if _, ok := mtlsOptions[dns]; ok {
		return common.ErrConfigAlreadyRegistered
	}
	// Store the configuration.
	mtlsOptions[dns] = options
	return nil
}

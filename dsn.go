package immusql

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// parseDSN parses a dsn specifying a immudb database
// into a configuration for connecting to it.
func parseDSN(dsn string) (dsnConfig, error) {
	conf := dsnConfig{}
	url, err := url.Parse(dsn)
	if err != nil {
		return conf, fmt.Errorf("parsing dsn %s failed: %v", dsn, err)
	}
	if url.Scheme != "immudb" && url.Scheme != "immudbe" {
		return conf, fmt.Errorf("scheme of dsn is %s but has to be immudb or immudbe", url.Scheme)
	}
	// The immudbe scheme indicates that an embedded engine should be created.
	if url.Scheme == "immudbe" {
		conf.Embedded = true
	}
	conf.User = url.User.Username()
	conf.Pass, _ = url.User.Password()
	// If no hostname is specified, use localhost as default.
	host := url.Hostname()
	if host == "" {
		conf.Host = "localhost"
	} else {
		conf.Host = host
	}
	// If no port is specified, use the default port 3322.
	port := url.Port()
	if port == "" {
		conf.Port = 3322
	} else {
		portInt, err := strconv.Atoi(port)
		if err != nil {
			return conf, fmt.Errorf("parsing port number '%s' as integer failed: %v", port, err)
		}
		conf.Port = portInt
	}
	// If no database is given, use the database named default.
	name := url.Path
	if name == "" {
		conf.Name = "defaultdb"
	} else {
		conf.Name = strings.TrimPrefix(name, "/")
	}
	return conf, nil
}

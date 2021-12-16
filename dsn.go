package immusql

import (
	"fmt"
	"net/url"
	"path/filepath"
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
	// Parse the datebase name and the path to it,
	// if the embedded engine is used.
	path := url.Path
	if conf.Embedded {
		// Set the path to the embedded database.
		conf.Path = filepath.Dir(filepath.FromSlash(path))
		// Use the last part of the path as database name.
		conf.Name = filepath.Base(path)
		if conf.Name == "/" || conf.Name == "." {
			// "/" and "." are not valid database names.
			// An empty name is set and replaced with the
			// name of the default database below.
			conf.Name = ""
		}
	} else {
		// Use the full path as database name.
		conf.Name = strings.TrimPrefix(path, "/")
	}
	// If no database is given, use the database named defaultdb.
	if conf.Name == "" {
		conf.Name = "defaultdb"
	}
	return conf, nil
}

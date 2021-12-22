package immusql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const userErrorMessage string = "User value introduced = %v / Expected = ImmuUser"
const passErrorMessage string = "Password value introduced = %v / Expected = immuPW"
const hostErrorMessage string = "Host value introduced = %v / Expected = localhost"
const portErrorMessage string = "Port value introduced = %v / Expected = 3322"
const dbNameErrorMessage string = "DataBase name introduced = %v / Expected = dbtest"
const embeddedErrorMessage string = "Embedded value = %v / Expected = false"

func TestParseDSN(t *testing.T) {

	// Example client connection string: immudb://immuUser:immuPW@localhost:3322/dbtest
	urlDSN := "immudb://immuUser:immuPW@localhost:3322/dbtest"

	config, err := parseDSN(urlDSN)
	if err != nil {
		t.FailNow()
	}
	user := config.User
	pass := config.Pass
	host := config.Host
	port := config.Port
	dbName := config.Name
	embeded := config.Embedded

	assert.Equal(t, "immuUser", user, userErrorMessage, user)
	assert.Equal(t, "immuPW", pass, passErrorMessage, pass)
	assert.Equal(t, "localhost", host, hostErrorMessage, host)
	assert.Equal(t, 3322, port, portErrorMessage, port)
	assert.Equal(t, "dbtest", dbName, dbNameErrorMessage, dbName)
	assert.Equal(t, false, embeded, embeddedErrorMessage, embeded)

}

func TestParseSSL(t *testing.T) {

	// Example client connection using ssl: immudbs://immuUser:immuPW@localhost:3322/dbtest
	urlSSL := "immudb://immuUser:immuPW@localhost:3322/dbtest"

	config, err := parseDSN(urlSSL)
	if err != nil {
		t.FailNow()
	}

	user := config.User
	pass := config.Pass
	host := config.Host
	port := config.Port
	dbName := config.Name
	embeded := config.Embedded

	assert.Equal(t, "immuUser", user, userErrorMessage, user)
	assert.Equal(t, "immuPW", pass, passErrorMessage, pass)
	assert.Equal(t, "localhost", host, hostErrorMessage, host)
	assert.Equal(t, 3322, port, portErrorMessage, port)
	assert.Equal(t, "dbtest", dbName, dbNameErrorMessage, dbName)
	assert.Equal(t, false, embeded, embeddedErrorMessage, embeded)

}

func TestParseEMBEDED(t *testing.T) {

	// Example embedded connection string:  immudbe://test/dbtest
	urlEMBEDDED := "immudbe://test/dbtest"

	config, err := parseDSN(urlEMBEDDED)
	if err != nil {
		t.FailNow()
	}

	host := config.Host
	port := config.Port
	dbName := config.Name
	embeded := config.Embedded

	assert.Equal(t, "test", host, "Host value introduced = %v / Expected = test", host)
	assert.Equal(t, 3322, port, portErrorMessage, port)
	assert.Equal(t, "dbtest", dbName, dbNameErrorMessage, dbName)
	assert.Equal(t, true, embeded, "Embedded value = %v / Expected = true", embeded)

}

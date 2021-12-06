package common

import "errors"

var ErrNoQueryInTx = errors.New("immudb currently does not support queries in transactions")
var ErrConfigAlreadyRegistered = errors.New("an embedded engine configuration with this name already exists")
var ErrNoConfigRegistered = errors.New("the named embedded configuration was not registered")

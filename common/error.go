package common

import "errors"

var ErrNotImplemented = errors.New("the interface is not implemented")
var ErrTxAlreadyFinished = errors.New("the transaction has already been finished")
var ErrNestedTxNotSupported = errors.New("nested transactions are currently not supported")
var ErrReadOnlyTxNotSupported = errors.New("immudb currently does not support read only transactions")
var ErrIsolationLevelNotSupported = errors.New("immudb currently does not support the requested isolation level")
var ErrConfigAlreadyRegistered = errors.New("an embedded engine configuration with this name already exists")
var ErrNoConfigRegistered = errors.New("the named embedded configuration was not registered")

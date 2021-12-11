package immusql

// ImmuDBconn exposes functions of an immudb connection
// or an embedded engine, which cannot be called using the sql api.
type ImmuDBconn interface {
	ExistTable(name string) (bool, error)
}

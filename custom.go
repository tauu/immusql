package immusql

// ImmuDBconn exposes functions of an immudb connection
// or an embedded engine, which cannot be called using the sql api.
type ImmudDBconn interface {
	ExistTable(name string) (bool, error)
}

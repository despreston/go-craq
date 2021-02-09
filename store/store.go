package store

import "errors"

var (
	// ErrNotFound should be returned by storage during a read operation if no
	// item exists for the given key.
	ErrNotFound = errors.New("that key does not exist")

	// ErrDirtyItem should be returned by storage if the latest version for the
	// key has not been committed yet.
	ErrDirtyItem = errors.New("key has an uncommitted version")
)

type Storer interface {
	Read(string) (*Item, error)
	Write(string, []byte, uint64) error
	Commit(string, uint64) error
	ReadVersion(string, uint64) (*Item, error)
	AllNewerCommitted(map[string][]uint64) ([]*Item, error)
	AllNewerDirty(map[string][]uint64) ([]*Item, error)
	AllDirty() ([]*Item, error)
	AllCommitted() ([]*Item, error)
}

// Item is an object in the Store. A key inside the store might have multiple
// versions.
type Item struct {
	Version   uint64
	Committed bool
	Value     []byte
	Key       string
}

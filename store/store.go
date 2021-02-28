// store package represents the storage layer used by the nodes in the chain.

package store

import (
	"bytes"
	"encoding/gob"
	"errors"
)

var (
	// ErrNotFound should be returned by storage during a read operation if no
	// item exists for the given key.
	ErrNotFound = errors.New("that key does not exist")

	// ErrDirtyItem should be returned by storage if the latest version for the
	// key has not been committed yet.
	ErrDirtyItem = errors.New("key has an uncommitted version")
)

type Storer interface {
	// Read an item from the store by key. If there is an uncommitted (dirty)
	// version of the item in the store, it returns a ErrDirtyItem error. If
	// no item exists for that key it returns a ErrNotFound error.
	Read(key string) (*Item, error)

	// Write a new item to the store.
	Write(key string, val []byte, version uint64) error

	// Commit a version for the given key. All items with matching key and older
	// than version are cleared.
	Commit(key string, version uint64) error

	// ReadVersion finds an item for the given key with the matching version. If
	// no item is found for that version of key, ErrNotFound is returned
	ReadVersion(key string, version uint64) (*Item, error)

	// AllNewerCommitted returns all committed items who's key is not in
	// versionsByKey or who's version is higher than the versions in
	// versionsByKey.
	AllNewerCommitted(versionsByKey map[string]uint64) ([]*Item, error)

	// AllNewerDirty returns all uncommitted items who's key is not in
	// versionsByKey or who's version is higher than the versions in
	// versionsByKey.
	AllNewerDirty(versionsByKey map[string]uint64) ([]*Item, error)

	// AllDirty returns all uncommitted items.
	AllDirty() ([]*Item, error)

	// AllCommitted returns all committed items.
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

// Encode returns the byte representation of the interface given, using
// gob.NewEncoder.
func Encode(i interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(i)
	return buf.Bytes(), err
}

// DecodeMany decodes a byte slice into a slice of Items.
func DecodeMany(b []byte) ([]*Item, error) {
	i := make([]*Item, 0)
	dec := gob.NewDecoder(bytes.NewReader(b))
	err := dec.Decode(&i)
	return i, err
}

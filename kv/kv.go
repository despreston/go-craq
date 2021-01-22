// kv package is an in-memory key/val storage. Not concurrency-safe.

package kv

import (
	"fmt"
	"sync"
)

// Item is a meta data and value for an object in the Store. A key inside the
// store might have multiple versions of the same Item.
type Item struct {
	version uint64
	dirty   bool
	val     []byte
}

func (i *Item) Version() uint64 { return i.version }
func (i *Item) Dirty() bool     { return i.dirty }
func (i *Item) Val() []byte     { return i.val }

// Store is an in-memory key/value storage.
type Store struct {
	items map[string][]*Item
	mu    sync.Mutex
}

// Read returns the latest clean version of an item. If there's a dirty version
// of the item in the store, (nil, false) is returned to signify that the item
// should be requested from the tail of the chain. If the item is in the clean
// state, (*Item, true) is returned.
func (s *Store) Read(key string) (*Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.items[key]

	if !has {
		return nil, has
	}

	// If the object has multiple versions, it can be implicitly determined that
	// the item's state is dirty, because the Item's history is purged when a
	// version is marked clean.
	// if len(items) > 1 {
	// 	return nil, false
	// }

	return items[0], true
}

// ReadVersion finds an item for the given key with the matching version. If no
// item is found for that version of key, (nil, false) is returned.
func (s *Store) ReadVersion(key string, version uint64) (*Item, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.items[key]
	if !has {
		return nil, false
	}

	for _, item := range items {
		if item.version == version {
			return item, true
		}
	}

	return nil, false
}

// Adds a new item to the store.
func (s *Store) Write(key string, val []byte, version uint64) *Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := Item{
		dirty:   true,
		val:     val,
		version: version,
	}

	s.items[key] = append(s.items[key], &item)
	return &item
}

// MarkClean sets the dirty flag to false and purges any old versions of the
// item for the given key.
func (s *Store) MarkClean(key string, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.items[key]
	if !has {
		return fmt.Errorf("no item for key %s so can't mark clean", key)
	}

	// Update the dirty flag and find index in items where version is older than
	// item.version. If this version is the oldest for this key, the index will be
	// -1.
	var older int
	for i, itm := range items {
		if itm.version == version {
			itm.dirty = false
			older = i - 1
			break
		}
	}

	// Remove the older items if there are any.
	if older > -1 {
		s.items[key] = s.items[key][older:]
	}

	return nil
}

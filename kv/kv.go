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
	val     interface{}
}

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
	if len(items) > 1 {
		return nil, false
	}

	return items[0], true
}

// Adds a new item to the store. If items already exist for this key, the
// version for the item is set using the latest version of that key.
func (s *Store) Write(key string, val interface{}) *Item {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := Item{
		dirty: true,
		val:   val,
	}

	items, has := s.items[key]

	// new key
	if !has {
		item.version = 1
		s.items[key] = []*Item{&item}
		return &item
	}

	item.version = items[0].version + 1
	items = append(items, &item)
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
	// item.version.
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

// kv package is an in-memory key/value database

package kv

import (
	"log"
	"sync"

	"github.com/despreston/go-craq/store"
)

// KV is an in-memory key/value storage.
type KV struct {
	items map[string][]*store.Item
	mu    sync.Mutex
}

// New store
func New() *KV {
	return &KV{
		items: make(map[string][]*store.Item),
	}
}

func (s *KV) lookup(key string) ([]*store.Item, bool) {
	items := s.items[key]
	if len(items) == 0 {
		return nil, false
	}
	return items, true
}

// Read an item from the store by key. If there is an uncommitted (dirty)
// version of the item in the store, it returns a ErrDirtystore.Item error. If
// no item exists for that key it returns a ErrNotFound error.
func (s *KV) Read(key string) (*store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.lookup(key)
	if !has {
		return nil, store.ErrNotFound
	}

	if !items[len(items)-1].Committed {
		return nil, store.ErrDirtyItem
	}

	return items[0], nil
}

// ReadVersion finds an item for the given key with the matching version. If no
// item is found for that version of key, ErrNotFound is returned
func (s *KV) ReadVersion(key string, version uint64) (*store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.lookup(key)
	if !has {
		return nil, store.ErrNotFound
	}

	for _, item := range items {
		if item.Version == version {
			return item, nil
		}
	}

	return nil, store.ErrNotFound
}

// Write a new item to the store.
func (s *KV) Write(key string, val []byte, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := store.Item{
		Committed: false,
		Value:     val,
		Version:   version,
		Key:       key,
	}

	s.items[key] = append(s.items[key], &item)
	return nil
}

// Commit a version for the given key.
func (s *KV) Commit(key string, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.lookup(key)
	if !has {
		return store.ErrNotFound
	}

	// Update the committed flag and find index in items where version is older
	// than item.version. If this version is the oldest for this key, the index
	// will be -1.
	var older int
	for i, itm := range items {
		if itm.Version == version {
			itm.Committed = true
			older = i - 1
			break
		}
	}

	// Remove the older items if there are any.
	if older > -1 {
		s.items[key] = s.items[key][older+1:]
	}

	log.Printf("Marked version %d of key %s committed.\n", version, key)
	return nil
}

// AllNewerCommitted returns all committed items who's key is not in keyVersions
// or who's version is higher than the versions in keyVersions.
func (s *KV) AllNewerCommitted(keyVersions map[string]uint64) ([]*store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newer := []*store.Item{}

	for key, itemsForKey := range s.items {
		// Highest local version
		local := itemsForKey[len(itemsForKey)-1]

		highestVer, has := keyVersions[key]
		if local.Committed && (!has || local.Version > highestVer) {
			newer = append(newer, local)
		}
	}

	return newer, nil
}

// AllNewerDirty returns all uncommitted items who's key is not in keyVersions
// or who's version is higher than the versions in keyVersions.
func (s *KV) AllNewerDirty(keyVersions map[string]uint64) ([]*store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newer := []*store.Item{}

	for key, items := range s.items {
		// Highest local version
		local := items[len(items)-1]

		highestVer, has := keyVersions[key]
		if !local.Committed && (!has || local.Version > highestVer) {
			newer = append(newer, local)
		}
	}

	return newer, nil
}

// AllDirty returns all uncommitted items.
func (s *KV) AllDirty() ([]*store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirty := []*store.Item{}

	for _, forKey := range s.items {
		for _, item := range forKey {
			if !item.Committed {
				dirty = append(dirty, item)
			}
		}
	}

	return dirty, nil
}

// AllCommitted returns all committed items.
func (s *KV) AllCommitted() ([]*store.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	committed := []*store.Item{}

	for _, forKey := range s.items {
		for _, item := range forKey {
			if item.Committed {
				committed = append(committed, item)
			}
		}
	}

	return committed, nil
}

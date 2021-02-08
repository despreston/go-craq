// kv package is an in-memory key/val storage. Not concurrency-safe.

package kv

import (
	"fmt"
	"log"
	"sync"

	"github.com/despreston/go-craq/node"
)

// Store is an in-memory key/value storage.
type Store struct {
	items map[string][]*node.Item
	mu    sync.Mutex
}

// New store
func New() *Store {
	return &Store{
		items: make(map[string][]*node.Item),
	}
}

func (s *Store) lookup(key string) ([]*node.Item, bool) {
	items := s.items[key]
	if len(items) == 0 {
		return nil, false
	}
	return items, true
}

// Read an item from the store by key. If there is an uncommitted (dirty)
// version of the item in the store, it returns a node.ErrDirtyItem error. If no
// item exists for that key it returns a node.ErrNotFound error.
func (s *Store) Read(key string) (*node.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.lookup(key)
	if !has {
		return nil, node.ErrNotFound
	}

	// If the object has multiple versions, it can be implicitly determined that
	// the item's state is dirty, because the node.Item's history is purged
	// when a version is marked clean.
	if len(items) > 1 {
		return nil, node.ErrDirtyItem
	}

	return items[0], nil
}

// ReadVersion finds an item for the given key with the matching version. If no
// item is found for that version of key, node.ErrNotFound is returned
func (s *Store) ReadVersion(key string, version uint64) (*node.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.lookup(key)
	if !has {
		return nil, node.ErrNotFound
	}

	for _, item := range items {
		if item.Version == version {
			return item, nil
		}
	}

	return nil, node.ErrNotFound
}

// Write a new item to the store.
func (s *Store) Write(key string, val []byte, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	item := node.Item{
		Committed: false,
		Value:     val,
		Version:   version,
		Key:       key,
	}

	s.items[key] = append(s.items[key], &item)
	return nil
}

// Commit a version for the given key.
func (s *Store) Commit(key string, version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, has := s.lookup(key)
	if !has {
		return fmt.Errorf("no item for key %s so can't commit", key)
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
func (s *Store) AllNewerCommitted(
	keyVersions map[string][]uint64,
) ([]*node.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newer := []*node.Item{}

	for key, items := range s.items {
		// Highest local version
		local := items[len(items)-1]

		given, has := keyVersions[key]
		if !has || local.Committed && local.Version > given[0] {
			newer = append(newer, local)
		}
	}

	return newer, nil
}

// AllNewerDirty returns all uncommitted items who's key is not in keyVersions
// or who's version is higher than the versions in keyVersions.
func (s *Store) AllNewerDirty(
	keyVersions map[string][]uint64,
) ([]*node.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	newer := []*node.Item{}

	for key, items := range s.items {
		// Highest local version
		local := items[len(items)-1]

		given, has := keyVersions[key]
		if !has || !local.Committed && local.Version > given[0] {
			newer = append(newer, local)
		}
	}

	return newer, nil
}

// AllDirty returns all uncommitted items.
func (s *Store) AllDirty() ([]*node.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirty := []*node.Item{}

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
func (s *Store) AllCommitted() ([]*node.Item, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	committed := []*node.Item{}

	for _, forKey := range s.items {
		for _, item := range forKey {
			if item.Committed {
				committed = append(committed, item)
			}
		}
	}

	return committed, nil
}

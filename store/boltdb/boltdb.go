package boltdb

// TODO: I'm sure most of these methods are dead fucking slow and can be
// improved.

import (
	"fmt"
	"log"

	"github.com/despreston/go-craq/store"
	bolt "go.etcd.io/bbolt"
)

type Bolt struct {
	DB     *bolt.DB
	file   string
	bucket []byte
}

func New(f, b string) *Bolt {
	return &Bolt{
		file:   f,
		bucket: []byte(b),
	}
}

func (b *Bolt) Connect() error {
	DB, err := bolt.Open(b.file, 0600, nil)
	if err != nil {
		return err
	}

	// create bucket
	err = DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(b.bucket))
		if err != nil {
			return fmt.Errorf("could not create root bucket: %v", err)
		}
		return nil
	})

	if err != nil {
		return err
	}

	b.DB = DB
	return nil
}

func (b *Bolt) Read(key string) (*store.Item, error) {
	var result []byte

	b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		result = b.Get([]byte(key))
		return nil
	})

	if result == nil {
		return nil, store.ErrNotFound
	}

	items, err := store.DecodeMany(result)
	if err != nil {
		return nil, err
	}

	if !items[len(items)-1].Committed {
		return nil, store.ErrDirtyItem
	}

	return items[len(items)-1], nil
}

func (b *Bolt) Write(key string, val []byte, version uint64) error {
	var v []*store.Item
	k := []byte(key)

	item := &store.Item{
		Committed: false,
		Value:     val,
		Version:   version,
		Key:       key,
	}

	return b.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		existing := bucket.Get(k)

		if existing == nil {
			v = append(v, item)
		} else {
			items, err := store.DecodeMany(existing)
			if err != nil {
				log.Printf("Write() decoding error\n  %#v", err)
				return err
			}
			v = append(items, item)
		}

		encoded, err := store.Encode(v)
		if err != nil {
			log.Printf("Write() encode error\n  %#v", err)
			return err
		}

		return bucket.Put(k, encoded)
	})
}

func (b *Bolt) Commit(key string, version uint64) error {
	k := []byte(key)

	return b.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		result := bucket.Get(k)

		if result == nil {
			return store.ErrNotFound
		}

		items, err := store.DecodeMany(result)
		if err != nil {
			return err
		}

		// Find index of older versions and mark the item that matches the version
		// being committed as committed.
		var older int
		for i, itm := range items {
			if itm.Version == version {
				itm.Committed = true
				older = i - 1
				break
			}
		}

		// Remove older items
		if older > -1 {
			items = items[older+1:]
		}

		encoded, err := store.Encode(items)
		if err != nil {
			return err
		}

		return bucket.Put(k, encoded)
	})
}

func (b *Bolt) ReadVersion(key string, version uint64) (*store.Item, error) {
	var result []byte

	b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		result = b.Get([]byte(key))
		return nil
	})

	if result == nil {
		return nil, store.ErrNotFound
	}

	items, err := store.DecodeMany(result)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		if item.Version == version {
			return item, nil
		}
	}

	return nil, store.ErrNotFound
}

func (b *Bolt) AllNewerCommitted(verByKey map[string]uint64) ([]*store.Item, error) {
	newer := []*store.Item{}

	err := b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			items, err := store.DecodeMany(v)
			if err != nil {
				log.Printf("Error decoding items for key %s in AllNewerCommitted\n", k)
				return err
			}

			newest := items[len(items)-1]
			highestVer, has := verByKey[string(k)]
			if newest.Committed && (!has || newest.Version > highestVer) {
				newer = append(newer, newest)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return newer, nil
}

func (b *Bolt) AllNewerDirty(verByKey map[string]uint64) ([]*store.Item, error) {
	newer := []*store.Item{}

	err := b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			items, err := store.DecodeMany(v)
			if err != nil {
				log.Printf("Error decoding items for key %s in AllNewerDirty\n", k)
				return err
			}

			newest := items[len(items)-1]
			highestVer, has := verByKey[string(k)]
			if !has || !newest.Committed && newest.Version > highestVer {
				newer = append(newer, newest)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return newer, nil
}

func (b *Bolt) AllDirty() ([]*store.Item, error) {
	dirty := []*store.Item{}

	err := b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			items, err := store.DecodeMany(v)
			if err != nil {
				log.Printf("Error decoding items for key %s in AllDirty\n", k)
				return err
			}

			for _, item := range items {
				if !item.Committed {
					dirty = append(dirty, item)
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return dirty, nil
}

func (b *Bolt) AllCommitted() ([]*store.Item, error) {
	committed := []*store.Item{}

	err := b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			items, err := store.DecodeMany(v)
			if err != nil {
				log.Printf("Error decoding items for key %s in AllCommitted\n", k)
				return err
			}

			for _, item := range items {
				if item.Committed {
					committed = append(committed, item)
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return committed, nil
}

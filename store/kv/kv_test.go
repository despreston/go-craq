package kv

import (
	"reflect"
	"testing"

	"github.com/despreston/go-craq/store"
	"github.com/despreston/go-craq/store/storetest"
)

func TestStorer(t *testing.T) {
	storetest.Run(t, func(name string, test storetest.Test) {
		test(t, New())
	})
}

func TestRead(t *testing.T) {
	item := &store.Item{Value: []byte("yes")}
	committed := &store.Item{Value: []byte("a second"), Committed: true}

	tests := []struct {
		id   string
		item *store.Item // expected item
		err  error       // expected error
		key  string      // key to read
		pre  func(*KV)   // before test hook
	}{
		{
			id:   "not found",
			item: nil,
			err:  store.ErrNotFound,
			key:  "nothing",
		},
		{
			id:   "correct item",
			item: committed,
			err:  nil,
			key:  "hello",
			pre: func(s *KV) {
				s.items["hello"] = append(s.items["hello"], committed)
			},
		},
		{
			id:   "dirty",
			item: nil,
			err:  store.ErrDirtyItem,
			key:  "hello",
			pre: func(s *KV) {
				s.items["hello"] = append(s.items["hello"], item, item)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			s := New()

			if tt.pre != nil {
				tt.pre(s)
			}

			got, err := s.Read(tt.key)
			if want := tt.item; !reflect.DeepEqual(want, got) {
				t.Errorf(
					"unexpected item\n  test: %s\n  want: %#v\n  got: %#v",
					tt.id,
					want,
					got,
				)
			}
			if err != tt.err {
				t.Errorf(
					"unexpected error\n  test: %s\n  want: %#v\n  got: %#v",
					tt.id,
					tt.err,
					err,
				)
			}
		})
	}
}

func TestKVReadVersion(t *testing.T) {
	item := &store.Item{Value: []byte("yes"), Version: 1}

	tests := []struct {
		id      string
		item    *store.Item // expected item
		err     error       // expected error
		version uint64      // version to query
		key     string      // key to read
		pre     func(*KV)   // before test hook
	}{
		{
			id:   "key not found",
			item: nil,
			err:  store.ErrNotFound,
			key:  "nothing",
		},
		{
			id:      "correct item",
			item:    item,
			err:     nil,
			version: 1,
			key:     "hello",
			pre: func(s *KV) {
				s.items["hello"] = append(s.items["hello"], item)
			},
		},
		{
			id:      "version not found",
			item:    nil,
			err:     store.ErrNotFound,
			key:     "hello",
			version: 2,
			pre: func(s *KV) {
				s.items["hello"] = append(s.items["hello"], item, item)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.id, func(t *testing.T) {
			s := New()

			if tt.pre != nil {
				tt.pre(s)
			}

			got, err := s.ReadVersion(tt.key, tt.version)
			if want := tt.item; !reflect.DeepEqual(want, got) {
				t.Errorf(
					"unexpected item\n  test: %s\n  want: %#v\n  got: %#v",
					tt.id,
					want,
					got,
				)
			}
			if err != tt.err {
				t.Errorf(
					"unexpected error\n  test: %s\n  want: %#v\n  got: %#v",
					tt.id,
					tt.err,
					err,
				)
			}
		})
	}
}

func TestKVWrite(t *testing.T) {
	s := New()
	if err := s.Write("hello", []byte("world"), 1); err != nil {
		t.Fatalf("Write(hello) unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}

	got := s.items["hello"][0]

	want := store.Item{
		Committed: false,
		Value:     []byte("world"),
		Version:   1,
	}

	if reflect.DeepEqual(want, got) {
		t.Errorf("Write() unexpected item\n  want: %#v\n  got: %#v", want, got)
	}
}

func TestKVCommit(t *testing.T) {
	s := New()

	want := store.ErrNotFound
	if got := s.Commit("whatever", 1); got != want {
		t.Fatalf("Commit(whatever) unexpected error\n  want: %#v\n  got: %#v", want, got)
	}

	s.items["hello"] = append(
		s.items["hello"],
		&store.Item{Committed: false, Version: 1},
		&store.Item{Committed: false, Version: 2},
	)

	if err := s.Commit("hello", 2); err != nil {
		t.Errorf("Commit(whatever, 2) unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}

	if len(s.items["hello"]) != 1 {
		t.Errorf("Commit(whatever, 2) expected old items to be cleared")
	}

	if item := s.items["hello"][0]; item.Version != 2 || !item.Committed {
		t.Errorf("Commit(whatever, 2) expected item to be committed")
	}
}

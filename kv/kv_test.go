package kv

import (
	"reflect"
	"testing"

	"github.com/despreston/go-craq/node"
)

func TestRead(t *testing.T) {
	item := &node.Item{Value: []byte("yes")}

	tests := []struct {
		id   string
		item *node.Item   // expected item
		err  error        // expected error
		key  string       // key to read
		pre  func(*Store) // before test hook
	}{
		{
			id:   "not found",
			item: nil,
			err:  node.ErrNotFound,
			key:  "nothing",
		},
		{
			id:   "correct item",
			item: item,
			err:  nil,
			key:  "hello",
			pre: func(s *Store) {
				s.items["hello"] = append(s.items["hello"], item)
			},
		},
		{
			id:   "dirty",
			item: nil,
			err:  node.ErrDirtyItem,
			key:  "hello",
			pre: func(s *Store) {
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
				t.Fatalf(
					"unexpected item\n  test: %s\n  want: %#v\n  got: %#v",
					tt.id,
					want,
					got,
				)
			}
			if err != tt.err {
				t.Fatalf(
					"unexpected error\n  test: %s\n  want: %#v\n  got: %#v",
					tt.id,
					tt.err,
					err,
				)
			}
		})
	}
}

func TestReadVersion(t *testing.T) {
	item := &node.Item{Value: []byte("yes"), Version: 1}

	tests := []struct {
		id      string
		item    *node.Item   // expected item
		err     error        // expected error
		version uint64       // version to query
		key     string       // key to read
		pre     func(*Store) // before test hook
	}{
		{
			id:   "key not found",
			item: nil,
			err:  node.ErrNotFound,
			key:  "nothing",
		},
		{
			id:      "correct item",
			item:    item,
			err:     nil,
			version: 1,
			key:     "hello",
			pre: func(s *Store) {
				s.items["hello"] = append(s.items["hello"], item)
			},
		},
		{
			id:      "version not found",
			item:    nil,
			err:     node.ErrNotFound,
			key:     "hello",
			version: 2,
			pre: func(s *Store) {
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
				t.Fatalf(
					"unexpected item\n  test: %s\n  want: %#v\n  got: %#v",
					tt.id,
					want,
					got,
				)
			}
			if err != tt.err {
				t.Fatalf(
					"unexpected error\n  test: %s\n  want: %#v\n  got: %#v",
					tt.id,
					tt.err,
					err,
				)
			}
		})
	}
}

func TestWrite(t *testing.T) {
	s := New()
	if err := s.Write("hello", []byte("world"), 1); err != nil {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}

	got := s.items["hello"][0]

	want := node.Item{
		Committed: false,
		Value:     []byte("world"),
		Version:   1,
	}

	if reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected item\n  want: %#v\n  got: %#v", want, got)
	}
}

func TestCommit(t *testing.T) {
	s := New()

	want := "no item for key whatever so can't commit"
	if got := s.Commit("whatever", 1); got.Error() != want {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", want, got)
	}

	s.items["hello"] = append(
		s.items["hello"],
		&node.Item{Committed: false, Version: 1},
		&node.Item{Committed: false, Version: 2},
	)

	if err := s.Commit("hello", 2); err != nil {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}

	if len(s.items["hello"]) != 1 {
		t.Fatalf("expected old items to be cleared")
	}

	if item := s.items["hello"][0]; item.Version != 2 || !item.Committed {
		t.Fatalf("expected item to be committed")
	}
}

package storetest

import (
	"reflect"
	"testing"

	"github.com/despreston/go-craq/store"
	"github.com/google/go-cmp/cmp"
)

func Test(t *testing.T, s func() store.Storer) {
	t.Run("BasicWriteCommitRead", func(t *testing.T) { testBasicWriteCommitRead(t, s()) })
	t.Run("ReadUnknownKey", func(t *testing.T) { testReadUnknownKey(t, s()) })
	t.Run("ReadDirty", func(t *testing.T) { testReadDirty(t, s()) })
	t.Run("ReadVersion", func(t *testing.T) { testReadVersion(t, s()) })
	t.Run("ReadVersionUnknownKey", func(t *testing.T) { testReadVersionUnknownKey(t, s()) })
	t.Run("AllNewerCommitted", func(t *testing.T) { testAllNewerCommitted(t, s()) })
	t.Run("AllNewerDirty", func(t *testing.T) { testAllNewerDirty(t, s()) })
	t.Run("AllDirty", func(t *testing.T) { testAllDirty(t, s()) })
	t.Run("AllCommitted", func(t *testing.T) { testAllCommitted(t, s()) })
}

func testBasicWriteCommitRead(t *testing.T, s store.Storer) {
	itm := store.Item{
		Key:       "hello",
		Value:     []byte("world"),
		Version:   uint64(1),
		Committed: true,
	}

	if err := s.Write(itm.Key, itm.Value, itm.Version); err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}

	if err := s.Commit(itm.Key, itm.Version); err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}

	got, err := s.Read(itm.Key)
	if err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}
	if !reflect.DeepEqual(got, &itm) {
		t.Fatalf(
			"unexpected item\n  want: %#v\n  got: %#v",
			itm,
			got,
		)
	}
}

func testReadUnknownKey(t *testing.T, s store.Storer) {
	want := store.ErrNotFound
	if _, err := s.Read("unknown"); err != want {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			want,
			err,
		)
	}
}

func testReadDirty(t *testing.T, s store.Storer) {
	itm := store.Item{
		Key:     "hello",
		Value:   []byte("world"),
		Version: uint64(1),
	}

	if err := s.Write(itm.Key, itm.Value, itm.Version); err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}

	want := store.ErrDirtyItem
	if _, err := s.Read(itm.Key); err != want {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			want,
			err,
		)
	}
}

func testReadVersion(t *testing.T, s store.Storer) {
	itm := &store.Item{
		Key:     "hello",
		Value:   []byte("world"),
		Version: uint64(1),
	}

	s.Write(itm.Key, itm.Value, itm.Version)
	read, err := s.ReadVersion(itm.Key, itm.Version)

	if err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}

	if !reflect.DeepEqual(itm, read) {
		t.Fatalf(
			"unexpected item\n  want: %#v\n  got: %#v",
			itm,
			read,
		)
	}
}

func testReadVersionUnknownKey(t *testing.T, s store.Storer) {
	want := store.ErrNotFound
	if _, err := s.ReadVersion("wrong", 0); err != want {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			want,
			err,
		)
	}
}

func testAllNewerCommitted(t *testing.T, s store.Storer) {
	items := []*store.Item{
		{
			Key:     "test",
			Value:   []byte("world"),
			Version: uint64(1),
		},
		{
			Key:       "hello",
			Value:     []byte("another"),
			Version:   uint64(2),
			Committed: true,
		},
		{
			Key:       "another",
			Value:     []byte("some value"),
			Version:   uint64(1),
			Committed: true,
		},
	}

	for _, i := range items {
		s.Write(i.Key, i.Value, i.Version)
	}

	s.Commit("hello", items[1].Version)
	s.Commit("another", items[2].Version)

	in := map[string][]uint64{"hello": []uint64{0}}
	got, err := s.AllNewerCommitted(in)

	if err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}

	if diff := cmp.Diff(items[1:], got); diff != "" {
		t.Errorf("AllNewerCommitted response mismatch (-want +got):\n%s", diff)
	}
}

func testAllNewerDirty(t *testing.T, s store.Storer) {
	items := []*store.Item{
		{
			Key:     "hello",
			Value:   []byte("world"),
			Version: uint64(1),
		},
		{
			Key:     "another",
			Value:   []byte("some value"),
			Version: uint64(1),
		},
		{
			Key:     "hello",
			Value:   []byte("foo"),
			Version: uint64(2),
		},
		{
			Key:     "bizz",
			Value:   []byte("bazz"),
			Version: uint64(1),
		},
	}

	for _, i := range items {
		s.Write(i.Key, i.Value, i.Version)
	}

	s.Commit("another", items[1].Version)
	in := map[string][]uint64{"hello": []uint64{1}}
	got, err := s.AllNewerDirty(in)

	if err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}

	for _, wantItem := range items[2:] {
		var found bool
		for _, gotItem := range got {
			if diff := cmp.Diff(wantItem, gotItem); diff == "" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("AllNewerDirty response missing item:\n%#v", wantItem)
		}
	}
}

func testAllDirty(t *testing.T, s store.Storer) {
	items := []*store.Item{
		{
			Key:     "hello",
			Value:   []byte("world"),
			Version: uint64(1),
		},
		{
			Key:     "hello",
			Value:   []byte("foo"),
			Version: uint64(2),
		},
	}

	for _, i := range items {
		s.Write(i.Key, i.Value, i.Version)
	}

	s.Commit("hello", items[0].Version)

	got, err := s.AllDirty()
	if err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}

	var found bool
	want := items[1]
	for _, gotItem := range got {
		if diff := cmp.Diff(want, gotItem); diff == "" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("AllDirty response missing item:\n%#v", want)
	}
}

func testAllCommitted(t *testing.T, s store.Storer) {
	items := []*store.Item{
		{
			Key:       "hello",
			Value:     []byte("world"),
			Version:   uint64(1),
			Committed: true,
		},
		{
			Key:     "foo",
			Value:   []byte("bar"),
			Version: uint64(1),
		},
	}

	for _, i := range items {
		s.Write(i.Key, i.Value, i.Version)
	}

	s.Commit("hello", items[0].Version)

	got, err := s.AllCommitted()
	if err != nil {
		t.Fatalf(
			"unexpected error\n  want: %#v\n  got: %#v",
			nil,
			err,
		)
	}
	if want, got := items[0], got[0]; !reflect.DeepEqual(want, got) {
		t.Fatalf(
			"unexpected response\n  want: %#v\n  got: %#v",
			want,
			got,
		)
	}

	var found bool
	want := items[0]
	for _, gotItem := range got {
		if diff := cmp.Diff(want, gotItem); diff == "" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("AllCommitted response missing item:\n%#v", want)
	}
}

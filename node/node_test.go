package node

import (
	"bytes"
	"testing"
	"time"

	"github.com/despreston/go-craq/coordinator"
	"github.com/despreston/go-craq/store/kv"
	"github.com/despreston/go-craq/transport"
	"github.com/google/go-cmp/cmp"
)

type FakeClient struct{}

func (f *FakeClient) Connect(path string) error {
	return nil
}

func (f *FakeClient) Close() error {
	return nil
}

type FakeNode struct {
	*Node
	*FakeClient
}

type FakeCoordinator struct {
	*coordinator.Coordinator
	*FakeClient
	clientCount int
}

func setupTwoNodeChain() (*Node, *Node, *FakeCoordinator) {
	var n, n2 *Node
	c := &FakeCoordinator{Coordinator: coordinator.New("coordinator")}

	n = New(Opts{
		Address:           "a",
		CdrAddress:        "coordinator",
		PubAddress:        "nodea-public",
		Store:             kv.New(),
		CoordinatorClient: c,
		Transport: func() transport.NodeClient {
			return &FakeNode{Node: n2}
		},
	})

	n2 = New(Opts{
		Address:           "b",
		CdrAddress:        "coordinator",
		PubAddress:        "nodeb-public",
		Store:             kv.New(),
		CoordinatorClient: c,
		Transport: func() transport.NodeClient {
			return &FakeNode{Node: n}
		},
	})

	c.Coordinator.Transport = func() transport.NodeClient {
		// In order to send messages to the right node. n2 connects after n
		defer func() { c.clientCount++ }()
		if c.clientCount == 0 {
			return &FakeNode{Node: n}
		}
		return &FakeNode{Node: n2}
	}

	return n, n2, c
}

func TestStart(t *testing.T) {
	c := &FakeCoordinator{Coordinator: coordinator.New("coordinator")}

	n := New(Opts{
		Address:           "node",
		CdrAddress:        "coordinator",
		PubAddress:        "node-public",
		Transport:         func() transport.NodeClient { return &FakeNode{} },
		CoordinatorClient: c,
	})

	c.Coordinator.Transport = func() transport.NodeClient {
		return &FakeNode{Node: n}
	}

	if err := n.Start(); err != nil {
		t.Fatalf("unexpected error\n  got: %#v", err.Error())
	}

	if !n.IsHead {
		t.Fatalf("unexpected IsHead\n  want: %#v\n  got: %#v", true, n.IsHead)
	}

	if !n.IsTail {
		t.Fatalf("unexpected IsTail\n  want: %#v\n  got: %#v", true, n.IsTail)
	}
}

func TestStart_SecondNode(t *testing.T) {
	n, n2, c := setupTwoNodeChain()
	n.Start()

	if err := n2.Start(); err != nil {
		t.Fatalf("unexpected error\n  got: %#v", err.Error())
	}

	c.Updates.Wait()

	if !n.IsHead {
		t.Fatalf("unexpected IsHead\n  want: %#v\n  got: %#v", true, n.IsHead)
	}

	if n2.IsHead {
		t.Fatalf("unexpected IsHead\n  want: %#v\n  got: %#v", false, n2.IsHead)
	}

	if n.IsTail {
		t.Fatalf("unexpected IsTail\n  want: %#v\n  got: %#v", false, n.IsTail)
	}

	if !n2.IsTail {
		t.Fatalf("unexpected IsTail\n  want: %#v\n  got: %#v", true, n2.IsTail)
	}
}

// The new item should end up being committed to each node. Should be able to
// read the item from each node.
func TestWriteRead(t *testing.T) {
	n, n2, c := setupTwoNodeChain()
	n.Start()
	n2.Start()
	c.Updates.Wait()
	n.committed = make(chan commit, 1)
	c.Write("hello", []byte("world"))

	select {
	case got := <-n.committed:
		want := commit{Key: "hello", Version: 0}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Fatalf("unexpected commit\n  want: %#v\n  got: %#v", want, got)
		}

		// Reading from node A
		k, v, err := n.Read("hello")
		if err != nil {
			t.Fatalf("unexpected error\n  got: %#v", err)
		}
		if k != "hello" {
			t.Fatalf("unexpected key\n  want: hello\n  got: %#v", k)
		}
		if want := []byte("world"); !bytes.Equal(want, v) {
			t.Fatalf("unexpected value\n  want: %#v\n  got: %#v", want, v)
		}

		// Reading from node B
		k, v, err = n2.Read("hello")
		if err != nil {
			t.Fatalf("unexpected error\n  got: %#v", err)
		}
		if k != "hello" {
			t.Fatalf("unexpected key\n  want: hello\n  got: %#v", k)
		}
		if want := []byte("world"); !bytes.Equal(want, v) {
			t.Fatalf("unexpected value\n  want: %#v\n  got: %#v", want, v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for commit")
	}
}

func TestReadUnknownKey(t *testing.T) {
	n, _, _ := setupTwoNodeChain()
	_, _, err := n.Read("whatever")
	want := "key doesn't exist"
	if err == nil || err.Error() != want {
		t.Fatalf("unexpected error\n  want: %s\n  got:%s", want, err)
	}
}

// Getting the latest version from the tail.
func TestReadDirty(t *testing.T) {
	n, n2, c := setupTwoNodeChain()
	n.Start()
	n2.Start()
	c.Updates.Wait()

	// Alter the store directly to avoid triggering RPCs
	n.store.Write("hello", []byte("world"), 0)
	n.store.Write("hello", []byte("foo"), 1)
	n.store.Commit("hello", 0)
	n2.latest["hello"] = 1

	k, v, err := n.Read("hello")
	if err != nil {
		t.Fatalf("unexpected error\n  got: %#v", err)
	}
	if k != "hello" {
		t.Fatalf("unexpected key\n  want: hello\n  got: %#v", k)
	}
	if want := []byte("foo"); !bytes.Equal(want, v) {
		t.Fatalf("unexpected value\n  want: %#v\n  got: %#v", want, v)
	}
}

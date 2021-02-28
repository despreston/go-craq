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

func assertItem(t *testing.T, n *Node, kWant string, vWant []byte) {
	t.Helper()
	k, v, err := n.Read(kWant)
	if err != nil {
		t.Errorf("Read(%s) unexpected error\n  got: %#v", kWant, err)
	}
	if k != kWant {
		t.Errorf("Read(%s) unexpected key\n  want: %s\n  got: %s", kWant, kWant, k)
	}
	if !bytes.Equal(vWant, v) {
		t.Errorf("Read(%s) unexpected value\n  want: %#v\n  got: %#v", kWant, vWant, v)
	}
}

func setupTwoNodeChain() (*Node, *Node, *FakeCoordinator) {
	var n, n2 *Node
	var c FakeCoordinator

	n = New(Opts{
		Address:           "a",
		CdrAddress:        "coordinator",
		PubAddress:        "nodea-public",
		Store:             kv.New(),
		CoordinatorClient: &c,
		Transport: func() transport.NodeClient {
			return &FakeNode{Node: n2}
		},
	})

	n2 = New(Opts{
		Address:           "b",
		CdrAddress:        "coordinator",
		PubAddress:        "nodeb-public",
		Store:             kv.New(),
		CoordinatorClient: &c,
		Transport: func() transport.NodeClient {
			return &FakeNode{Node: n}
		},
	})

	tport := func() transport.NodeClient {
		// In order to send messages to the right node. n2 connects after n
		defer func() { c.clientCount++ }()
		if c.clientCount == 0 {
			return &FakeNode{Node: n}
		}
		return &FakeNode{Node: n2}
	}

	c = FakeCoordinator{Coordinator: coordinator.New(tport)}
	return n, n2, &c
}

func TestStart(t *testing.T) {
	var c FakeCoordinator

	n := New(Opts{
		Address:           "node",
		CdrAddress:        "coordinator",
		PubAddress:        "node-public",
		Transport:         func() transport.NodeClient { return &FakeNode{} },
		CoordinatorClient: &c,
		Store:             kv.New(),
	})

	c = FakeCoordinator{
		Coordinator: coordinator.New(func() transport.NodeClient {
			return &FakeNode{Node: n}
		}),
	}

	if err := n.Start(); err != nil {
		t.Errorf("Start() unexpected error\n  got: %#v", err.Error())
	}

	if !n.IsHead {
		t.Errorf("Start() unexpected IsHead\n  want: %#v\n  got: %#v", true, n.IsHead)
	}

	if !n.IsTail {
		t.Errorf("Start() unexpected IsTail\n  want: %#v\n  got: %#v", true, n.IsTail)
	}
}

func TestStart_SecondNode(t *testing.T) {
	n, n2, c := setupTwoNodeChain()
	n.Start()

	if err := n2.Start(); err != nil {
		t.Fatalf("Start() unexpected error\n  got: %#v", err.Error())
	}

	c.Updates.Wait()

	if !n.IsHead {
		t.Errorf("Start() unexpected IsHead\n  want: %#v\n  got: %#v", true, n.IsHead)
	}

	if n2.IsHead {
		t.Errorf("Start() unexpected IsHead\n  want: %#v\n  got: %#v", false, n2.IsHead)
	}

	if n.IsTail {
		t.Errorf("Start() unexpected IsTail\n  want: %#v\n  got: %#v", false, n.IsTail)
	}

	if !n2.IsTail {
		t.Errorf("Start() unexpected IsTail\n  want: %#v\n  got: %#v", true, n2.IsTail)
	}
}

// The new item should end up being committed to each node. Should be able to
// read the item from each node.
func TestWriteRead(t *testing.T) {
	n, n2, c := setupTwoNodeChain()
	n.Start()
	n2.Start()
	c.Updates.Wait()
	n.committed = make(chan commitEvent, 1)
	c.Write("hello", []byte("world"))

	select {
	case got := <-n.committed:
		want := commitEvent{Key: "hello", Version: 0}
		if diff := cmp.Diff(got, want); diff != "" {
			t.Fatalf("unexpected commit\n  want: %#v\n  got: %#v", want, got)
		}

		// Reading from node A
		assertItem(t, n, "hello", []byte("world"))

		// Reading from node B
		assertItem(t, n2, "hello", []byte("world"))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for commit")
	}
}

func TestReadUnknownKey(t *testing.T) {
	n, _, _ := setupTwoNodeChain()
	_, _, err := n.Read("whatever")
	want := "key doesn't exist"
	if err == nil || err.Error() != want {
		t.Errorf("Read(whatever) unexpected error\n  want: %s\n  got:%s", want, err)
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

	assertItem(t, n, "hello", []byte("foo"))
}

// New node gets caught up by asking for items from the other node.
func TestFullPropagation(t *testing.T) {
	n, n2, c := setupTwoNodeChain()
	n.committed = make(chan commitEvent, 1)
	n2.committed = make(chan commitEvent, 1)
	n.Start()
	c.Write("hello", []byte("world"))
	n2.Start()

	select {
	case <-n2.committed:
		assertItem(t, n2, "hello", []byte("world"))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for propagation")
	}
}

// Tail fails and the new tail has dirty items. The dirty items get committed.
func TestNewTailCommitDirty(t *testing.T) {
	n, n2, c := setupTwoNodeChain()
	n.committed = make(chan commitEvent, 1)
	n2.committed = make(chan commitEvent, 1)
	n.Start()
	n2.Start()
	c.Updates.Wait()
	n.store.Write("hello", []byte("world"), 0)
	c.RemoveNode("nodeb-public")

	select {
	case <-n.committed:
		assertItem(t, n, "hello", []byte("world"))
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for propagation")
	}
}

// Node has committed items in the store and needs to backfill it's map of
// latest versions.
func TestBackfill(t *testing.T) {
	var c FakeCoordinator

	n := New(Opts{
		Address:           "node",
		CdrAddress:        "coordinator",
		PubAddress:        "node-public",
		Transport:         func() transport.NodeClient { return &FakeNode{} },
		CoordinatorClient: &c,
		Store:             kv.New(),
	})

	c = FakeCoordinator{
		Coordinator: coordinator.New(func() transport.NodeClient {
			return &FakeNode{Node: n}
		}),
	}

	n.store.Write("hello", []byte("world"), 1)
	n.store.Commit("hello", 1)

	if err := n.Start(); err != nil {
		t.Errorf("Start() unexpected error\n  got: %#v", err.Error())
	}

	if k, ver, err := n.LatestVersion("hello"); err != nil {
		t.Errorf("LatestVersion(hello) unexpected error %#v", err)
	} else if k != "hello" || ver != 1 {
		t.Errorf("LatestVersion(hello) = %s, %d, nil. Want hello, 1, nil", k, ver)
	}
}

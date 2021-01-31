package coordinator

import (
	"reflect"
	"testing"

	"github.com/despreston/go-craq/craqrpc"
)

type fakeNode struct {
	path       string
	updateArgs *craqrpc.NodeMeta // track what Update was called with
	connected  bool
}

func (fakeNode) Connect() error      { return nil }
func (n fakeNode) Path() string      { return n.path }
func (n fakeNode) IsConnected() bool { return n.connected }

func (fakeNode) Ping() (*craqrpc.AckResponse, error) {
	return &craqrpc.AckResponse{}, nil
}

func (f *fakeNode) Update(nm *craqrpc.NodeMeta) (*craqrpc.AckResponse, error) {
	f.updateArgs = nm
	return &craqrpc.AckResponse{Ok: true}, nil
}

func (fakeNode) ClientWrite(
	args *craqrpc.ClientWriteArgs,
) (*craqrpc.AckResponse, error) {
	return &craqrpc.AckResponse{Ok: true}, nil
}

// Trying to remove a node that isn't in the list of replicas. I guess just make
// sure it doesn't change the existing list of nodes?
func TestRemoveNodeUnknown(t *testing.T) {
	node, another := fakeNode{path: "123"}, fakeNode{path: "456"}

	cdr := Coordinator{
		replicas: []nodeDispatcher{&node},
		tail:     &node,
	}

	cdr.removeNode(another)

	if l := len(cdr.replicas); l != 1 {
		t.Fatalf("unexpected number of replicas, want: 1, got: %d", l)
	}
}

func TestRemoveNodeTail(t *testing.T) {
	node := fakeNode{}

	cdr := Coordinator{
		replicas: []nodeDispatcher{&node},
		tail:     &node,
	}

	cdr.removeNode(node)

	if len(cdr.replicas) != 0 {
		t.Fatalf("expected no replicas, got %d", len(cdr.replicas))
	}

	if cdr.tail != nil {
		t.Fatalf("unexpected tail, got %#v", cdr.tail)
	}
}

func TestRemoveNodeTailWithOthers(t *testing.T) {
	var (
		a = fakeNode{path: "123"}
		b = fakeNode{path: "456"}
	)

	cdr := Coordinator{
		replicas: []nodeDispatcher{&b, &a},
		tail:     &a,
	}

	cdr.removeNode(a)

	if want, got := 1, len(cdr.replicas); want != got {
		t.Fatalf(
			"unexpected number of replicas\n  want: %#v\n  got: %#v",
			want,
			got,
		)
	}

	if cdr.tail != &b {
		t.Fatalf("unexpected tail\n  want: %#v\n  got: %#v", b, cdr.tail)
	}

	want := &craqrpc.NodeMeta{
		IsHead: true,
		IsTail: true,
		Tail:   "456",
	}

	if got := b.updateArgs; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected node update\n  want: %#v\n  got: %#v", want, got)
	}
}

func TestRemoveNodeHead(t *testing.T) {
	var (
		a = fakeNode{path: "123"}
		b = fakeNode{path: "456"}
	)

	cdr := Coordinator{
		replicas: []nodeDispatcher{&a, &b},
		tail:     &b,
	}

	cdr.removeNode(a)

	if want, got := 1, len(cdr.replicas); want != got {
		t.Fatalf(
			"unexpected number of replicas\n  want: %#v\n  got: %#v",
			want,
			got,
		)
	}

	if cdr.tail != &b {
		t.Fatalf("unexpected tail\n  want: %#v\n  got: %#v", b, cdr.tail)
	}

	want := &craqrpc.NodeMeta{
		IsHead: true,
		IsTail: true,
		Tail:   "456",
	}

	if got := b.updateArgs; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected node update\n  want: %#v\n  got: %#v", want, got)
	}
}

func TestRemoveNodeMiddle(t *testing.T) {
	var (
		a = fakeNode{path: "123"}
		b = fakeNode{path: "456"}
		c = fakeNode{path: "789"}
	)

	cdr := Coordinator{
		replicas: []nodeDispatcher{&a, &b, &c},
		tail:     &c,
	}

	cdr.removeNode(b)

	if want, got := 2, len(cdr.replicas); want != got {
		t.Fatalf(
			"unexpected number of replicas\n  want: %#v\n  got: %#v",
			want,
			got,
		)
	}

	if cdr.tail != &c {
		t.Fatalf("unexpected tail\n  want: %#v\n  got: %#v", b, cdr.tail)
	}

	want := &craqrpc.NodeMeta{
		IsHead: true,
		IsTail: false,
		Tail:   "789",
		Next:   "789",
	}

	if got := a.updateArgs; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected node update\n  want: %#v\n  got: %#v", want, got)
	}

	want = &craqrpc.NodeMeta{
		IsHead: false,
		IsTail: true,
		Tail:   "789",
		Prev:   "123",
	}

	if got := c.updateArgs; !reflect.DeepEqual(want, got) {
		t.Fatalf("unexpected node update\n  want: %#v\n  got: %#v", want, got)
	}
}

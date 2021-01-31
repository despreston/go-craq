package coordinator

import (
	"reflect"
	"testing"

	"github.com/despreston/go-craq/craqrpc"
	"github.com/despreston/go-craq/transport"
)

type FakeTransportClient struct{}

func (ftc *FakeTransportClient) Call(
	method string,
	args, reply interface{},
) error {
	return nil
}

type FakeTransport struct {
	connected bool
}

func (ft *FakeTransport) Connect(path string) (transport.Client, error) {
	ft.connected = true
	return &FakeTransportClient{}, nil
}

func TestAddNode(t *testing.T) {
	reply := &craqrpc.NodeMeta{}
	args := &craqrpc.AddNodeArgs{Path: "123"}
	cdr := &Coordinator{Transport: &FakeTransport{}}
	rpc := RPC{c: cdr}

	if err := rpc.AddNode(args, reply); err != nil {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}

	want := &craqrpc.NodeMeta{IsHead: true, IsTail: true, Tail: "123"}
	if !reflect.DeepEqual(want, reply) {
		t.Fatalf("unexpected reply\n  want: %#v\n  got: %#v", want, reply)
	}

	if !cdr.replicas[0].IsConnected() {
		t.Fatalf("expected node to be connected")
	}
}

func TestAddNodeSecond(t *testing.T) {
	node := fakeNode{path: "456"}

	cdr := &Coordinator{
		Transport: &FakeTransport{},
		replicas:  []nodeDispatcher{&node},
		tail:      &node,
	}

	rpc := RPC{c: cdr}

	reply := &craqrpc.NodeMeta{}
	args := &craqrpc.AddNodeArgs{Path: "123"}
	if err := rpc.AddNode(args, reply); err != nil {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}

	want := &craqrpc.NodeMeta{
		IsHead: false,
		IsTail: true,
		Tail:   "123",
		Prev:   "456",
	}

	if !reflect.DeepEqual(want, reply) {
		t.Fatalf("unexpected reply\n  want: %#v\n  got: %#v", want, reply)
	}
}

func TestWriteNoNodes(t *testing.T) {
	cdr := &Coordinator{}
	rpc := RPC{c: cdr}
	args := &craqrpc.ClientWriteArgs{}
	reply := &craqrpc.AckResponse{}

	if want, got := ErrEmptyChain, rpc.Write(args, reply); want != got {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", want, got)
	}

	if reply.Ok {
		t.Fatalf("unexpected reply\n  want: %#v\n  got: %#v", false, true)
	}
}

type nodeWriteFail struct {
	fakeNode
}

func (nodeWriteFail) ClientWrite(
	args *craqrpc.ClientWriteArgs,
) (*craqrpc.AckResponse, error) {
	return &craqrpc.AckResponse{Ok: false}, nil
}

func TestWriteError(t *testing.T) {
	node := nodeWriteFail{}
	cdr := &Coordinator{replicas: []nodeDispatcher{&node}}
	rpc := RPC{c: cdr}
	reply := &craqrpc.AckResponse{}
	rpc.Write(&craqrpc.ClientWriteArgs{}, reply)

	if reply.Ok {
		t.Fatalf("unexpected reply\n  want: %#v\n  got: %#v", false, true)
	}
}

func TestWrite(t *testing.T) {
	node := fakeNode{}
	cdr := &Coordinator{replicas: []nodeDispatcher{&node}}
	rpc := RPC{c: cdr}
	reply := &craqrpc.AckResponse{}
	rpc.Write(&craqrpc.ClientWriteArgs{}, reply)

	if !reply.Ok {
		t.Fatalf("unexpected reply\n  want: %#v\n  got: %#v", true, false)
	}
}

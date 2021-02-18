package node

import (
	"errors"
	"testing"

	"github.com/despreston/go-craq/craqrpc"
	"github.com/despreston/go-craq/store/kv"
	"github.com/despreston/go-craq/transport"
)

type testClient struct {
	callFail bool
}

func (tc *testClient) Call(method string, args, reply interface{}) error {
	if tc.callFail {
		return errors.New("fail")
	}
	if method == "RPC.AddNode" {
		r := reply.(*craqrpc.NodeMeta)
		r.IsHead = false
		r.IsTail = true
		r.Tail = "0.0.0.0"
		r.Next = ""
		r.Prev = "0.0.0.0:1235"
		reply = &r
	}
	return nil
}

func (tc *testClient) Close() error {
	return nil
}

type testTransport struct {
	connectError bool
	connectArg   string
	client       testClient
}

func (tt *testTransport) Connect(addr string) (transport.Client, error) {
	tt.connectArg = addr
	if tt.connectError {
		return &tt.client, errors.New("oops")
	}
	return &tt.client, nil
}

func TestConnectToCoordinator_ConnectFail(t *testing.T) {
	tt := &testTransport{connectError: true}
	n := Node{transport: tt}
	if err := n.ConnectToCoordinator(); err == nil || err.Error() != "oops" {
		t.Fatalf("unexpected error\n  want: oops\n  got: %#v", err.Error())
	}
}

func TestConnectToCoordinator_AnnounceFail(t *testing.T) {
	tt := &testTransport{client: testClient{callFail: true}}
	n := New(Opts{Transport: tt})
	if want, err := "fail", n.ConnectToCoordinator(); err == nil || err.Error() != want {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", want, err.Error())
	}
}

func TestConnectToCoordinator_SingleNode(t *testing.T) {
	tt := &testTransport{}
	n := New(Opts{Transport: tt, Store: kv.New()})

	if got := n.ConnectToCoordinator(); got != nil {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", nil, got)
	}

	if want, got := false, n.isHead; want != got {
		t.Fatalf("unexpected isHead\n  want: %#v\n  got: %#v", want, got)
	}

	if want, got := true, n.isTail; want != got {
		t.Fatalf("unexpected isHead\n  want: %#v\n  got: %#v", want, got)
	}

	tailN := n.neighbors[craqrpc.NeighborPosTail].address
	if want, got := "0.0.0.0", tailN; want != got {
		t.Fatalf("unexpected isHead\n  want: %#v\n  got: %#v", want, got)
	}
}

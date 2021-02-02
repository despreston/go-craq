package coordinator

import (
	"testing"
)

func TestConnect(t *testing.T) {
	n := node{transport: &fakeTransport{}}

	if err := n.Connect(); err != nil {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}

	if !n.connected {
		t.Fatalf("expected connected to be true")
	}

	if want := (&fakeTransportClient{}); n.client == nil {
		t.Fatalf("unexpected client\n  want: %#v\n  got: %#v", want, n.client)
	}
}

func TestConnectFail(t *testing.T) {
	n := node{transport: &fakeTransport{errorOnConnect: true}}

	if err := n.Connect(); err == nil {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}

	if n.connected {
		t.Fatalf("expected connected to be false")
	}
}

func TestPing(t *testing.T) {
	n := node{client: &fakeTransportClient{}}
	_, err := n.Ping()

	if err != nil {
		t.Fatalf("unexpected error\n  want: %#v\n  got: %#v", nil, err)
	}
}

func TestPingFail(t *testing.T) {
	n := node{client: &fakeTransportClient{errorOnPing: true}}

	if _, err := n.Ping(); err == nil {
		t.Fatalf("expected ping to return error")
	}
}

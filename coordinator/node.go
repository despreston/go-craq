package coordinator

import (
	"time"

	"github.com/despreston/go-craq/craqrpc"
	"github.com/despreston/go-craq/transport"
)

type node struct {
	transport transport.Transporter
	client    transport.Client
	connected bool
	last      time.Time // last successful ping
	path      string    // host and port
}

func (n *node) Connect() error {
	client, err := n.transport.Connect(n.path)
	if err != nil {
		n.connected = false
		return err
	}
	n.connected = true
	n.client = client
	return nil
}

func (n *node) Ping() (*craqrpc.AckResponse, error) {
	var reply craqrpc.AckResponse
	err := n.client.Call("RPC.Ping", &craqrpc.PingArgs{}, &reply)
	return &reply, err
}

func (n *node) Update(args *craqrpc.NodeMeta) (*craqrpc.AckResponse, error) {
	var reply craqrpc.AckResponse
	err := n.client.Call("RPC.Update", args, &reply)
	return &reply, err
}

func (n *node) ClientWrite(
	args *craqrpc.ClientWriteArgs,
) (*craqrpc.AckResponse, error) {
	var reply craqrpc.AckResponse
	err := n.client.Call("RPC.ClientWrite", args, &reply)
	return &reply, err
}

func (n node) Address() string   { return n.path }
func (n node) IsConnected() bool { return n.connected }

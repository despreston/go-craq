package coordinator

import (
	"net/rpc"
	"time"

	"github.com/despreston/go-craq/craqrpc"
)

type node struct {
	client *rpc.Client
	last   time.Time // last successful ping
	path   string
}

func (n *node) Connect() error {
	client, err := rpc.DialHTTP("tcp", n.Path())
	if err != nil {
		return err
	}
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

func (n *node) Path() string { return n.path }

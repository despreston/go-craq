package netrpc

import (
	"github.com/despreston/go-craq/transport"
)

func NewCoordinatorClient() transport.CoordinatorClient {
	return &CoordinatorClient{Client: &Client{}}
}

// CoordinatorBinding provides a layer of translation between the
// CoordinatorService which is transport agnostic and the net/rpc package. This
// allows using the net/rpc package to invoke CoordinatorService methods.
type CoordinatorBinding struct {
	Svc transport.CoordinatorService
}

func (c *CoordinatorBinding) AddNode(addr *string, r *transport.NodeMeta) error {
	meta, err := c.Svc.AddNode(*addr)
	if err != nil {
		return err
	}
	*r = *meta
	return nil
}

func (c *CoordinatorBinding) RemoveNode(addr *string, r *EmptyReply) error {
	return c.Svc.RemoveNode(*addr)
}

func (c *CoordinatorBinding) Write(args *ClientWriteArgs, r *EmptyReply) error {
	return c.Svc.Write(args.Key, args.Value)
}

// CoordinatorClient is for invoking net/rpc methods on a Coordinator.
type CoordinatorClient struct {
	*Client
}

func (cc *CoordinatorClient) AddNode(addr string) (*transport.NodeMeta, error) {
	reply := &transport.NodeMeta{}
	err := cc.Client.rpc.Call("RPC.AddNode", addr, reply)
	return reply, err
}

func (cc *CoordinatorClient) RemoveNode(addr string) error {
	return cc.Client.rpc.Call("RPC.RemoveNode", addr, &EmptyReply{})
}

func (cc *CoordinatorClient) Write(k string, v []byte) error {
	args := ClientWriteArgs{Key: k, Value: v}
	return cc.Client.rpc.Call("RPC.Write", &args, &EmptyReply{})
}

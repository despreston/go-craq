// client package is a control plane for a CRAQ chain.

package client

import (
	"log"
	"net/rpc"

	"github.com/despreston/go-craq/craqrpc"
)

type Client struct {
	cRPC, nRPC *rpc.Client // connections to coordinator and a node
}

func New(cdr, node string) (*Client, error) {
	c := &Client{}

	cRPC, err := rpc.DialHTTP("tcp", cdr)
	if err != nil {
		return c, err
	}

	nRPC, err := rpc.DialHTTP("tcp", node)
	if err != nil {
		return c, err
	}

	c.cRPC = cRPC
	c.nRPC = nRPC
	return c, nil
}

func (c *Client) Write(key string, val []byte) error {
	reply := craqrpc.AckResponse{}

	log.Printf("Write, key: %s, val: %s\n", key, string(val))

	args := craqrpc.ClientWriteArgs{
		Key:   key,
		Value: val,
	}

	return c.cRPC.Call("RPC.Write", &args, &reply)
}

func (c *Client) Read(key string) error {
	reply := craqrpc.ReadResponse{}
	args := craqrpc.ReadArgs{Key: key}

	err := c.nRPC.Call("RPC.Read", &args, &reply)
	if err != nil {
		return nil
	}

	log.Printf("key %s, value: %s", key, string(reply.Value))
	return nil
}

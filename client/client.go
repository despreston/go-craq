// client package is a CLI tool for sending messages to a CRAQ chain.

package client

import (
	"log"
	"net/rpc"

	"github.com/despreston/go-craq/craqrpc"
)

type Client struct {
	coordinator, port string
	cRPC              *rpc.Client // coordinator
	nRPC              *rpc.Client // node
}

func New(cdr, node, port string) (*Client, error) {
	c := &Client{}

	cRPC, err := rpc.DialHTTP("tcp", "127.0.0.1:"+cdr)
	if err != nil {
		return c, err
	}

	nRPC, err := rpc.DialHTTP("tcp", "127.0.0.1:"+node)
	if err != nil {
		return c, err
	}

	c.coordinator = cdr
	c.port = port
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

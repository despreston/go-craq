// netrpc package is a transporter client that uses golang's built-in net/rpc
// package for communication.
package netrpc

import (
	"net/rpc"
)

type Client struct {
	rpc *rpc.Client
}

func (c *Client) Connect(addr string) error {
	client, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		return err
	}
	c.rpc = client
	return nil
}

func (c *Client) Close() error {
	return c.rpc.Close()
}

// ----------------------------------------------------------------------------
// net/rpc argument and reply structs
type (
	EmptyArgs  struct{}
	EmptyReply struct{}

	CommitArgs struct {
		Key     string
		Version uint64
	}

	ClientWriteArgs struct {
		Key   string
		Value []byte
	}

	WriteArgs struct {
		Key     string
		Value   []byte
		Version uint64
	}

	VersionResponse struct {
		Key     string
		Version uint64
	}
)

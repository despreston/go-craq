// netrpc package is a transporter client that uses golang's built-in net/rpc
// package for communication.
package netrpc

import (
	"net/rpc"

	"github.com/despreston/go-craq/transport"
)

type Client struct{}

func (nrpc *Client) Connect(path string) (transport.Client, error) {
	return rpc.DialHTTP("tcp", path)
}

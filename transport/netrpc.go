package transport

import "net/rpc"

type NetRPC struct{}

func (nrpc *NetRPC) Connect(path string) (Client, error) {
	return rpc.DialHTTP("tcp", path)
}

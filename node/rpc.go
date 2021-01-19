package node

import (
	"log"

	"github.com/despreston/go-craq/craqrpc"
)

// RPC provides methods to be used as part of an RPC server for nodes. Other
// Nodes and the Coordinator can communicate with Nodes using these methods.
type RPC struct {
	n *Node
}

// Ping responds to ping messages. The coordinator should call this method via
// rpc to ensure the node is still functioning.
func (nRPC *RPC) Ping(_ *craqrpc.PingArgs, r *craqrpc.PingResponse) error {
	log.Println("replying to ping")
	r.Ok = true
	return nil
}

// AddNeighbor connects to args.Path and replaces the current neighbor at
// args.Pos.
func (nRPC *RPC) AddNeighbor(
	args *craqrpc.AddNeighborArgs,
	_ *craqrpc.AddNeighborResponse,
) error {
	return nRPC.n.connectToNode(args.Path, args.Pos)
}

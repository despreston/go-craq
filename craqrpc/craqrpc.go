// Package craqrpc provides message argument and response structs for rpc
// communication.

package craqrpc

// Position of neighbor node on the chain. Head nodes have no previous
// neighbors, and tail nodes have no next neighbors.
type NeighborPos int

const (
	NeighborPosPrev NeighborPos = iota
	NeighborPosNext
)

// AddNodeArgs are arguments for the AddNode RPC comamnd.
type AddNodeArgs struct {
	Path string // address for rpc.Client connection
}

// AddNodeResponse lets the Coordinator respond to the AddNode message with some
// metadata for the node to place itself into the correct position.
type AddNodeResponse struct {
	Head, Tail bool
	Prev, Next string // path to neighbors
}

// UpdateNodeArgs are the arguments for the RPC command for updating a node's
// metadata.
type UpdateNodeArgs struct {
	Head, Tail bool
	Prev, Next string
}

// UpdateNodeResponse is an RPC response that includes info for a node to position
// themselves properly in the chain.
type UpdateNodeResponse struct {
	Ok bool
}

// PingResponse is the reply for Ping RPC command.
type PingResponse struct {
	Ok bool
}

// PingArgs are arguments for Ping RPC command.
type PingArgs struct{}

// ChangeNeighborArgs are arguments for the ChangeNeighbor RPC command.
type ChangeNeighborArgs struct {
	Pos  NeighborPos // Position of neighbor relative to the node receiving msg
	Path string      // Host + port of neighbor
}

// ChangeNeighborResponse ...
type ChangeNeighborResponse struct{}

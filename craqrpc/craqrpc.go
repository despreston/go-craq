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

// ShuffleResponse is an RPC response meant to be sent to a node when the chain
// is shuffled.
type ShuffleResponse struct {
	Head, Tail bool
	Prev       string // path to previous node in the chain
}

// PingResponse is the reply for Ping RPC command.
type PingResponse struct {
	Ok bool
}

// PingArgs are arguments for Ping RPC command.
type PingArgs struct{}

// AddNeighborArgs are arguments for the AddNeighbor RPC command.
type AddNeighborArgs struct {
	Pos  NeighborPos // Position of neighbor relative to the node receiving msg
	Path string      // Host + port of neighbor
}

type AddNeighborResponse struct{}

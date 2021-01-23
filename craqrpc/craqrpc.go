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

// AckResponse is used to respond to RPC commands with a simple flag.
type AckResponse struct {
	Ok bool
}

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

// PingArgs are arguments for Ping RPC command.
type PingArgs struct{}

// ChangeNeighborArgs are arguments for the ChangeNeighbor RPC command.
type ChangeNeighborArgs struct {
	Pos  NeighborPos // Position of neighbor relative to the node receiving msg
	Path string      // Host + port of neighbor
}

// ChangeNeighborResponse ...
type ChangeNeighborResponse struct{}

// ClientWriteArgs are arguments a client should send to the Coordinator in
// order to initiate a new write. The Coordinator will forward the
// ClientWriteArgs to the head node.
type ClientWriteArgs struct {
	Key   string
	Value []byte
}

// WriteArgs are arguments used when one node wants to tell it's successor to
// write a new object to storage.
type WriteArgs struct {
	Key     string
	Value   []byte
	Version uint64
}

// MarkCleanArgs should be sent to a node to tell it to mark an object as clean.
type MarkCleanArgs struct {
	Key     string
	Version uint64
}

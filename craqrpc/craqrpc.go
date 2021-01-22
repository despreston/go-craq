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

// ClientWriteArgs are arguments a client should send to the Coordinator in
// order to initiate a new write. The Coordinator will forward the
// ClientWriteArgs to the head node.
type ClientWriteArgs struct {
	Key   string
	Value []byte
}

// ClientWriteResponse is the response from the coordinator after a request for
// a new write from a client.
type ClientWriteResponse struct {
	Ok bool
}

// WriteArgs are arguments used when one node wants to tell it's successor to
// write a new object to storage.
type WriteArgs struct {
	Key     string
	Value   []byte
	Version uint64
}

// WriteResponse ...
type WriteResponse struct {
	Ok bool
}

// MarkCleanArgs should be sent to a node to tell it to mark an object as clean.
type MarkCleanArgs struct {
	Key     string
	Version uint64
}

// MarkCleanResponse ...
type MarkCleanResponse struct {
	Ok bool
}

// Package craqrpc provides message argument and response structs for rpc
// communication.

package craqrpc

// Position of neighbor node on the chain. Head nodes have no previous
// neighbors, and tail nodes have no next neighbors.
type NeighborPos int

const (
	NeighborPosPrev NeighborPos = iota
	NeighborPosNext
	NeighborPosTail
)

type ValueVersion struct {
	Value   []byte
	Version uint64
}

// AckResponse is used to respond to RPC commands with a simple flag.
type AckResponse struct {
	Ok bool
}

// NodeMeta is for sending info to a node to let the node know where in the
// chain it sits. The node will update itself when receiving this message.
type NodeMeta struct {
	IsHead, IsTail   bool
	Prev, Next, Tail string // host + port to neighbors and tail node
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

// CommitArgs should be sent to a node to tell it to mark an object as
// committed. When an object is marked committed, it means it's been replicated
// on every available node in the chain.
type CommitArgs struct {
	Key     string
	Version uint64
}

// ReadResponse is the response a node will give when reading from the store.
type ReadResponse struct {
	Key   string
	Value []byte
}

// VersionResponse is the tail node's response to a node asking what the latest
// committed version of an item is.
type VersionResponse struct {
	Key     string
	Version uint64
}

// PropagateRequest is the request a node should send to the predecessor or
// successor (depending on whether it's forward or backward propagation) to ask
// for objects it needs in order to catch up with the rest of the chain.
type PropagateRequest map[string][]uint64

// PropagateResponse is the response a node should use in order to propagate
// unseen objects to the predecessor or successor.
type PropagateResponse map[string][]ValueVersion

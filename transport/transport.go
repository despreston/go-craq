// transport package are interfaces for the transport layer of the application.
// The purpose is to make it easy to swap one type of transport (e.g. net/rpc)
// for another (e.g. gRPC). The preferred mode of communication is RPC and so
// communication follows the remote-procedure paradigm, but nothing's stopping
// anyone from writing whatever implementation they prefer.

package transport

// Position of neighbor node on the chain. Head nodes have no previous
// neighbors, and tail nodes have no next neighbors.
type NeighborPos int

const (
	NeighborPosPrev NeighborPos = iota
	NeighborPosNext
	NeighborPosTail
)

// CoordinatorService is the API provided by the Coordinator.
type CoordinatorService interface {
	AddNode(address string) (*NodeMeta, error)
	Write(key string, value []byte) error
	RemoveNode(address string) error
}

// NodeService is the API provided by a Node.
type NodeService interface {
	Ping() error
	Update(meta *NodeMeta) error
	ClientWrite(key string, value []byte) error
	Write(key string, value []byte, version uint64) error
	LatestVersion(key string) (string, uint64, error)
	FwdPropagate(verByKey *PropagateRequest) (*PropagateResponse, error)
	BackPropagate(verByKey *PropagateRequest) (*PropagateResponse, error)
	Commit(key string, version uint64) error
	Read(key string) (string, []byte, error)
	ReadAll() (*[]Item, error)
}

// Client facilitates communication.
type Client interface {
	Close() error
	Connect(string) error
}

type NodeClient interface {
	Client
	NodeService
}

type CoordinatorClient interface {
	Client
	CoordinatorService
}

type NodeClientFactory func() NodeClient

// ----------------------------------------------------------------------------
// Message argument and reply types

// NodeMeta is for sending info to a node to let the node know where in the
// chain it sits. The node will update itself when receiving this message.
type NodeMeta struct {
	IsHead, IsTail   bool
	Prev, Next, Tail string // host + port to neighbors and tail node
}

// PropagateRequest is the request a node should send to the predecessor or
// successor (depending on whether it's forward or backward propagation) to ask
// for objects it needs in order to catch up with the rest of the chain.
type PropagateRequest map[string][]uint64

// PropagateResponse is the response a node should use in order to propagate
// unseen objects to the predecessor or successor.
type PropagateResponse map[string][]ValueVersion

type ValueVersion struct {
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

// VersionResponse is the tail node's response to a node asking what the latest
// committed version of an item is.
type VersionResponse struct {
	Key     string
	Version uint64
}

// Item is a single key/val pair.
type Item struct {
	Key   string
	Value []byte
}

type EmptyArgs struct{}
type EmptyReply struct{}

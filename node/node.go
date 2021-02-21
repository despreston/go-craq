// node package corresponds to what the CRAQ white paper refers to as a node.

package node

import (
	"errors"
	"log"
	"sync"

	"github.com/despreston/go-craq/craqrpc"
	"github.com/despreston/go-craq/store"
	"github.com/despreston/go-craq/transport"
)

// neighbor is another node in the chain
type neighbor struct {
	rpc     transport.NodeClient
	address string
}

// Opts is for passing options to the Node constructor.
type Opts struct {
	// Storage layer to use
	Store store.Storer
	// Local listening address
	Address string
	// Address to advertise to other nodes and coordinator
	PubAddress string
	// Address of coordinator
	CdrAddress string
	// Transport creates new clients for communication with other nodes
	Transport transport.NodeClientFactory
	// For communication with the Coordinator
	CoordinatorClient transport.CoordinatorClient
}

type commitEvent struct {
	Key     string
	Version uint64
}

// Node is what the white paper refers to as a node. This is the client that is
// responsible for storing data and handling reads/writes.
type Node struct {
	// Other nodes in the chain
	neighbors map[craqrpc.NeighborPos]neighbor
	// Storage layer
	store store.Storer
	// Latest version of a given key
	latest map[string]uint64
	// For listening to commit's. For testing.
	committed                    chan commitEvent
	cdrAddress, address, pubAddr string
	cdr                          transport.CoordinatorClient
	IsHead, IsTail               bool
	mu                           sync.Mutex
	transport                    func() transport.NodeClient
}

// New creates a new Node.
func New(opts Opts) *Node {
	return &Node{
		latest:     make(map[string]uint64),
		neighbors:  make(map[craqrpc.NeighborPos]neighbor, 3),
		cdrAddress: opts.CdrAddress,
		address:    opts.Address,
		store:      opts.Store,
		transport:  opts.Transport,
		pubAddr:    opts.PubAddress,
		cdr:        opts.CoordinatorClient,
	}
}

// ListenAndServe starts listening for messages and connects to the coordinator.
func (n *Node) Start() error {
	return n.connectToCoordinator()
}

// ConnectToCoordinator let's the Node announce itself to the chain coordinator
// to be added to the chain. The coordinator responds with a message to tell the
// Node if it's the head or tail, and with the address of the previous node in the
// chain and the address to the tail node. The Node announces itself to the
// neighbor using the address given by the coordinator.
func (n *Node) connectToCoordinator() error {
	err := n.cdr.Connect(n.cdrAddress)
	if err != nil {
		log.Println("Error connecting to the coordinator")
		return err
	}

	log.Printf("Connected to coordinator at %s\n", n.cdrAddress)

	// Announce self to the Coordinatorr
	reply, err := n.cdr.AddNode(n.pubAddr)
	if err != nil {
		log.Println(err.Error())
		return err
	}

	n.IsHead = reply.IsHead
	n.IsTail = reply.IsTail
	n.neighbors[craqrpc.NeighborPosTail] = neighbor{address: reply.Tail}

	// Connect to predecessor
	if reply.Prev != "" {
		if err := n.connectToNode(reply.Prev, craqrpc.NeighborPosPrev); err != nil {
			log.Printf("Failed to connect to node in ConnectToCoordinator. %v\n", err)
			return err
		}
		if err := n.fullPropagate(); err != nil {
			return err
		}
	} else if n.neighbors[craqrpc.NeighborPosPrev].address != "" {
		// Close the connection to the previous predecessor.
		n.neighbors[craqrpc.NeighborPosPrev].rpc.Close()
	}

	return nil
}

// send FwdPropagate and BackPropagate requests to new predecessor to get fully
// caught up. Forward propagation should go first so that it has all the dirty
// items needed before receiving backwards propagation response.
func (n *Node) fullPropagate() error {
	prevNeighbor := n.neighbors[craqrpc.NeighborPosPrev].rpc
	if err := n.requestFwdPropagation(prevNeighbor); err != nil {
		return err
	}
	return n.requestBackPropagation(prevNeighbor)
}

func (n *Node) connectToNode(address string, pos craqrpc.NeighborPos) error {
	if err := n.transport().Connect(address); err != nil {
		return err
	}

	log.Printf("connected to %s\n", address)

	// Disconnect from current neighbor if there's one connected.
	nbr := n.neighbors[pos]
	if nbr.rpc != nil {
		nbr.rpc.Close()
	}

	n.neighbors[pos] = neighbor{
		rpc:     n.transport(),
		address: address,
	}

	return nil
}

func (n *Node) writePropagated(reply *transport.PropagateResponse) error {
	// Save items from reply to store.
	for key, forKey := range *reply {
		for _, item := range forKey {
			if err := n.store.Write(key, item.Value, item.Version); err != nil {
				log.Printf("Failed to write item %+v to store: %#v\n", item, err)
				return err
			}
		}
	}
	return nil
}

// Commit the version to the store, update n.latest for this key, and announce
// the commit to the n.committed channel if there is one.
func (n *Node) commit(key string, version uint64) error {
	if err := n.store.Commit(key, version); err != nil {
		log.Printf("Failed to commit. Key: %s Version: %d Error: %#v", key, version, err)
		return err
	}

	n.latest[key] = version

	if n.committed != nil {
		n.committed <- commitEvent{Key: key, Version: version}
	}

	return nil
}

func (n *Node) commitPropagated(reply *transport.PropagateResponse) error {
	// Commit items from reply to store.
	for key, forKey := range *reply {
		for _, item := range forKey {
			// It's possible the item doesn't exist. In that case, add it first.
			// This sort of a poor man's upsert, but it saves from having to
			// deal w/ it in the storage layer, which should make it easier to
			// write new storers.
			if err := n.commit(key, item.Version); err != nil {
				if err == store.ErrNotFound {
					if err := n.store.Write(key, item.Value, item.Version); err != nil {
						return err
					}
					if err := n.commit(key, item.Version); err != nil {
						return err
					}
				}
				return err
			}
		}
	}
	return nil
}

func propagateRequestFromItems(items []*store.Item) *transport.PropagateRequest {
	req := transport.PropagateRequest{}
	for _, item := range items {
		req[item.Key] = append(req[item.Key], item.Version)
	}
	return &req
}

// requestFwdPropagation asks client to respond with all uncommitted (dirty)
// items that this node either does not have or are newer than what this node
// has.
func (n *Node) requestFwdPropagation(client transport.NodeClient) error {
	dirty, err := n.store.AllDirty()
	if err != nil {
		log.Printf("Failed to get all dirty items: %#v\n", err)
		return err
	}

	reply, err := client.FwdPropagate(propagateRequestFromItems(dirty))
	if err != nil {
		log.Printf("Failed during forward propagation: %#v\n", err)
		return err
	}

	return n.writePropagated(reply)
}

// requestBackPropagation asks client to respond with all committed items that
// this node either does not have or are newer than what this node has.
func (n *Node) requestBackPropagation(client transport.NodeClient) error {
	committed, err := n.store.AllCommitted()
	if err != nil {
		log.Printf("Failed to get all committed items: %#v\n", err)
		return err
	}

	reply, err := client.BackPropagate(propagateRequestFromItems(committed))
	if err != nil {
		log.Printf("Failed during back propagation: %#v\n", err)
		return err
	}

	return n.commitPropagated(reply)
}

// resetNeighbor closes any open connection and resets the neighbor.
func (n *Node) resetNeighbor(pos craqrpc.NeighborPos) {
	n.neighbors[pos].rpc.Close()
	n.neighbors[pos] = neighbor{}
}

// Ping responds to ping messages.
func (n *Node) Ping() error {
	return nil
}

func (n *Node) connectToPredecessor(address string) error {
	prev := n.neighbors[craqrpc.NeighborPosPrev]

	if prev.address == address {
		log.Println("New predecessor same address as last one, keeping conn.")
		return nil
	} else if address == "" {
		log.Println("Resetting predecessor")
		n.resetNeighbor(craqrpc.NeighborPosPrev)
		return nil
	}

	log.Printf("connecting to new predecessor %s\n", address)
	if err := n.connectToNode(address, craqrpc.NeighborPosPrev); err != nil {
		return err
	}

	prevC := n.neighbors[craqrpc.NeighborPosPrev].rpc
	return n.requestFwdPropagation(prevC)
}

func (n *Node) connectToSuccessor(address string) error {
	next := n.neighbors[craqrpc.NeighborPosNext]

	if next.address == address {
		log.Println("New successor same address as last one, keeping conn.")
		return nil
	} else if address == "" {
		log.Println("Resetting successor")
		n.resetNeighbor(craqrpc.NeighborPosNext)
		return nil
	}

	log.Printf("connecting to new successor %s\n", address)
	if err := n.connectToNode(address, craqrpc.NeighborPosNext); err != nil {
		return err
	}

	nextC := n.neighbors[craqrpc.NeighborPosNext].rpc
	return n.requestBackPropagation(nextC)
}

// Update is for updating a node's metadata. If new neighbors are given, the
// Node will disconnect from the current neighbors before connecting to the new
// ones. Coordinator uses this method to update metadata of the node when there
// is a failure or re-organization of the chain.
func (n *Node) Update(meta *transport.NodeMeta) error {
	log.Printf("Received metadata update: %+v\n", meta)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.IsHead = meta.IsHead
	n.IsTail = meta.IsTail

	if err := n.connectToPredecessor(meta.Prev); err != nil {
		return err
	}

	// connect to tail if address is different
	tail := n.neighbors[craqrpc.NeighborPosTail]
	if !meta.IsTail && tail.address != meta.Tail && meta.Tail != "" {
		err := n.connectToNode(meta.Tail, craqrpc.NeighborPosTail)
		if err != nil {
			return err
		}
	}

	if err := n.connectToSuccessor(meta.Next); err != nil {
		return err
	}

	// If this node is now the tail, commit all dirty versions, then forward
	// commits to predecessor.
	if meta.IsTail {
		dirty, err := n.store.AllDirty()
		if err != nil {
			log.Println("Error fetching all dirty items during node Update")
			return err
		}

		for i := range dirty {
			go func(item *store.Item) {
				if err := n.commitAndSend(item.Key, item.Version); err != nil {
					log.Printf(
						"Error during commit & send for item: %#v, error: %#v\n",
						item,
						err,
					)
				}
			}(dirty[i])
		}
	}

	return nil
}

// ClientWrite adds a new object to the chain and starts the process of
// replication.
func (n *Node) ClientWrite(key string, val []byte) error {
	// Increment version based off any existing objects for this key.
	var version uint64
	old, err := n.store.Read(key)
	if err == nil {
		version = old.Version + 1
	}

	if err := n.store.Write(key, val, version); err != nil {
		log.Printf("Failed to create during ClientWrite. %v\n", err)
		return err
	}

	log.Printf("Node RPC ClientWrite() created version %d of key %s\n", version, key)

	// Forward the new object to the successor node.

	next := n.neighbors[craqrpc.NeighborPosNext]

	// If there's no successor, it means this is the only node in the chain, so
	// mark the item as committed and return early.
	if next.address == "" {
		log.Println("No successor")
		if err := n.commit(key, version); err != nil {
			return err
		}
		return nil
	}

	if err := next.rpc.Write(key, val, version); err != nil {
		log.Printf("Failed to send to successor during ClientWrite. %v\n", err)
		return err
	}

	return nil
}

// Write adds an object to the chain. If the node is not the tail, the Write is
// forwarded to the next node in the chain. If the node is tail, the object is
// marked committed and a Commit message is sent to the predecessor in the
// chain.
func (n *Node) Write(key string, val []byte, version uint64) error {
	log.Printf("Node RPC Write() %s version %d to store\n", key, version)

	if err := n.store.Write(key, val, version); err != nil {
		log.Printf("Failed to write. %v\n", err)
		return err
	}

	// If this isn't the tail node, the write needs to be forwarded along the
	// chain to the next node.
	if !n.IsTail {
		next := n.neighbors[craqrpc.NeighborPosNext]
		if err := next.rpc.Write(key, val, version); err != nil {
			log.Printf("Failed to send to successor during Write. %v\n", err)
			return err
		}
		return nil
	}

	// At this point it's assumed this node is the tail.

	if err := n.commit(key, version); err != nil {
		log.Printf("Failed to mark as committed in Write. %v\n", err)
		return err
	}

	// Start telling predecessors to mark this version committed.
	n.sendCommitToPrev(key, version)
	return nil
}

// commitAndSend commits an item to the store and sends a message to the
// predecessor node to tell it to commit as well.
func (n *Node) commitAndSend(key string, version uint64) error {
	if err := n.commit(key, version); err != nil {
		return err
	}

	// if this node has a predecessor, send commit to previous node
	if n.neighbors[craqrpc.NeighborPosPrev].address != "" {
		return n.sendCommitToPrev(key, version)
	}

	return nil
}

func (n *Node) sendCommitToPrev(key string, version uint64) error {
	prev := n.neighbors[craqrpc.NeighborPosPrev]
	if err := prev.rpc.Commit(key, version); err != nil {
		log.Printf("Failed to send Commit to predecessor. %v\n", err)
		return err
	}
	return nil
}

// Commit marks an object as committed in storage.
func (n *Node) Commit(key string, version uint64) error {
	return n.commitAndSend(key, version)
}

// Read returns values from the store. If the store returns ErrDirtyItem, ask
// the tail for the latest committed version for this key. That ensures that
// every node in the chain returns the same version.
func (n *Node) Read(key string) (string, []byte, error) {
	item, err := n.store.Read(key)

	switch err {
	case store.ErrNotFound:
		return "", nil, errors.New("key doesn't exist")
	case store.ErrDirtyItem:
		v, err := n.getLatestVersion(key)

		if err != nil {
			log.Printf(
				"Failed to get latest version of %s from the tail. %v\n",
				key,
				err,
			)
			return "", nil, err
		}

		item, err = n.store.ReadVersion(key, v)
		if err != nil {
			return "", nil, err
		}
	}

	return key, item.Value, nil
}

func (n *Node) getLatestVersion(key string) (uint64, error) {
	_, v, err := n.neighbors[craqrpc.NeighborPosTail].rpc.LatestVersion(key)
	return v, err
}

// LatestVersion provides the latest committed version for a given key in the
// store.
func (n *Node) LatestVersion(key string) (string, uint64, error) {
	return key, n.latest[key], nil
}

// BackPropagate let's another node ask this node to send it all the committed
// items it has in it's storage. The node requesting back propagation should
// send the key + latest version of all committed items it has. This node
// responds with all committed items that: have a newer version, weren't
// included in the request.
func (n *Node) BackPropagate(
	verByKey *transport.PropagateRequest,
) (*transport.PropagateResponse, error) {
	unseen, err := n.store.AllNewerCommitted(map[string][]uint64(*verByKey))
	if err != nil {
		return nil, err
	}
	return makePropagateResponse(unseen), nil
}

// FwdPropagate let's another node ask this node to send it all the dirty items
// it has in it's storage. The node requesting forward propagation should send
// the key + latest version of all uncommitted items it has. This node responds
// with all uncommitted items that: have a newer version, weren't included in
// the request.
func (n *Node) FwdPropagate(
	verByKey *transport.PropagateRequest,
) (*transport.PropagateResponse, error) {
	unseen, err := n.store.AllNewerDirty(map[string][]uint64(*verByKey))
	if err != nil {
		return nil, err
	}
	return makePropagateResponse(unseen), nil
}

func makePropagateResponse(items []*store.Item) *transport.PropagateResponse {
	response := transport.PropagateResponse{}

	for _, item := range items {
		response[item.Key] = append(response[item.Key], transport.ValueVersion{
			Value:   item.Value,
			Version: item.Version,
		})
	}

	return &response
}

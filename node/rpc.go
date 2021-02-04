package node

import (
	"errors"
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
func (nRPC *RPC) Ping(_ *craqrpc.PingArgs, r *craqrpc.AckResponse) error {
	r.Ok = true
	return nil
}

func (nRPC *RPC) connectToPredecessor(path string) error {
	prev := nRPC.n.neighbors[craqrpc.NeighborPosPrev]

	if prev.path == path {
		return nil
	} else if path == "" {
		resetNeighbor(&prev)
		return nil
	}

	log.Printf("connecting to new predecessor %s\n", path)
	if err := nRPC.n.connectToNode(path, craqrpc.NeighborPosPrev); err != nil {
		return err
	}

	prevC := nRPC.n.neighbors[craqrpc.NeighborPosPrev].client
	return nRPC.n.requestFwdPropagation(&prevC)
}

func (nRPC *RPC) connectToSuccessor(path string) error {
	next := nRPC.n.neighbors[craqrpc.NeighborPosNext]

	if next.path == path {
		return nil
	} else if path == "" {
		resetNeighbor(&next)
		return nil
	}

	log.Printf("connecting to new successor %s\n", path)
	if err := nRPC.n.connectToNode(path, craqrpc.NeighborPosNext); err != nil {
		return err
	}

	nextC := nRPC.n.neighbors[craqrpc.NeighborPosNext].client
	return nRPC.n.requestBackPropagation(&nextC)
}

// Update is for updating a node's metadata. If new neighbors are given, the
// Node will disconnect from the current neighbors before connecting to the new
// ones. Coordinator uses this method to update metadata of the node when there
// is a failure or re-organization of the chain.
func (nRPC *RPC) Update(
	args *craqrpc.NodeMeta,
	reply *craqrpc.AckResponse,
) error {
	log.Printf("Received metadata update: %+v\n", args)

	nRPC.n.mu.Lock()
	defer nRPC.n.mu.Unlock()

	nRPC.n.isHead = args.IsHead
	nRPC.n.isTail = args.IsTail

	// connect to the predecessor if path is different
	if err := nRPC.connectToPredecessor(args.Prev); err != nil {
		return err
	}

	// connect to tail if path is different
	tail := nRPC.n.neighbors[craqrpc.NeighborPosTail]
	if !args.IsTail && tail.path != args.Tail && args.Tail != "" {
		err := nRPC.n.connectToNode(args.Tail, craqrpc.NeighborPosTail)
		if err != nil {
			return err
		}
	}

	if err := nRPC.connectToSuccessor(args.Next); err != nil {
		return err
	}

	// TODO: If this node is now the tail, commit all dirty versions, then forward
	// commits to predecessor.

	reply.Ok = true
	return nil
}

// ClientWrite adds a new object to the chain and starts the process of
// replication.
func (nRPC *RPC) ClientWrite(
	args *craqrpc.ClientWriteArgs,
	reply *craqrpc.AckResponse,
) error {
	// Increment version based off any existing objects for this key.
	var version uint64
	old, err := nRPC.n.store.Read(args.Key)
	if err == nil {
		version = old.Version + 1
	}

	if err := nRPC.n.store.Write(args.Key, args.Value, version); err != nil {
		log.Printf("Failed to create during ClientWrite. %v\n", err)
		return err
	}

	log.Printf("Created version %d of key %s\n", version, args.Key)

	// Forward the new object to the successor node.

	next := nRPC.n.neighbors[craqrpc.NeighborPosNext]

	// If there's no successor, it means this is the only node in the chain, so
	// mark the item as committed and return early.
	if next.path == "" {
		log.Println("No successor")
		if err := nRPC.n.store.Commit(args.Key, version); err != nil {
			return err
		}
		reply.Ok = true
		return nil
	}

	writeArgs := craqrpc.WriteArgs{
		Key:     args.Key,
		Value:   args.Value,
		Version: version,
	}

	err = next.client.Call("RPC.Write", &writeArgs, &craqrpc.AckResponse{})
	if err != nil {
		log.Printf("Failed to send to successor during ClientWrite. %v\n", err)
		return err
	}

	reply.Ok = true
	return nil
}

// Write adds an object to the chain. If the node is not the tail, the Write is
// forwarded to the next node in the chain. If the node is tail, the object is
// marked committed and a Commit message is sent to the predecessor in the
// chain.
func (nRPC *RPC) Write(
	args *craqrpc.WriteArgs,
	reply *craqrpc.AckResponse,
) error {
	log.Printf("Writing key %s version %d to store\n", args.Key, args.Version)

	if err := nRPC.n.store.Write(args.Key, args.Value, args.Version); err != nil {
		log.Printf("Failed to write. %v\n", err)
		return err
	}

	// If this isn't the tail node, the write needs to be forwarded along the
	// chain to the next node.
	if !nRPC.n.isTail {
		next := nRPC.n.neighbors[craqrpc.NeighborPosNext]

		err := next.client.Call("RPC.Write", &args, &craqrpc.AckResponse{})
		if err != nil {
			log.Printf("Failed to send to successor during Write. %v\n", err)
			return err
		}

		reply.Ok = true
		return nil
	}

	if err := nRPC.n.store.Commit(args.Key, args.Version); err != nil {
		log.Printf("Failed to mark as committed in Write. %v\n", err)
		return err
	}

	// Start telling predecessors to mark this version committed.
	nRPC.sendCommitToPrev(args.Key, args.Version)

	reply.Ok = true
	return nil
}

func (nRPC *RPC) sendCommitToPrev(key string, version uint64) error {
	err := nRPC.n.neighbors[craqrpc.NeighborPosPrev].client.Call(
		"RPC.Commit",
		&craqrpc.CommitArgs{Key: key, Version: version},
		&craqrpc.AckResponse{},
	)

	if err != nil {
		log.Printf("Failed to send Commit to predecessor. %v\n", err)
	}

	return err
}

// Commit marks an object as committed in storage.
func (nRPC *RPC) Commit(
	args *craqrpc.CommitArgs,
	reply *craqrpc.AckResponse,
) error {
	if err := nRPC.n.store.Commit(args.Key, args.Version); err != nil {
		return err
	}

	nRPC.n.latest[args.Key] = args.Version

	// if this node has a predecessor, send commit to previous node
	if nRPC.n.neighbors[craqrpc.NeighborPosPrev].path != "" {
		return nRPC.sendCommitToPrev(args.Key, args.Version)
	}

	return nil
}

// Read returns values from the store. If the store returns ErrDirtyItem, ask
// the tail for the latest committed version for this key. That ensures that
// every node in the chain returns the same version.
func (nRPC *RPC) Read(key string, reply *craqrpc.ReadResponse) error {
	item, err := nRPC.n.store.Read(key)

	switch err {
	case ErrNotFound:
		return errors.New("key doesn't exist")
	case ErrDirtyItem:
		v, err := nRPC.getLatestVersion(key)

		if err != nil {
			log.Printf(
				"Failed to get latest version of %s from the tail. %v\n",
				key,
				err,
			)
			return err
		}

		item, err = nRPC.n.store.ReadVersion(key, v)
		if err != nil {
			return err
		}
	}

	reply.Key = key
	reply.Value = item.Value
	return nil
}

func (nRPC *RPC) getLatestVersion(key string) (uint64, error) {
	var reply craqrpc.VersionResponse
	tail := nRPC.n.neighbors[craqrpc.NeighborPosTail]
	err := tail.client.Call("RPC.LatestVersion", key, &reply)
	return reply.Version, err
}

// LatestVersion provides the latest committed version for a given key in the
// store.
func (nRPC *RPC) LatestVersion(
	key string,
	reply *craqrpc.VersionResponse,
) error {
	reply.Key = key
	reply.Version = nRPC.n.latest[key]
	return nil
}

// BackPropagate let's another node ask this node to send it all the committed
// items it has in it's storage. The node requesting back propagation should
// send the key + latest version of all committed items it has. This node
// responds with all committed items that: have a newer version, weren't
// included in the request.
func (nRPC *RPC) BackPropagate(
	args *craqrpc.PropagateRequest,
	reply *craqrpc.PropagateResponse,
) error {
	unseen, err := nRPC.n.store.AllNewerCommitted(map[string][]uint64(*args))
	if err != nil {
		return err
	}
	*reply = makePropagateResponse(unseen)
	return nil
}

// FwdPropagate let's another node ask this node to send it all the dirty items
// it has in it's storage. The node requesting forward propagation should send
// the key + latest version of all uncommitted items it has. This node responds
// with all uncommitted items that: have a newer version, weren't included in
// the request.
func (nRPC *RPC) FwdPropagate(
	args *craqrpc.PropagateRequest,
	reply *craqrpc.PropagateResponse,
) error {
	unseen, err := nRPC.n.store.AllNewerDirty(map[string][]uint64(*args))
	if err != nil {
		return err
	}
	*reply = makePropagateResponse(unseen)
	return nil
}

func makePropagateResponse(items []*Item) craqrpc.PropagateResponse {
	response := craqrpc.PropagateResponse{}

	for _, item := range items {
		response[item.Key] = append(response[item.Key], craqrpc.ValueVersion{
			Value:   item.Value,
			Version: item.Version,
		})
	}

	return response
}

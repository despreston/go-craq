package node

import (
	"errors"
	"log"

	"github.com/despreston/go-craq/craqrpc"
	"golang.org/x/sync/errgroup"
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

// Update is for updating a node's metadata. If new neighbors are given, the
// Node will disconnect from the current neighbors before connecting to the new
// ones. Coordinator uses this method to update metadata of the node when there
// is a failure or re-organization of the chain.
func (nRPC *RPC) Update(
	args *craqrpc.NodeMeta,
	_ *craqrpc.AckResponse,
) error {
	log.Printf("Received metadata update: %+v\n", args)

	nRPC.n.mu.Lock()
	defer nRPC.n.mu.Unlock()

	errg := errgroup.Group{}

	nRPC.n.isHead = args.IsHead
	nRPC.n.isTail = args.IsTail
	nRPC.n.tail = args.Tail

	prev := nRPC.n.neighbors[craqrpc.NeighborPosPrev].path
	if prev != args.Prev && args.Prev != "" {
		errg.Go(func() error {
			return nRPC.n.connectToNode(args.Prev, craqrpc.NeighborPosPrev)
		})
	}

	next := nRPC.n.neighbors[craqrpc.NeighborPosNext].path
	if next != args.Next && args.Next != "" {
		errg.Go(func() error {
			return nRPC.n.connectToNode(args.Next, craqrpc.NeighborPosNext)
		})
	}

	return errg.Wait()
}

// ChangeNeighbor connects to args.Path and replaces the current neighbor at
// args.Pos. ChangeNeighbor provides a way for Nodes to announce themselves to
// their neighbors directly without having to go through the Coordinator.
func (nRPC *RPC) ChangeNeighbor(
	args *craqrpc.ChangeNeighborArgs,
	_ *craqrpc.ChangeNeighborResponse,
) error {
	nRPC.n.mu.Lock()
	defer nRPC.n.mu.Unlock()

	if nRPC.n.isHead && args.Pos == craqrpc.NeighborPosPrev {
		// Neighbor is predecessor so this node can't be head.
		log.Println("No longer head.")
		nRPC.n.isHead = false
	} else if nRPC.n.isTail && args.Pos == craqrpc.NeighborPosNext {
		// Neighbor is successor so this node can't be tail.
		log.Println("No longer tail.")
		nRPC.n.isTail = false
	}

	return nRPC.n.connectToNode(args.Path, args.Pos)
}

// ClientWrite adds a new object to the chain and starts the process of
// replication.
func (nRPC *RPC) ClientWrite(
	args *craqrpc.ClientWriteArgs,
	reply *craqrpc.AckResponse,
) error {
	// Increment version based off any existing objects for this key.
	old, _ := nRPC.n.store.Read(args.Key)
	version := old.Version + 1

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

	err := next.client.Call("RPC.Write", &writeArgs, &craqrpc.AckResponse{})
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
	if err := nRPC.n.store.Write(args.Key, args.Value, args.Version); err != nil {
		log.Printf("Failed to write. %v\n", err)
		return err
	}

	if !nRPC.n.isTail {
		// Forward to successor
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

	prev := nRPC.n.neighbors[craqrpc.NeighborPosPrev]
	mcArgs := craqrpc.CommitArgs{Key: args.Key, Version: args.Version}

	err := prev.client.Call(
		"RPC.Commit",
		&mcArgs,
		&craqrpc.AckResponse{},
	)

	if err != nil {
		log.Printf("Failed to send Commit to predecessor in Write. %v\n", err)
		return err
	}

	reply.Ok = true
	return nil
}

// Commit marks an object as committed in storage.
func (nRPC *RPC) Commit(
	args *craqrpc.CommitArgs,
	reply *craqrpc.AckResponse,
) error {
	log.Printf("marking committed k: %s, v: %v\n", args.Key, args.Version)
	return nRPC.n.store.Commit(args.Key, args.Version)
}

// Read returns values from the store.
func (nRPC *RPC) Read(
	args *craqrpc.ReadArgs,
	reply *craqrpc.ReadResponse,
) error {
	item, err := nRPC.n.store.Read(args.Key)

	switch err {
	case ErrNotFound:
		return errors.New("key doesn't exist")
	case ErrDirtyItem:
		// TODO: Get latest version from tail and then read that specific version.
	}

	reply.Key = args.Key
	reply.Value = item.Value
	return nil
}

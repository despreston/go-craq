package node

import (
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
func (nRPC *RPC) Ping(_ *craqrpc.PingArgs, r *craqrpc.PingResponse) error {
	log.Println("Replying to ping.")
	r.Ok = true
	return nil
}

// Update is for updating a node's metadata. If new neighbors are given, the
// Node will disconnect from the current neighbors before connecting to the new
// ones. Coordinator uses this method to update metadata of the node when there
// is a failure or re-organization of the chain.
func (nRPC *RPC) Update(
	args *craqrpc.UpdateNodeArgs,
	_ *craqrpc.UpdateNodeResponse,
) error {
	log.Printf("Received metadata update: %+v\n", args)

	nRPC.n.mu.Lock()
	defer nRPC.n.mu.Unlock()

	errg := errgroup.Group{}

	nRPC.n.head = args.Head
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

	if nRPC.n.head && args.Pos == craqrpc.NeighborPosPrev {
		// Neighbor is predecessor so this node can't be head.
		log.Println("No longer head.")
		nRPC.n.head = false
	} else if nRPC.n.tail && args.Pos == craqrpc.NeighborPosNext {
		// Neighbor is successor so this node can't be tail.
		log.Println("No longer tail.")
		nRPC.n.tail = false
	}

	return nRPC.n.connectToNode(args.Path, args.Pos)
}

package coordinator

import (
	"errors"
	"log"
	"time"

	"github.com/despreston/go-craq/craqrpc"
)

// RPC wraps around Coordinator for rpc calls.
type RPC struct {
	c *Coordinator
}

func ConnectToNode(node nodeDispatcher) error {
	return node.Connect()
}

// AddNode should be called by Nodes to announce themselves to the Coordinator.
// The coordinator then adds them to the end of the chain. The coordinator
// replies with some flags to let the node know if they're head or tail, and
// the path to the previous Node in the chain. The node is responsible for
// announcing itself to the previous Node in the chain.
func (cRPC *RPC) AddNode(
	args *craqrpc.AddNodeArgs,
	reply *craqrpc.NodeMeta,
) error {
	log.Printf("received AddNode from %s\n", args.Path)

	n := &node{
		last: time.Now(),
		path: args.Path,
	}

	if err := ConnectToNode(n); err != nil {
		log.Printf("failed to connect to node %s\n", args.Path)
		return err
	}

	cRPC.c.replicas = append(cRPC.c.replicas, n)
	cRPC.c.tail = n
	reply.IsTail = true
	reply.Tail = args.Path

	if len(cRPC.c.replicas) == 1 {
		cRPC.c.head = n
		reply.IsHead = true
	} else {
		reply.Prev = cRPC.c.replicas[len(cRPC.c.replicas)-2].Path()
	}

	// Because the tail node changed, all the other nodes need to be updated to
	// know where the tail is.
	for i := 0; i < len(cRPC.c.replicas)-1; i++ {
		go cRPC.c.updateNode(i)
	}

	return nil
}

// Write a new object to the chain.
func (cRPC *RPC) Write(
	args *craqrpc.ClientWriteArgs,
	reply *craqrpc.AckResponse,
) error {
	if len(cRPC.c.replicas) < 1 {
		reply.Ok = false
		return errors.New("no nodes in the chain")
	}

	// Forward the write to the head
	head := cRPC.c.replicas[0]
	result, err := head.ClientWrite(args)
	if err != nil || !result.Ok {
		reply.Ok = false
		return err
	}

	return nil
}

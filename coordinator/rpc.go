package coordinator

import (
	"errors"
	"log"
	"time"

	"github.com/despreston/go-craq/craqrpc"
)

var ErrEmptyChain = errors.New("no nodes in the chain")

// RPC wraps around Coordinator for rpc calls.
type RPC struct {
	c *Coordinator
}

// AddNode should be called by Nodes to announce themselves to the Coordinator.
// The coordinator then adds them to the end of the chain. The coordinator
// replies with some flags to let the node know if they're head or tail, and
// the path to the previous Node in the chain. The node is responsible for
// announcing itself to the previous Node in the chain.
func (cRPC *RPC) AddNode(
	path string,
	reply *craqrpc.NodeMeta,
) error {
	log.Printf("received AddNode from %s\n", path)

	n := &node{
		last:      time.Now(),
		path:      path,
		transport: cRPC.c.Transport,
	}

	if err := n.Connect(); err != nil {
		log.Printf("failed to connect to node %s\n", path)
		return err
	}

	cRPC.c.replicas = append(cRPC.c.replicas, n)
	cRPC.c.tail = n
	reply.IsTail = true
	reply.Tail = path

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
		return ErrEmptyChain
	}

	// Forward the write to the head
	head := cRPC.c.replicas[0]
	result, err := head.ClientWrite(args)
	if err != nil || !result.Ok {
		reply.Ok = false
		return err
	}

	reply.Ok = true
	return nil
}

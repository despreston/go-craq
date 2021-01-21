package coordinator

import (
	"log"
	"net/rpc"
	"time"

	"github.com/despreston/go-craq/craqrpc"
)

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
	args *craqrpc.AddNodeArgs,
	reply *craqrpc.AddNodeResponse,
) error {
	log.Printf("received AddNode from %s\n", args.Path)

	client, err := rpc.DialHTTP("tcp", args.Path)
	if err != nil {
		log.Printf("failed to dial client %s\n", args.Path)
		return err
	}

	n := &node{
		RPC:  client,
		last: time.Now(),
		Path: args.Path,
	}

	cRPC.c.replicas = append(cRPC.c.replicas, n)
	cRPC.c.tail = n
	reply.Tail = true

	if len(cRPC.c.replicas) == 1 {
		cRPC.c.head = n
		reply.Head = true
	} else {
		reply.Prev = cRPC.c.replicas[len(cRPC.c.replicas)-2].Path
	}

	return nil
}

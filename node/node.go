// node package corresponds to what the CRAQ white paper refers to as a node.

package node

import (
	"context"
	"log"
	"net/http"
	"net/rpc"

	"github.com/despreston/go-craq/coordinator"
	"github.com/despreston/go-craq/kv"
	"golang.org/x/sync/errgroup"
)

// RPC is a wrapper around Node for rpc calls.
type RPC struct {
	n *Node
}

// Node is what the white paper refers to as a node. This is the client that is
// responsible for storing data and handling reads/writes.
type Node struct {
	head    bool        // true if this node is head
	tail    bool        // true if this node is tail
	next    *rpc.Client // prev node in the chain
	prev    *rpc.Client // next node in the chain
	store   *kv.Store
	CdrPath string      // host + port to coordinator
	cdr     *rpc.Client // coordinator rpc client
	Path    string      // host + port for rpc communication
}

// ConnectToCoordinator bootstraps the Node by connecting and announcing itself
// to the coordinator. This also connects to any neighbor nodes.
func (n *Node) ConnectToCoordinator() error {
	cdrClient, err := rpc.DialHTTP("tcp", n.CdrPath)
	if err != nil {
		log.Println("error connecting to the coordinator")
		return err
	}

	log.Printf("connected to coordinator at %s\n", n.CdrPath)
	n.cdr = cdrClient

	// Announce self to the Coordinatorr
	reply := coordinator.AddNodeReply{}
	args := coordinator.AddNodeArgs{Path: n.Path}
	if err := cdrClient.Call("CoordinatorRPC.AddNode", args, &reply); err != nil {
		return err
	}

	n.head = reply.Head
	n.tail = reply.Tail

	if err := n.connectToNeighbors(reply.Prev, reply.Next); err != nil {
		return err
	}

	log.Println("connected to neighbors")
	return nil
}

func (n *Node) connectToNeighbors(prev, next string) error {
	errg := errgroup.Group{}

	if prev != "" {
		errg.Go(func() error {
			c, err := rpc.DialHTTP("tcp", prev)
			n.prev = c
			return err
		})
	}

	if next != "" {
		errg.Go(func() error {
			c, err := rpc.DialHTTP("tcp", next)
			n.next = c
			return err
		})
	}

	return errg.Wait()
}

// ListenAndServe starts listening for messages and connects to the coordinator.
func (n *Node) ListenAndServe() error {
	nRPC := &RPC{n}
	rpc.Register(nRPC)
	rpc.HandleHTTP()

	errg := errgroup.Group{}
	server := &http.Server{Addr: n.Path}

	errg.Go(server.ListenAndServe)

	errg.Go(func() error {
		err := n.ConnectToCoordinator()
		if err != nil {
			server.Shutdown(context.Background())
		}
		return err
	})

	return errg.Wait()
}

// Ping responds to ping messages. The coordinator should call this method via
// rpc to ensure the node is still functioning.
func (nRPC *RPC) Ping(
	_ *coordinator.PingArgs,
	reply *coordinator.PingReply,
) error {
	log.Println("replying to ping")
	reply.Ok = true
	return nil
}

// node package corresponds to what the CRAQ white paper refers to as a node.

package node

import (
	"context"
	"log"
	"net/http"
	"net/rpc"
	"sync"

	"github.com/despreston/go-craq/craqrpc"
	"github.com/despreston/go-craq/kv"
	"golang.org/x/sync/errgroup"
)

// Node is what the white paper refers to as a node. This is the client that is
// responsible for storing data and handling reads/writes.
type Node struct {
	neighbors map[craqrpc.NeighborPos]*rpc.Client
	head      bool // true if this node is head
	tail      bool // true if this node is tail
	// next      *rpc.Client // prev node in the chain
	// prev      *rpc.Client // next node in the chain
	store   *kv.Store
	CdrPath string      // host + port to coordinator
	cdr     *rpc.Client // coordinator rpc client
	Path    string      // host + port for rpc communication
	mu      sync.Mutex
}

// ListenAndServe starts listening for messages and connects to the coordinator.
func (n *Node) ListenAndServe() error {
	n.neighbors = make(map[craqrpc.NeighborPos]*rpc.Client, 2)

	nRPC := &RPC{n}
	rpc.Register(nRPC)
	rpc.HandleHTTP()

	errg := errgroup.Group{}
	server := &http.Server{Addr: n.Path}

	errg.Go(server.ListenAndServe)

	errg.Go(func() error {
		err := n.ConnectToCoordinator()
		if err != nil {
			log.Println(err.Error())
			server.Shutdown(context.Background())
		}
		return err
	})

	return errg.Wait()
}

// ConnectToCoordinator let's the Node announce itself to the chain coordinator
// to be added to the chain. The coordinator responds with a message to tell the
// Node if it's the head or tail, and with the path of the previous node in the
// chain. The Node announces itself to the neighbor using the path given by the
// coordinator.
func (n *Node) ConnectToCoordinator() error {
	cdrClient, err := rpc.DialHTTP("tcp", n.CdrPath)
	if err != nil {
		log.Println("error connecting to the coordinator")
		return err
	}

	log.Printf("connected to coordinator at %s\n", n.CdrPath)
	n.cdr = cdrClient

	// Announce self to the Coordinatorr
	reply := craqrpc.ShuffleResponse{}
	args := craqrpc.AddNodeArgs{Path: n.Path}
	if err := cdrClient.Call("RPC.AddNode", args, &reply); err != nil {
		return err
	}

	log.Printf("reply %+v\n", reply)
	n.head = reply.Head
	n.tail = reply.Tail

	if reply.Prev != "" {
		// If the neighbor is unreachable, swallow the error so this node doesn't
		// also fail.
		if err := n.connectToNode(reply.Prev, craqrpc.NeighborPosPrev); err == nil {
			announceToNeighbor(
				n.neighbors[craqrpc.NeighborPosPrev],
				n.Path,
				craqrpc.NeighborPosNext,
			)
		}
	} else if n.neighbors[craqrpc.NeighborPosPrev] != nil {
		// Close the connection to the previous predecessor.
		n.neighbors[craqrpc.NeighborPosPrev].Close()
	}

	return nil
}

func announceToNeighbor(
	c *rpc.Client,
	path string,
	pos craqrpc.NeighborPos,
) error {
	args := craqrpc.AddNeighborArgs{Pos: pos, Path: path}
	return c.Call("RPC.AddNeighbor", args, &craqrpc.AddNeighborResponse{})
}

func (n *Node) connectToNode(path string, pos craqrpc.NeighborPos) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	client, err := rpc.DialHTTP("tcp", path)
	if err != nil {
		return err
	}

	log.Printf("connected to neighbor %s\n", path)

	// Disconnect from current neighbor if there's one connected.
	neighbor := n.neighbors[pos]
	if neighbor != nil {
		neighbor.Close()
	}

	n.neighbors[pos] = client

	return nil
}

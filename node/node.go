// node package corresponds to what the CRAQ white paper refers to as a node.

package node

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/rpc"
	"sync"

	"github.com/despreston/go-craq/craqrpc"
	"golang.org/x/sync/errgroup"
)

var (
	// ErrNotFound should be returned by storage during a read operation if no
	// item exists for the given key.
	ErrNotFound = errors.New("that key does not exist")

	// ErrDirtyItem should be returned by storage if the latest version for the
	// key has not been committed yet.
	ErrDirtyItem = errors.New("key has an uncommitted version")
)

type neighbor struct {
	client *rpc.Client
	path   string
}

// Item is a meta data and value for an object in the Store. A key inside the
// store might have multiple versions of the same Item.
type Item struct {
	Version   uint64
	Committed bool
	Value     []byte
}

type storer interface {
	Read(string) (*Item, error)
	Write(string, []byte, uint64) error
	Commit(string, uint64) error
}

// Opts is for passing options to the Node constructor.
type Opts struct {
	Store   storer
	Path    string
	CdrPath string
}

// Node is what the white paper refers to as a node. This is the client that is
// responsible for storing data and handling reads/writes.
type Node struct {
	neighbors      map[craqrpc.NeighborPos]neighbor
	isHead, isTail bool
	tail           string      // path to the tail node
	store          storer      // storage layer
	CdrPath        string      // host + port to coordinator
	cdr            *rpc.Client // coordinator rpc client
	Path           string      // host + port for rpc communication
	mu             sync.Mutex
}

func New(opts Opts) *Node {
	return &Node{
		neighbors: make(map[craqrpc.NeighborPos]neighbor, 2),
		CdrPath:   opts.CdrPath,
		Path:      opts.Path,
		store:     opts.Store,
	}
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
// chain and the path to the tail node. The Node announces itself to the
// neighbor using the path given by the coordinator.
func (n *Node) ConnectToCoordinator() error {
	cdrClient, err := rpc.DialHTTP("tcp", n.CdrPath)
	if err != nil {
		log.Println("error connecting to the coordinator")
		return err
	}

	log.Printf("connected to coordinator at %s\n", n.CdrPath)
	n.cdr = cdrClient

	// Announce self to the Coordinatorr
	reply := craqrpc.NodeMeta{}
	args := craqrpc.AddNodeArgs{Path: n.Path}
	if err := cdrClient.Call("RPC.AddNode", args, &reply); err != nil {
		return err
	}

	log.Printf("reply %+v\n", reply)
	n.isHead = reply.IsHead
	n.isTail = reply.IsTail
	n.tail = reply.Tail

	if reply.Prev != "" {
		// If the neighbor is unreachable, swallow the error so this node doesn't
		// also fail.
		if err := n.connectToNode(reply.Prev, craqrpc.NeighborPosPrev); err == nil {
			announceToNeighbor(
				n.neighbors[craqrpc.NeighborPosPrev].client,
				n.Path,
				craqrpc.NeighborPosNext,
			)
		}
	} else if n.neighbors[craqrpc.NeighborPosPrev].path != "" {
		// Close the connection to the previous predecessor.
		n.neighbors[craqrpc.NeighborPosPrev].client.Close()
	}

	return nil
}

func announceToNeighbor(
	c *rpc.Client,
	path string,
	pos craqrpc.NeighborPos,
) error {
	args := craqrpc.ChangeNeighborArgs{Pos: pos, Path: path}
	return c.Call("RPC.ChangeNeighbor", args, &craqrpc.ChangeNeighborResponse{})
}

func (n *Node) connectToNode(path string, pos craqrpc.NeighborPos) error {
	log.Printf("connecting to %s\n", path)
	client, err := rpc.DialHTTP("tcp", path)
	if err != nil {
		return err
	}

	log.Printf("connected to neighbor %s\n", path)

	// Disconnect from current neighbor if there's one connected.
	nbr := n.neighbors[pos]
	if nbr.client != nil {
		nbr.client.Close()
	}

	n.neighbors[pos] = neighbor{
		client: client,
		path:   path,
	}

	return nil
}

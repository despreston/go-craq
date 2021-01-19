// coordinator package is manages the state of the chain. The Coordinator is
// responsible for detecting and handling node failures, electing head and tail
// nodes, adding new nodes to the chain. The Coordinator runs as a single
// process but could be replaced by a more resilient consensus service like
// Paxos or Raft.
//
// If the Coordinator process fails but the chain is still intact, reads and
// writes are still possible. If the Coordinator fails and the head or tail
// nodes fail, reads made up to the time of failure should still be available.

package coordinator

import (
	"log"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"github.com/despreston/go-craq/craqrpc"
)

const (
	pingTimeout  = 5 // secs
	pingInterval = 3 // secs
)

type node struct {
	RPC  *rpc.Client
	last time.Time // last successful ping
	Path string
}

// Coordinator is responsible for tracking the Nodes in the chain.
type Coordinator struct {
	Path     string
	head     *node
	tail     *node
	mu       sync.Mutex
	replicas []*node
}

// ListenAndServe registers this node with the coordinator and begins listening
// for messages.
func (cdr *Coordinator) ListenAndServe() error {
	cRPC := &RPC{c: cdr}
	rpc.Register(cRPC)
	rpc.HandleHTTP()
	go cdr.pingReplicas()
	log.Printf("listening at %s\n", cdr.Path)
	return http.ListenAndServe(cdr.Path, nil)
}

// Ping each node. If the response is !Ok or the pingTimeout is reached, remove
// the node from the list of replicas. This method should be called shortly
// after creating a new Coordinator.
func (cdr *Coordinator) pingReplicas() {
	log.Println("starting pinging")

	for {
		for i, n := range cdr.replicas {
			go func(n *node, i int) {
				log.Printf("pinging replica %d\n", i)

				var reply craqrpc.PingResponse
				pingCh := n.RPC.Go("RPC.Ping", &craqrpc.PingArgs{}, &reply, nil)

				select {
				case <-pingCh.Done:
					if !reply.Ok {
						cdr.removeNode(n)
					}
				case <-time.After(pingTimeout * time.Second):
					cdr.removeNode(n)
				}
			}(n, i)
		}

		time.Sleep(pingInterval * time.Second)
	}
}

// Removes the node from the list of replicas.
func (cdr *Coordinator) removeNode(n *node) {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	log.Printf("removing node %s", n.Path)

	var idx int
	for i, node := range cdr.replicas {
		if node == n {
			idx = i
			break
		}
	}

	// If the node is head, send message to head+1 node to tell it to be head.
	if n == cdr.head && len(cdr.replicas) > 1 {
		cdr.setHead(cdr.replicas[idx+1])
	}

	// If the node is tail, send message to tail-1 node to tell it to be tail.
	if n == cdr.tail && len(cdr.replicas) > 1 {
		cdr.setTail(cdr.replicas[idx-1])
	}

	cdr.replicas = append(cdr.replicas[:idx], cdr.replicas[idx+1:]...)
}

// Send a message to node to tell it that it's now the head. Update cdr.head on
// successful ack.
func (cdr *Coordinator) setHead(n *node) {}

// Send a message to node to tell it that it's now the tail. Update cdr.tail on
// successful ack.
func (cdr *Coordinator) setTail(n *node) {}

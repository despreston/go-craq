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

func (cdr *Coordinator) findReplicaIndex(n *node) (int, bool) {
	for i, replica := range cdr.replicas {
		if replica == n {
			return i, true
		}
	}
	return 0, false
}

func (cdr *Coordinator) removeNode(n *node) {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	idx, found := cdr.findReplicaIndex(n)
	if !found {
		return
	}

	cdr.replicas = append(cdr.replicas[:idx], cdr.replicas[idx+1:]...)
	log.Printf("removed node %s", n.Path)

	// No more nodes in the chain.
	if len(cdr.replicas) < 1 {
		return
	}

	// After removing the node, the successor, if there was one, now sits at idx
	// in the chain. Send a message to that node to update it's metadata.
	if len(cdr.replicas) > idx {
		err := cdr.updateNode(idx)
		if err != nil {
			log.Printf("Failed to update successor: %s\n", err.Error())
			return
		}
	}

	// If there's more than 1 replica now it means there was a predecessor to the
	// node that was removed. Need to send updated metadata to that node.
	if len(cdr.replicas) > 1 {
		err := cdr.updateNode(idx - 1)
		if err != nil {
			log.Printf("Failed to update predecessor: %v\n", err)
			return
		}
	}
}

// updateNode sends the latest metadata to a Node to tell it whether it's head
// or tail and what it's neighbors' paths are.
func (cdr *Coordinator) updateNode(i int) error {
	n := cdr.replicas[i]

	log.Printf("Sending metadata to %s.\n", n.Path)

	var reply craqrpc.UpdateNodeResponse
	var args craqrpc.UpdateNodeArgs

	args.Head = i == 0
	args.Tail = len(cdr.replicas) == i+1

	if len(cdr.replicas) > 1 {
		if i > 0 {
			// Not the first node, so add path to previous.
			args.Prev = cdr.replicas[i-1].Path
		}
		if i+1 != len(cdr.replicas) {
			// Not the last node, so add path to next.
			args.Next = cdr.replicas[i+1].Path
		}
	}

	// call Update method on node
	err := n.RPC.Call("RPC.Update", &args, &reply)
	if err != nil || !reply.Ok {
		return err
	}

	return nil
}

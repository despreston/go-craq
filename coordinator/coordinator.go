// coordinator package manages the state of the chain. The Coordinator is
// responsible for detecting and handling node failures, electing head and tail
// nodes, and adding new nodes to the chain.
//
// If the Coordinator process fails but the chain is still intact, reads would
// still be possible. Writes will not be possible because all writes are first
// sent to the Coordinator. The coordinator forwards write requests to the head
// node.

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
	pingTimeout  = 5 * time.Second
	pingInterval = 3 * time.Second
)

type nodeDispatcher interface {
	Connect() error
	Path() string
	Ping() (*craqrpc.AckResponse, error)
	Update(*craqrpc.NodeMeta) (*craqrpc.AckResponse, error)
	ClientWrite(*craqrpc.ClientWriteArgs) (*craqrpc.AckResponse, error)
}

// Coordinator is responsible for tracking the Nodes in the chain.
type Coordinator struct {
	Path     string
	head     *node
	tail     *node
	mu       sync.Mutex
	replicas []nodeDispatcher
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
		for _, n := range cdr.replicas {
			go func(n nodeDispatcher) {
				doneCh := make(chan bool, 1)
				result, err := n.Ping()
				doneCh <- true

				select {
				case <-doneCh:
					if err != nil || !result.Ok {
						cdr.removeNode(n)
					}
				case <-time.After(pingTimeout):
					cdr.removeNode(n)
				}
			}(n)
		}
		time.Sleep(pingInterval)
	}
}

func (cdr *Coordinator) findReplicaIndex(n nodeDispatcher) (int, bool) {
	for i, replica := range cdr.replicas {
		if replica == n {
			return i, true
		}
	}
	return 0, false
}

func (cdr *Coordinator) removeNode(n nodeDispatcher) {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	idx, found := cdr.findReplicaIndex(n)
	if !found {
		return
	}

	wasTail := idx == len(cdr.replicas)-1
	cdr.replicas = append(cdr.replicas[:idx], cdr.replicas[idx+1:]...)
	log.Printf("removed node %s", n.Path())

	// No more nodes in the chain.
	if len(cdr.replicas) < 1 {
		return
	}

	if wasTail {
		// Because the tail node changed, all the other nodes need to be updated to
		// know where the tail is.
		for i := 0; i < len(cdr.replicas); i++ {
			go cdr.updateNode(i)
		}
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

	// Send update to predecessor
	if idx > 0 {
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

	log.Printf("Sending metadata to %s.\n", n.Path())

	var args craqrpc.NodeMeta

	args.IsHead = i == 0
	args.IsTail = len(cdr.replicas) == i+1
	args.Tail = cdr.replicas[len(cdr.replicas)-1].Path()

	if len(cdr.replicas) > 1 {
		if i > 0 {
			// Not the first node, so add path to previous.
			args.Prev = cdr.replicas[i-1].Path()
		}
		if i+1 != len(cdr.replicas) {
			// Not the last node, so add path to next.
			args.Next = cdr.replicas[i+1].Path()
		}
	}

	// call Update method on node
	reply, err := n.Update(&args)
	if err != nil {
		return err
	}

	if !reply.Ok {
		log.Printf("Update reply from node %d was not OK.\n", i)
	}

	return nil
}

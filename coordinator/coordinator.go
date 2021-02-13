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
	"github.com/despreston/go-craq/transport"
)

const (
	pingTimeout  = 5 * time.Second
	pingInterval = 3 * time.Second
)

type nodeDispatcher interface {
	Connect() error
	Address() string
	Ping() (*craqrpc.AckResponse, error)
	Update(*craqrpc.NodeMeta) (*craqrpc.AckResponse, error)
	ClientWrite(*craqrpc.ClientWriteArgs) (*craqrpc.AckResponse, error)
	IsConnected() bool
}

// Coordinator is responsible for tracking the Nodes in the chain.
type Coordinator struct {
	// Local listening address
	Address string
	// Transport layer to use for communication with nodes
	Transport transport.Transporter
	head      nodeDispatcher
	tail      nodeDispatcher
	mu        sync.Mutex
	replicas  []nodeDispatcher
}

// ListenAndServe registers RPC methods, starts the RPC server, and begins
// pinging all connected nodes.
func (cdr *Coordinator) ListenAndServe() error {
	cRPC := &RPC{c: cdr}
	rpc.Register(cRPC)
	rpc.HandleHTTP()
	go cdr.pingReplicas()
	log.Printf("listening at %s\n", cdr.Address)
	return http.ListenAndServe(cdr.Address, nil)
}

// Ping each node. If the response is !Ok or the pingTimeout is reached, remove
// the node from the list of replicas. This method should be called shortly
// after creating a new Coordinator.
func (cdr *Coordinator) pingReplicas() {
	log.Println("starting pinging")
	for {
		for _, n := range cdr.replicas {
			go func(n nodeDispatcher) {
				resultCh := make(chan bool, 1)

				go func() {
					result, err := n.Ping()
					resultCh <- (result.Ok && err == nil)
				}()

				select {
				case ok := <-resultCh:
					if !ok {
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

func findReplicaIndex(address string, replicas []nodeDispatcher) (int, bool) {
	for i, replica := range replicas {
		if replica.Address() == address {
			return i, true
		}
	}
	return 0, false
}

func (cdr *Coordinator) updateAll() {
	wg := sync.WaitGroup{}
	for i := 0; i < len(cdr.replicas); i++ {
		wg.Add(1)
		go func(i int) {
			cdr.updateNode(i)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

type addressReader interface {
	Address() string
}

func (cdr *Coordinator) removeNode(n addressReader) {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	idx, found := findReplicaIndex(n.Address(), cdr.replicas)
	if !found {
		return
	}

	wasTail := idx == len(cdr.replicas)-1
	cdr.replicas = append(cdr.replicas[:idx], cdr.replicas[idx+1:]...)
	log.Printf("removed node %s", n.Address())

	if wasTail {
		cdr.tail = nil
		if idx > 0 {
			cdr.tail = cdr.replicas[idx-1]
		}

		// Because the tail node changed, all the other nodes need to be updated to
		// know where the tail is.
		cdr.updateAll()
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

	// Send update to predecessor and update the tail
	if idx > 0 {
		err := cdr.updateNode(idx - 1)
		if err != nil {
			log.Printf("Failed to update predecessor: %v\n", err)
			return
		}
	}
}

// updateNode sends the latest metadata to a Node to tell it whether it's head
// or tail and what it's neighbors' addresses are.
func (cdr *Coordinator) updateNode(i int) error {
	n := cdr.replicas[i]

	log.Printf("Sending metadata to %s.\n", n.Address())

	var args craqrpc.NodeMeta

	args.IsHead = i == 0
	args.IsTail = len(cdr.replicas) == i+1
	args.Tail = cdr.replicas[len(cdr.replicas)-1].Address()

	if len(cdr.replicas) > 1 {
		if i > 0 {
			// Not the first node, so add address to previous.
			args.Prev = cdr.replicas[i-1].Address()
		}
		if i+1 != len(cdr.replicas) {
			// Not the last node, so add address to next.
			args.Next = cdr.replicas[i+1].Address()
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

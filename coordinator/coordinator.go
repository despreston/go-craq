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
	"errors"
	"log"
	"sync"
	"time"

	"github.com/despreston/go-craq/transport"
)

const (
	pingTimeout  = 5 * time.Second
	pingInterval = 1 * time.Second
)

var ErrEmptyChain = errors.New("no nodes in the chain")

// Coordinator is responsible for tracking the Nodes in the chain.
type Coordinator struct {
	tport      transport.NodeClientFactory
	head, tail *node
	mu         sync.Mutex
	replicas   []*node

	// For testing the AddNode method. This WaitGroup is done when updates have
	// been sent to all nodes.
	Updates *sync.WaitGroup
}

func New(t transport.NodeClientFactory) *Coordinator {
	return &Coordinator{
		Updates: &sync.WaitGroup{},
		tport:   t,
	}
}

func (cdr *Coordinator) Start() {
	cdr.pingReplicas()
}

// Ping each node. If the response returns an error or the pingTimeout is
// reached, remove the node from the list of replicas.
func (cdr *Coordinator) pingReplicas() {
	log.Println("starting pinging")
	for {
		for _, n := range cdr.replicas {
			go func(n *node) {
				resultCh := make(chan bool, 1)

				go func() {
					err := n.rpc.Ping()
					resultCh <- err == nil
				}()

				select {
				case ok := <-resultCh:
					if !ok {
						cdr.RemoveNode(n.Address())
					}
				case <-time.After(pingTimeout):
					cdr.RemoveNode(n.Address())
				}
			}(n)
		}
		time.Sleep(pingInterval)
	}
}

func findReplicaIndex(address string, replicas []*node) (int, bool) {
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

func (cdr *Coordinator) RemoveNode(address string) error {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	idx, found := findReplicaIndex(address, cdr.replicas)
	if !found {
		return errors.New("unknown node")
	}

	wasTail := idx == len(cdr.replicas)-1
	cdr.replicas = append(cdr.replicas[:idx], cdr.replicas[idx+1:]...)
	log.Printf("removed node %s", address)

	if wasTail {
		cdr.tail = nil
		if idx > 0 {
			cdr.tail = cdr.replicas[idx-1]
		}

		// Because the tail node changed, all the other nodes need to be updated to
		// know where the tail is.
		cdr.updateAll()
		return nil
	}

	// After removing the node, the successor, if there was one, now sits at idx
	// in the chain. Send a message to that node to update it's metadata.
	if len(cdr.replicas) > idx {
		err := cdr.updateNode(idx)
		if err != nil {
			log.Printf("Failed to update successor: %s\n", err.Error())
			return err
		}
	}

	// Send update to predecessor and update the tail
	if idx > 0 {
		err := cdr.updateNode(idx - 1)
		if err != nil {
			log.Printf("Failed to update predecessor: %v\n", err)
			return err
		}
	}

	return nil
}

// updateNode sends the latest metadata to a Node to tell it whether it's head
// or tail and what it's neighbors' addresses are.
func (cdr *Coordinator) updateNode(i int) error {
	n := cdr.replicas[i]

	log.Printf("Sending metadata to %s.\n", n.Address())

	var args transport.NodeMeta
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
	if err := n.rpc.Update(&args); err != nil {
		return err
	}

	return nil
}

// AddNode should be called by Nodes to announce themselves to the Coordinator.
// The coordinator then adds them to the end of the chain. The coordinator
// replies with some flags to let the node know if they're head or tail, and
// the address to the previous Node in the chain. The node is responsible for
// announcing itself to the previous Node in the chain.
func (cdr *Coordinator) AddNode(address string) (*transport.NodeMeta, error) {
	log.Printf("received AddNode from %s\n", address)

	n := &node{
		last:    time.Now(),
		address: address,
		rpc:     cdr.tport(),
	}

	if err := n.Connect(); err != nil {
		log.Printf("failed to connect to node %s\n", address)
		return nil, err
	}

	cdr.replicas = append(cdr.replicas, n)
	cdr.tail = n
	meta := &transport.NodeMeta{}
	meta.IsTail = true
	meta.Tail = address

	if len(cdr.replicas) == 1 {
		cdr.head = n
		meta.IsHead = true
	} else {
		meta.Prev = cdr.replicas[len(cdr.replicas)-2].Address()
	}

	// Because the tail node changed, all the other nodes need to be updated to
	// know where the tail is.
	for i := 0; i < len(cdr.replicas)-1; i++ {
		cdr.Updates.Add(1)
		go func(i int) {
			cdr.updateNode(i)
			cdr.Updates.Done()
		}(i)
	}

	return meta, nil
}

// Write a new object to the chain.
func (cdr *Coordinator) Write(key string, value []byte) error {
	if len(cdr.replicas) < 1 {
		return ErrEmptyChain
	}

	// Forward the write to the head
	head := cdr.replicas[0]
	return head.rpc.ClientWrite(key, value)
}

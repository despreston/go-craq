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
	"time"
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

// AddNodeArgs are arguments for AddNode RPC call
type AddNodeArgs struct {
	Path string // address for rpc.Client connection
}

// AddNodeReply is the reply for the AddNode RPC command.
type AddNodeReply struct {
	Head, Tail bool
	Prev, Next string // paths to neighbors
}

// PingReply is the reply for Ping RPC command.
type PingReply struct {
	Ok bool
}

// PingArgs are arguments for Ping RPC command.
type PingArgs struct{}

// Coordinator is responsible for tracking the Nodes in the chain.
type Coordinator struct {
	Path     string
	head     *node
	tail     *node
	replicas []*node
}

// RPC wraps around Coordinator for rpc calls.
type RPC struct {
	cdr *Coordinator
}

// ListenAndServe registers this node with the coordinator and begins listening
// for messages.
func (cdr *Coordinator) ListenAndServe() error {
	cdrRPC := &RPC{cdr}
	rpc.Register(cdrRPC)
	rpc.HandleHTTP()
	go cdr.pingReplicas()
	log.Printf("listening at %s\n", cdr.Path)
	return http.ListenAndServe(cdr.Path, nil)
}

// AddNode should be called by Nodes to announce themselves to the Coordinator.
// The coordinator then adds them to the end of the chain. The coordinator
// replies with some flags to let the node know if they're head or tail, and
// also their prev and next neighbors, if any. The coordinator also sends a
// message to the existing last node in the chain to update that node's next
// neighbor.
func (cdrRPC *RPC) AddNode(args *AddNodeArgs, reply *AddNodeReply) error {
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

	cdrRPC.cdr.replicas = append(cdrRPC.cdr.replicas, n)
	cdrRPC.cdr.tail = n

	if len(cdrRPC.cdr.replicas) == 1 {
		cdrRPC.cdr.head = n
		reply.Head = true
	}

	return nil
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

				var reply PingReply
				pingCh := n.RPC.Go("NodeRPC.Ping", &PingArgs{}, &reply, nil)

				select {
				case <-pingCh.Done:
					if !reply.Ok {
						cdr.removeNode(i)
					}
				case <-time.After(pingTimeout * time.Second):
					cdr.removeNode(i)
				}
			}(n, i)
		}

		time.Sleep(pingInterval * time.Second)
	}
}

// Removes the node from the list of replicas.
func (cdr *Coordinator) removeNode(i int) {
	log.Printf("removing node %s", cdr.replicas[i].Path)

	n := cdr.replicas[i]

	// If the node is head, send message to head+1 node to tell it to be head.
	if n == cdr.head && len(cdr.replicas) > 1 {
		cdr.promoteToHead(cdr.replicas[i+1])
	}

	// If the node is tail, send message to tail-1 node to tell it to be tail.
	if n == cdr.tail && len(cdr.replicas) > 1 {
		cdr.promoteToTail(cdr.replicas[i-1])
	}

	cdr.replicas = append(cdr.replicas[:i], cdr.replicas[i+1:]...)
}

// Send a message to node to tell it that it's now the head. Update cdr.head on
// successful ack.
func (cdr *Coordinator) promoteToHead(n *node) {}

// Send a message to node to tell it that it's now the tail. Update cdr.tail on
// successful ack.
func (cdr *Coordinator) promoteToTail(n *node) {}

# go-craq [![Test Status](https://github.com/despreston/go-craq/workflows/Test/badge.svg)](https://github.com/despreston/go-craq/actions)

Package `go-craq` implements CRAQ (Chain Replication with Apportioned Queries)
as described in [the CRAQ
paper](https://pdos.csail.mit.edu/6.824/papers/craq.pdf). MIT Licensed.

CRAQ is a replication protocol that allows reads from any replica while still
maintaining strong consistency. CRAQ _should_ provide better read throughput
than Raft and Paxos. Read performance grows linearly with the number of nodes
added to the system. Network chatter is significantly lower compared to Raft and
Paxos.

```
            +------------------+
            |                  |
      +-----+   Coordinator    |
      |     |                  |
Write |     +------------------+
      |
      v
  +---+----+     +--------+     +--------+
  |        +---->+        +---->+        |
  |  Node  |     |  Node  |     |  Node  |
  |        +<----+        +<----+        |
  +---+-+--+     +---+-+--+     +---+-+--+
      ^ |            ^ |            ^ |
 Read | |       Read | |       Read | |
      | |            | |            | |
      + v            + v            + v
```

## Processes
### Coordinator
Facilitates new writes to the chain; allows nodes to announce themselves to the
chain; manages the order of the nodes of the chain. One Coordinator should be
run for each chain. For better resiliency, you _could_ run a cluster of
Coordinators and use something like Raft or Paxos for leader election, but
that's outside the scope of this project.

### Node
Represents a single node in the chain. Responsible for storing writes, serving
reads, and forwarding messages along the chain. In practice, you would probably
have a single Node process running on a machine. Each Node should have it's own
storage unit.

### Client
Basic CLI tool for interacting with the chain. Allows writes and reads. The one
included in this project uses the net/rpc package as the transport layer and
bbolt as the storage layer.

## Communication
_go_craq_ processes communicate via RPC using the built-in net/rpc package from
the Golang stdlib.

Common structs used for RPC method arguments and replies can be seen
[here](craqrpc/craqrpc.go)

## Storage
_go-craq_ is designed to make it easy to swap the persistance layer. CRAQ is
flexible and any storage unit that implements the `Storer` interface in
[store/store.go](store/store.go) can be used. Some implementations for common
storage projects can be found in the `store` package.

## Reading the Code
There are several places to start that'll give you a great understanding of how
things work.

`ConnectToCoordinator` method in [node/node.go](node/node.go). This is the
method the Node must run during startup to connect to the Coordinator and
announce itself to the chain. The Coordinator responds with some metadata about
where in the chain the node is now located. The node uses this info to connect
to the predecessor in the chain.

`Update` method in [node/rpc.go](node/rpc.go). This is the method the
Coordinator uses to update the node's metadata. New data is sent to the node if
the node's predecessor or successor changes, and if the address of the tail node
changes.

`ClientWrite` method in [node/rpc.go](node/rpc.go). This is the method the
Coordinator uses to send writes to the head node. This is where the chain begins
the process of propagation.

## FAQ
### What happens during a write?
A write request containing the key and value are sent to the Coordinator via the
Coordinator's `Write` RPC method. If the chain is not empty, the Coordinator
will pass the write request to the head node via the node's `ClientWrite`
method.

The head node receives the key and value. The node looks up the key in it's
store to determine what the latest version should be. If the key already exists,
the version of the new write will be the existing version incremented by one.
The new value is written to the store. If the node is not connected to another
node in the chain, it commits the version. If the node does have a successor,
the key, value, and version are forwarded to the next node in the chain via the
successor's `Write` RPC method.

The key, value, and version are passed along the chain one-by-one. Each node
adds the item to the store and sends a message to the successor in the chain.
When the write reaches the tail node, the tail marks the item as committed. The
tail sends a `Commit` RPC method to it's predecessor. The tail's predecessor
commits that version of the item, then continues to forward the `Commit` message
backwards through the chain, one node at a time, until every node has committed
the version.

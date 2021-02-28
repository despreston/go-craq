# go-craq [![Test Status](https://github.com/despreston/go-craq/workflows/Test/badge.svg)](https://github.com/despreston/go-craq/actions)  [![Go Reference](https://pkg.go.dev/badge/github.com/despreston/go-craq.svg)](https://pkg.go.dev/github.com/despreston/go-craq)

Package `go-craq` implements CRAQ (Chain Replication with Apportioned Queries)
as described in [the CRAQ paper](https://pdos.csail.mit.edu/6.824/papers/craq.pdf). MIT Licensed.

CRAQ is a replication protocol that allows reads from any replica while still
maintaining strong consistency. CRAQ _should_ provide better read throughput
than Raft and Paxos. Read performance grows linearly with the number of nodes
added to the system. Network chatter is significantly lower compared to Raft and
Paxos.

### Learn more about CRAQ
[CRAQ Paper](https://pdos.csail.mit.edu/6.824/papers/craq.pdf)

[Chain Replication: How to Build an Effective KV Storage](https://medium.com/coinmonks/chain-replication-how-to-build-an-effective-kv-storage-part-1-2-b0ce10d5afc3)

[MIT 6.824 Distributed Systems Lecture on CRAQ (80mins)](http://nil.csail.mit.edu/6.824/2020/video/9.html)

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
There are 3 packages that should be started to run the chain. The default node
implementation in [cmd/node](cmd/node) uses the Go net/rpc package and
[bbolt](go.etcd.io/bbolt) for storage.

### Coordinator
Facilitates new writes to the chain; allows nodes to announce themselves to the
chain; manages the order of the nodes of the chain. One Coordinator should be
run for each chain. For better resiliency, you _could_ run a cluster of
Coordinators and use something like Raft or Paxos for leader election, but
that's outside the scope of this project.

#### Run Flags
```sh
-a # Local address to listen on. Default: :1234
```

### Node
Represents a single node in the chain. Responsible for storing writes, serving
reads, and forwarding messages along the chain. In practice, you would probably
have a single Node process running on a machine. Each Node should have it's own
storage unit.

#### Run Flags
```sh
-a # Local address to listen on. Default: :1235
-p # Public address reachable by coordinator and the other nodes. Default: :1235
-c # Coordinator address. Default: :1234
-f # Bolt DB database file. Default: craq.db
```

### Client
Basic CLI tool for interacting with the chain. Allows writes and reads. The one
included in this project uses the net/rpc package as the transport layer and
bbolt as the storage layer.

#### Run Flags
```sh
-c # Address of coordinator. Default: :1234
-n # Address of node to send reads to. Default: :1235
```

#### Usage
```sh
./client write hello "world" # Write a new entry for key 'hello'
./client read hello # read the latest committed version of key 'hello'
```

## Communication
_go-craq_ processes communicate via RPC. The project is designed to be used with
whatever RPC system shall be desired. The basic default client included in the
go-craq package uses the net/rpc package from Go's stdlib; an easy-to-work-with
package with a great API.

### Adding a New Transport Implementation
Pull requests for additional transport implementations are very welcome. Some
common ones that would be great to have are gRPC and HTTP. Start by reading
through [transport/transport.go](transport/transport.go). Use
[transport/netrpc](transport/netrpc) as an example.

## Storage
_go-craq_ is designed to make it easy to swap the persistance layer. CRAQ is
flexible and any storage unit that implements the `Storer` interface in
[store/store.go](store/store.go) can be used. Some implementations for common
storage projects can be found in the `store` package. [store/kv](store/kv)
package is a _very simple_ in-memory key/value store that is included as an
example to work off of when adding new storage implementations.

### Adding a New Storage Implementation
Pull requests for additional storage implementations are very welcome. Start by
reading through the comments in [store/store.go](store/store.go). Use the
store/kv package as an example. CRAQ should work well with volatile and
non-volatile storage but mixing should be avoided or else you may end up seeing
long startup times due to data propagation. Mixing persistent storage mechanisms
is an interesting idea I've been playing with myself. For example, one node
storing items in the cloud and another storing items locally.

[store/storetest](store/storetest) should be used for testing new storage
implementations. Run the test suite like this:
```go
func TestStorer(t *testing.T) {
	storetest.Run(t, func(name string, test storetest.Test) {
    // New() is your store's constructor function.
		test(t, New())
	})
}
```

## Reading the Code
There are several places to start that'll give you a great understanding of how
things work.

`connectToCoordinator` method in [node/node.go](node/node.go). This is the
method the Node must run during startup to connect to the Coordinator and
announce itself to the chain. The Coordinator responds with some metadata about
where in the chain the node is now located. The node uses this info to connect
to the predecessor in the chain.

`Update` method in [node/node.go](node/node.go). This is the method the
Coordinator uses to update the node's metadata. New data is sent to the node if
the node's predecessor or successor changes, and if the address of the tail node
changes.

`ClientWrite` method in [node/node.go](node/node.go). This is the method the
Coordinator uses to send writes to the head node. This is where the chain begins
the process of propagation.

## Q/A
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

### What happens when a new node joins the chain?
When the `node.Start` method is run, the Node will backfill it's list of latest
versions for all committed items in it's store, then it'll connect to the
coordinator. Each Node stores the latest version of each committed item in it's
store in-memory. This is done so that if the node is or becomes the tail node,
other nodes can query it for the latest committed version of an item. The
resources at the top of the readme provide some info on why this is important,
but basically it helps ensure strong consistency.

After backfilling the map of latest versions the Node connects to the
Coordinator. The Coordinator adds the new Node to the list of Nodes in the
chain, connects to the Node, responds with some metadata for the new Node, then
sends updated metadata to the rest of the nodes in the chain to let it know
there's a new tail.

The metadata that the Coordinator sends back to the new Node includes info to
let the Node know if it's the head, the tail, and who it's predecessor in the
chain is. The Node uses this info to connect to the predecessor in the chain.

After connecting to the predecessor, the Node asks the predecessor for all items
it has that a) the Node has no record of, or b) the Node has older versions of.
This ensures that the new Node is caught up with the chain. Because the new node
is now the tail, any uncommitted items sent during propagation are immediately
committed.

Once the Node is connected to it's neighbor and the coordinator, it starts
listening for RPCs. The RPC server is setup and started in [cmd/node](cmd/node).

### Why store the latest committed versions in-memory?
It's worth mentioning that CRAQ works best with read-heavy workloads. One of
it's best "features" is being able to read from any node in the chain. If a node
receives a read request for key Y and the latest version of key Y in the store
is not committed, the node will send a request to the tail to ask for the latest
committed version of key Y; this helps ensure that all reads to any node returns
the same value. In other words, it asserts strong consistency. As the chain
grows, it takes longer for an item to be committed and the probability of
needing to ask the tail for the latest version rises. Asking the tail for the
latest version can quickly become a bottleneck. Therefore, storing these latest
versions in-memory affords us higher throughput from the tail for requests for
the latest version at the expense of slower startup times because the tail needs
to backfill it's map of latest versions.

In the future, it may be beneficial to let the operator of the node signify
whether they'd like to backfill at startup or serve 'latest version' requests
directly from the store.

## Backlog
- [ ] Benchmarks based off the tests in the paper, as close as reasonably possible.
- [ ] gRPC transporter
- [ ] HTTP transporter
- [ ] Allow nodes to join at any location in the chain.

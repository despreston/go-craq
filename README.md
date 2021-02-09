# go-craq [![Test Status](https://github.com/despreston/go-craq/workflows/Test/badge.svg)](https://github.com/despreston/go-craq/actions)

Package `go-craq` implements CRAQ (Chain Replication with Apportioned Queries) as described in
[the CRAQ paper](https://pdos.csail.mit.edu/6.824/papers/craq.pdf). MIT Licensed.

CRAQ is a replication protocol that allows reads from any replica while still maintaining strong consistency. CRAQ _should_ provide better read throughput than Raft and Paxos. Read performance grows linearly with the number of nodes added to the system. Network chatter is significantly lower compared to Raft and Paxos. 

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


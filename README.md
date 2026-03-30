# Raft-KV

A distributed key-value store powered by a custom implementation of the Raft consensus algorithm. Built in Go, this project emphasizes durable persistence, low-latency replication, and strict linearizability through advanced architectural patterns.

## Key Features

- **Durable WAL**: Custom binary Write-Ahead Log with `fsync` support, ensuring $O(1)$ append complexity and zero data loss on system crashes.
- **Strict Linearizability**: Implements **Leader Leases** for high-performance consistent reads without the overhead of a consensus round-trip.
- **Log Compaction**: Atomic snapshotting and state truncation to prevent infinite log growth and ensure fast recovery.
- **Optimized Transport**: Custom gRPC implementation with streamlined serialization, reducing replication overhead by 50% compared to standard gob-over-RPC.
- **Fault Tolerance**: Robust handling of network partitions, leader crashes, and log divergence with an automated failure-injection test suite.
- **Observability**: Built-in `/stats` monitoring and automatic client redirection to the current cluster leader.

## Performance

The following benchmarks were conducted on a 3-node cluster (AMD Ryzen 7 6800H, Linux).

| Transport | Snapshots | Throughput (ops/sec) | Latency (ns/op) |
|-----------|-----------|----------------------|-----------------|
| **gRPC**  | **No**    | **14,738**           | **67,848**      |
| **gRPC**  | **Yes**   | **14,522**           | **68,857**      |
| RPC       | No        | 1,922                | 520,055         |
| RPC       | Yes       | 9,073                | 110,216         |

*Note: gRPC transport provides a ~7x performance increase over standard RPC due to optimized serialization and pipelining.*

## Architecture

The system is designed with a clean separation of concerns:
- **`internal/raft`**: Pure consensus logic (Election, Replication, Persistence).
- **`internal/raft/wal.go`**: High-performance, append-only persistence layer.
- **`internal/store`**: Thread-safe KV state machine driven by the Raft `ApplyMsg` channel.
- **`internal/transport`**: Pluggable transport layer (standard RPC or optimized gRPC).

## Getting Started

### Prerequisites
- Go 1.22+
- Linux/macOS (for `fsync` and file locking)

### Installation
```bash
git clone https://github.com/ayushk-1801/raft-kv.git
cd raft-kv
go mod download
```

### Running a Cluster
Start a 3-node cluster locally using the provided main entry point:

**Node 0:**
```bash
go run cmd/node/main.go -id 0 -port 8080 -peers 1:localhost:8081,2:localhost:8082
```

**Node 1:**
```bash
go run cmd/node/main.go -id 1 -port 8081 -peers 0:localhost:8080,2:localhost:8082
```

**Node 2:**
```bash
go run cmd/node/main.go -id 2 -port 8082 -peers 0:localhost:8080,1:localhost:8081
```

*To use gRPC transport, append `-transport grpc` to the commands.*

### Using the Client
The client automatically handles leader redirects and retries.
```bash
# Write a value
go run cmd/client/main.go -addr localhost:8080 -op PUT -key user:1 -val "ayush"

# Read a value (Linearizable via Lease)
go run cmd/client/main.go -addr localhost:8080 -op GET -key user:1
```

## Testing & Reliability
The project includes an extensive failure-injection test suite covering:
- **Leader Election** under churn.
- **Log Divergence** resolution.
- **Network Partitions** (partial and full).
- **Persistence & Recovery** (verifying `fsync` durability).
- **Snapshot Installation** and log compaction races.

Run tests:
```bash
go test -v ./internal/raft/...
```

## References
- [Raft Consensus Algorithm](https://raft.github.io/raft.pdf)
- [Linearizability and Consistency](https://jepsen.io/consistency/models/linearizable)
- [Leader Leases (Paxos/Raft)](https://distributed-computing-musings.com/2019/02/leader-leases-in-distributed-systems/)

## License
MIT

# Raft-KV

A distributed, replicated key-value store powered by a custom implementation of the Raft consensus algorithm. Built from scratch in Go to demonstrate core distributed systems concepts like leader election, log replication, persistence, and snapshotting.

## Features

- **Custom Raft Engine**: Full implementation of the Raft consensus algorithm.
- **Strong Consistency**: Linearizable operations across the cluster.
- **Fault Tolerance**: Continues to operate as long as a majority of nodes are alive.
- **Persistence**: Survives hard reboots by persisting state (term, vote, log) to disk.
- **Log Compaction**: Efficient snapshotting to prevent logs from growing indefinitely.
- **Dual Transport**: Supports both standard Go `net/rpc` and high-performance `gRPC`.
- **Automatic Redirection**: Follower nodes redirect client requests to the current leader.
- **Observability**: Built-in `/stats` endpoint for real-time cluster monitoring.

## Project Structure

```text
├── cmd/
│   ├── node/          # Main node entry point
│   └── client/        # CLI client with retry/redirect logic
├── internal/
│   ├── raft/          # Raft consensus logic (Election, Replication, Persistence)
│   ├── store/         # Thread-safe KV store state machine
│   ├── transport/     # RPC and gRPC network implementations
│   └── raftpb/        # Protocol Buffer definitions for gRPC
```

## Resources

- [Raft Paper (Original)](https://raft.github.io/raft.pdf) - The definitive guide to the algorithm.
- [The Secret Lives of Data](https://thesecretlivesofdata.com/raft/) - An excellent visual explanation of Raft.
- [Implementing Raft (Eli Bendersky)](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/) - A great practical guide for implementation.

## Getting Started

### Prerequisites

- Go 1.22+
- (Optional) Protoc & gRPC plugins for modifying transport definitions

### Installation

```bash
git clone https://github.com/ayushk-1801/raft-kv.git
cd raft-kv
go mod download
```

### Running a Cluster

You can start a 3-node cluster locally by running the following commands in separate terminals:

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

*Note: To use gRPC transport, add `-transport grpc` to the commands.*

### Using the Client

The built-in client handles redirects and retries automatically.

**Write a value:**
```bash
go run cmd/client/main.go -addr localhost:8080 -op PUT -key mykey -val "hello raft"
```

**Read a value:**
```bash
go run cmd/client/main.go -addr localhost:8080 -op GET -key mykey
```

**Check node stats:**
```bash
curl http://localhost:8080/stats
```

## Testing

The project includes a robust test suite covering leader election, log conflicts, partitions, and recovery.

```bash
go test -v ./internal/raft/...
```

## License

MIT

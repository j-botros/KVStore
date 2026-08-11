# KVStore

KVStore is a distributed, high-performance Key-Value store written in Go. It is built on top of a custom **Log-Structured Merge (LSM) Tree** storage engine designed for high write throughput and efficient reads. 

## Features

- **LSM-Tree Storage Engine:**
  - **Memtable:** Fast in-memory writes and reads using a probabilistic skip list.
  - **Write-Ahead Log (WAL):** Ensures durability; data is appended to the WAL before being applied to the memtable to prevent data loss on crashes.
  - **SSTables (Sorted String Tables):** Immutable on-disk files. Includes CRC32 checksums for data integrity and Bloom Filters for fast point-lookup queries.
  - **Background Operations:** Concurrent memtable flushing and automatic leveled compaction.
- **RESTful API:** Exposes endpoints to `GET`, `POST`, and `DELETE` key-value pairs.
- **Observability:** Built-in Prometheus metrics (`/metrics`).
- **Distributed Ready:** Configuration schemas support defining clusters, sharding ranges, and leader-follower replication logic via gRPC.

## Architecture

The system is divided into two primary layers:

1. **Storage Engine (`pkg/kvstore/storageEngine`)**: The core of the database. It handles the lifecycle of data from the WAL to the Memtable, and eventually flushes data to leveled SSTables. It handles all compaction and background I/O operations safely using atomic variables and read-write mutexes.
2. **Server (`cmd/kvstore`)**: The HTTP/gRPC server that exposes the storage engine over a network interface and handles application-level configuration and observability.

## Configuration

The application is highly configurable. Example configurations include:

- **Storage Tuning:** `memtable_size_mb`, `wal_segment_size_mb`, `level_size_multiplier`, etc.
- **Cluster & Sharding:** Define gRPC node addresses, shard key ranges (e.g., `A` to `H`), and leader/follower assignments.

## Getting Started

### Building

```bash
# Build the KVStore binary
go build -o kvstore ./cmd/kvstore
```

### Running

Ensure you have a valid `config.json` in your working directory, then start the server:

```bash
./kvstore
```

### Usage Examples

```bash
# Set a value
curl -X POST -d "my-value" http://localhost:8080/kv/my-key

# Get a value
curl http://localhost:8080/kv/my-key

# Delete a value
curl -X DELETE http://localhost:8080/kv/my-key
```

## Testing

The project includes an extensive test suite verifying concurrent access, correct merging of SSTables, tombstone pruning, memtable rotation, and atomic compaction.

```bash
go test ./pkg/kvstore/storageEngine/... -v -count=1
```
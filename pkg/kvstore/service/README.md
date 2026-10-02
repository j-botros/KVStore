# `service` package

The `service` package contains the **core business logic** of the KVStore node. It sits in the middle of the dependency stack — above the storage engine, below the transport layer (HTTP/gRPC).

```
cmd/kvstore/main.go
       │
       ├── interface/http     (HTTP controllers)  ──┐
       ├── interface/grpc     (gRPC server)       ──┤──▶  service  ──▶  storageEngine
       └── kvstore.go         (lifecycle)         ──┘
```

The service layer **never imports** `interface/grpc` or `interface/http`. Transport concerns stay outside; the service only knows about keys, values, and errors.

---

## Files

| File | Purpose |
|---|---|
| `service.go` | `Service` struct, constructor, `Get` / `Put` / `Delete`, `NodeClient` interface |
| `service_errors.go` | Sentinel errors (`ErrNotFound`, `ErrReadOnly`) |

---

## Key Concepts

### Sharding and key ownership

Each node is responsible for a **contiguous key range** defined by `minKey` and `maxKey` in `config.yaml`. `ownsKey(key)` determines whether a key belongs to this node:

```
minKey == ""  →  no lower bound (owns everything before maxKey)
maxKey == ""  →  no upper bound (owns everything from minKey onwards)
```

The **upper bound is exclusive**: a key equal to `maxKey` belongs to the *next* shard. This ensures adjacent shard boundaries never leave gaps (e.g. `"Hello"` is correctly captured by a shard ending at `"I"`).

### Leader vs. follower

Each shard has one **leader** and zero or more **followers**:

- **Leader** — accepts reads and writes for its key range.
- **Follower** — accepts reads only. Writes return `ErrReadOnly`. Followers receive writes via `ReplicateEntry` from the leader (see TODO below).

`isLeader` is resolved at startup from `config.yaml` and injected into the service via `NewService`.

### `NodeClient` interface

`Service` needs to communicate with other nodes (for request forwarding) without importing the `interface/grpc` package — that would create an import cycle since `interface/grpc` already imports `service`.

The solution is **dependency inversion**: `service` defines the `NodeClient` interface describing what it needs; the concrete `NodeClientAdapter` in `interface/grpc` satisfies it. `main.go` wires them together.

```
interface/grpc (NodeClientAdapter) ──implements──▶ service.NodeClient
```

---

## How Requests Flow Through the Service Layer

### Read (`Get`)

```
caller
  │
  ▼
service.Get(key)
  │
  ├── ownsKey? ──NO──▶ nodeClient.ForwardGet(key)  ──▶  peer node
  │
  └── YES
        │
        ▼
      storageEngine.Get(key)
        │
        ├── ErrKeyNotFound ──▶ ErrNotFound (normalised)
        └── value           ──▶ return to caller
```

### Write (`Put` / `Delete`)

```
caller
  │
  ▼
service.Put(key, value)
  │
  ├── ownsKey? ──NO──▶ nodeClient.ForwardPut(key, value)  ──▶  owning leader
  │
  └── YES
        │
        ├── isLeader? ──NO──▶ ErrReadOnly
        │
        └── YES
              │
              ▼
            storageEngine.Put(key, value)
              │
              └── success ──▶ TODO: replicate to followers (see below)
```

---

## Errors

| Sentinel | Meaning | Returned by |
|---|---|---|
| `ErrNotFound` | Key does not exist in the store | `Get`, `Delete` |
| `ErrReadOnly` | This node is a follower; writes are rejected | `Put`, `Delete` |

Callers in `interface/http` and `interface/grpc` use `errors.Is(err, service.ErrXxx)` to map these to appropriate HTTP status codes or gRPC status codes.

---

## TODO

- **Replication (`ReplicateEntry`)**: After a leader applies a write, it should fan out the change to all followers as a fire-and-forget background gRPC call using `NodeClient.ReplicateEntry`. Followers need a dedicated apply path that writes directly to the storage engine, bypassing the `isLeader` guard.

- **Quorum writes**: Instead of fire-and-forget, require acknowledgement from N/2+1 followers before returning success to the client (stronger consistency guarantee).

- **Sequence numbers**: Assign a monotonically increasing `seqNum` to each write. Followers can use this to detect and reject out-of-order or duplicate entries.

- **Leader election / Raft**: The current leader/follower assignment is static (from config). A dynamic consensus protocol (e.g. Raft) would handle leader failure and re-election automatically.

- **Heartbeat**: Leaders should periodically send heartbeat pings to followers (via the existing `Heartbeat` RPC) to confirm liveness and allow followers to detect a failed leader.

- **Multi-shard routing**: The current `nodeClient` is a single peer. A proper router would map arbitrary keys to the correct shard leader across all shards.

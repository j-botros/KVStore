# `interface/grpc` package

gRPC transport between KVStore nodes. `GRPCServer` serves the `KVStore` service defined in `kvstore.proto` (request forwarding, replication, sequence numbers, and snapshot `Replay`). `NodeClientAdapter` is the client side and implements `coordinator.NodeClient`.

`kvstore.pb.go` and `kvstore_grpc.pb.go` are generated from `kvstore.proto`; regenerate them with `protoc` rather than editing them by hand.

---

## Testing

Run from the repository root:

```bash
# All gRPC tests
go test ./pkg/kvstore/interface/grpc -v -count=1

# With the race detector
go test ./pkg/kvstore/interface/grpc -race -count=1

# A single test
go test ./pkg/kvstore/interface/grpc -run TestName -v -count=1
```

Tests run the server in-process over `bufconn` (an in-memory listener), so no network ports are used.

# `cmd/kvstore`

The server entry point. `main.go` reads `config.yaml` from the working directory, opens the storage engine, connects to peer nodes, runs startup recovery, and starts the HTTP and gRPC servers.

`topology.go` holds the logic that can be tested without a network:
- `loadConfig`: parses and validates `config.yaml`.
- `resolveTopology`: determines this node's key range, leader/follower role, and the remote shards it routes to.

---

## Testing

Run from the repository root:

```bash
# Config and topology tests
go test ./cmd/kvstore -v -count=1

# A single test
go test ./cmd/kvstore -run TestName -v -count=1
```

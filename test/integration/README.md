# Integration tests

End-to-end tests against the real `kvstore` binary over HTTP and gRPC. Every file has the `//go:build integration` tag, so `go test ./...` skips them.

## Harness

- `main_test.go`: `TestMain` builds the binary once into a temp dir (`go build ./cmd/kvstore`), runs the tests, then kills any child process still tracked and fails the run if a `kvstore` process leaked.
- `harness_test.go`:
  - **Nodes.** Each node is its own OS process with its own `t.TempDir()` as the working directory, because the engine writes `data/wal/` and `data/sstables/` relative to the cwd and reads `./config.yaml`. The harness generates each node's `config.yaml` with free `127.0.0.1` ports. Lifecycle: `start`, `waitReady` (polls `GET /`, and fails early if the process exits), `term` (SIGTERM, then SIGKILL after 5s), `kill`, `pause`/`resume` (SIGSTOP/SIGCONT), `wipe`. Each node's stdout/stderr goes to `node.log` in its dir, and all node logs are printed when a test fails.
  - **Clients.** `put` (`POST /kv` with JSON `{"key","value"}`), `get`, `del`, `hasValue`, `isAbsent`, `metric` (scrapes `/metrics`), and `seqNum` (calls the `GetSeqNum` RPC directly).
  - **Sync.** `eventually(t, timeout, fn)` polls instead of sleeping. `cluster.settle` waits until every live node can write every shard and every leader's writes reach every live node, because gRPC channels to a restarted peer stay in reconnect backoff for a while.
  - **Boot gate.** `requireBoot` runs a single-node boot check once (I0). If the binary can't boot, every scenario fails fast with that message instead of timing out.
- **Bug tags.** A failure caused by a suspected bug starts with `BUG B-n (<one line>): ` (constants at the top of `harness_test.go`; the plan is `tmp-designs/design_test-coverage.md`). Bugs found by these tests are numbered `B-I13` onward. Known bugs are never skipped.

## Topologies

Every node belongs to every shard.

| Name | Nodes | Shards |
|---|---|---|
| one-node | `n0` | one shard `["", "")`, no peers |
| T-single | `n0`, `n1`, `n2` | one shard `["", "")`, leader `n0` |
| T-multi | `kvstore-0..2` | the repo's `config.yaml` layout: `["","I")`, `["I","Q")`, `["Q","")`; `kvstore-N` leads shard N+1 |

## Scenarios

| File | Covers |
|---|---|
| `i0_i1_boot_harness_test.go` | I0 boot check, generated config completeness, node lifecycle |
| `i2_single_node_test.go` | CRUD, delete of a missing key, `GET /`, persistence across SIGTERM/SIGKILL, first-write durability, flush and compaction across restarts |
| `i3_routing_test.go` | every boundary key through every node on T-multi; writes land on the configured leader |
| `i4_replication_test.go` | propagation, follower proxying, a follower down, rapid overwrites, forwarded delete of a missing key |
| `i5_recovery_test.go` | wiped follower (1201 keys, three `Replay` chunks), follower behind across deletes, seq nums, wiped leader, most up-to-date donor |
| `i6_failures_test.go` | shard leader down, leader down, frozen leader vs. timeouts, bad config exits non-zero (not gated on boot) |
| `i7_concurrency_test.go` | concurrent clients, `/metrics`, `kv_keys_total`, opt-in soak |

## Running

Run from the repository root:

```bash
# Full suite
go test -tags integration ./test/integration/... -count=1 -v

# A single test
go test -tags integration ./test/integration -run TestI4_FollowerDown -v -count=1

# Repeat timing-sensitive tests
go test -tags integration ./test/integration -run 'TestI4|TestI5|TestI6' -count=3

# Soak (opt-in; skipped unless -run matches "Soak"). Default 2m; followers are killed and restarted throughout.
go test -tags integration ./test/integration -run Soak -v -count=1
KVSTORE_SOAK_DURATION=10m go test -tags integration ./test/integration -run Soak -v -count=1 -timeout 20m

# Build the binary under test with the race detector
KVSTORE_IT_RACE=1 go test -tags integration ./test/integration/... -count=1 -v
```

Data races in the binary appear as `WARNING: DATA RACE` in the failing node's log. SIGTERM is not handled by the binary (no graceful shutdown), so `term()` and `kill()` differ only in the signal.

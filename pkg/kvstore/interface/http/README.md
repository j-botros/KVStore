# `interface/http` package

HTTP transport for KVStore. `StoreController` exposes the key-value API and maps coordinator errors to HTTP status codes; `MonitoringController` serves Prometheus metrics.

| Route | Description |
|---|---|
| `POST /kv` | Set a value. JSON body: `{"key": "...", "value": "..."}` |
| `GET /kv/{key}` | Get a value |
| `DELETE /kv/{key}` | Delete a key |
| `GET /metrics` | Prometheus metrics |

---

## Testing

Run from the repository root:

```bash
# All HTTP tests
go test ./pkg/kvstore/interface/http -v -count=1

# With the race detector
go test ./pkg/kvstore/interface/http -race -count=1

# A single test
go test ./pkg/kvstore/interface/http -run TestName -v -count=1
```

`MonitoringController` registers its metrics with the global Prometheus registry, so it can only be constructed once per test binary; tests create it once in `TestMain`.

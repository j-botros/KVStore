package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	coordinator "kvstore/pkg/kvstore/coordinator"
	storageengine "kvstore/pkg/kvstore/storageEngine"
)

/* ====================================================================================
	TestMain: one MonitoringController per test binary
==================================================================================== */

// MonitoringController registers metrics with the global Prometheus registry,
// so it is built exactly once. Its engine lives in metricsDir; the engine uses
// relative paths, so metrics tests chdir there (useMetricsEngine).
var (
	metricsDir    string
	metricsEngine *storageengine.StorageEngine
	metricsCtrl   *MonitoringController
)

func TestMain(m *testing.M) {
	orig, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	metricsDir, err = os.MkdirTemp("", "kvstore-http-metrics-")
	if err != nil {
		log.Fatal(err)
	}
	if err := os.Chdir(metricsDir); err != nil {
		log.Fatal(err)
	}
	metricsEngine, err = storageengine.OpenStorageEngine(64<<20, 64<<20, 4096, 10)
	if err != nil {
		log.Fatal(err)
	}
	metricsCtrl = NewMonitoringController(metricsEngine)
	if err := os.Chdir(orig); err != nil {
		log.Fatal(err)
	}

	code := m.Run()
	os.RemoveAll(metricsDir)
	os.Exit(code)
}

/* ====================================================================================
	HELPERS
==================================================================================== */

func chdir(t *testing.T, dir string) {
	t.Helper()
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(orig) })
}

// fakeNode is a coordinator.NodeClient with function-field overrides.
type fakeNode struct {
	get    func(ctx context.Context, key string) ([]byte, bool, error)
	put    func(ctx context.Context, key string, value []byte) error
	delete func(ctx context.Context, key string) error
}

func (f *fakeNode) ForwardGet(ctx context.Context, key string) ([]byte, bool, error) {
	if f.get != nil {
		return f.get(ctx, key)
	}
	return nil, false, nil
}
func (f *fakeNode) ForwardPut(ctx context.Context, key string, value []byte) error {
	if f.put != nil {
		return f.put(ctx, key, value)
	}
	return nil
}
func (f *fakeNode) ForwardDelete(ctx context.Context, key string) error {
	if f.delete != nil {
		return f.delete(ctx, key)
	}
	return nil
}
func (f *fakeNode) ReplicateEntry(context.Context, coordinator.LogEntry) error { return nil }
func (f *fakeNode) GetSeqNum(context.Context) (uint64, error)                  { return 0, nil }
func (f *fakeNode) ReplaySnapshot(context.Context, uint64, func(uint64, []coordinator.LogEntry, bool) error) error {
	return nil
}

// blockUntilDone blocks until ctx is done and returns its error. The 5s
// fallback only prevents a hung test if the deadline is never applied.
func blockUntilDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return errors.New("context was never cancelled")
	}
}

var defaultTimeouts = StoreTimeouts{Get: 2 * time.Second, Put: 2 * time.Second, Delete: 2 * time.Second}

type srvOpts struct {
	minKey, maxKey string
	isLeader       bool
	leaderClient   coordinator.NodeClient
	remote         []coordinator.ShardNodeConfig
	timeouts       *StoreTimeouts
}

// newServer builds a StoreController over a real engine in a fresh temp dir.
func newServer(t *testing.T, o srvOpts) http.Handler {
	t.Helper()
	chdir(t, t.TempDir())
	engine, err := storageengine.OpenStorageEngine(64<<20, 64<<20, 4096, 10)
	if err != nil {
		t.Fatal(err)
	}
	coord := coordinator.NewCoordinator(engine, coordinator.NewShardRouter(o.remote), o.leaderClient, o.minKey, o.maxKey, o.isLeader, nil)
	to := defaultTimeouts
	if o.timeouts != nil {
		to = *o.timeouts
	}
	mux := http.NewServeMux()
	NewStoreController(coord, to).RegisterRoutes(mux)
	return mux
}

func do(t *testing.T, h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func putJSON(key, value string) string {
	b, _ := json.Marshal(map[string]string{"key": key, "value": value})
	return string(b)
}

func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, want int, what string) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("%s: status %d, want %d (body %q)", what, rec.Code, want, rec.Body.String())
	}
}

/* ====================================================================================
	CRUD + status codes
==================================================================================== */

func TestHTTP_PutGetDelete_RoundTrip(t *testing.T) {
	h := newServer(t, srvOpts{isLeader: true})

	wantStatus(t, do(t, h, "POST", "/kv", putJSON("name", "alice")), 200, "POST")

	rec := do(t, h, "GET", "/kv/name", "")
	wantStatus(t, rec, 200, "GET")
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var got map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("GET body not JSON: %v (%q)", err, rec.Body.String())
	}
	if got["key"] != "name" || got["value"] != "alice" {
		t.Fatalf("GET body = %v, want {key:name value:alice}", got)
	}

	wantStatus(t, do(t, h, "POST", "/kv", putJSON("name", "bob")), 200, "POST overwrite")
	json.Unmarshal(do(t, h, "GET", "/kv/name", "").Body.Bytes(), &got)
	if got["value"] != "bob" {
		t.Fatalf("after overwrite value = %q, want bob", got["value"])
	}

	wantStatus(t, do(t, h, "DELETE", "/kv/name", ""), 200, "DELETE")
	wantStatus(t, do(t, h, "GET", "/kv/name", ""), 404, "GET after DELETE")
}

func TestHTTP_ValueRoundTrip_Odd(t *testing.T) {
	h := newServer(t, srvOpts{isLeader: true})
	cases := map[string]string{
		"empty":   "",
		"unicode": "héllo 世界 🚀",
		"json":    `{"nested":"x"}`,
		"spaces":  "  a b  ",
		"large":   strings.Repeat("v", 64*1024),
	}
	for name, val := range cases {
		t.Run(name, func(t *testing.T) {
			key := "odd-" + name
			wantStatus(t, do(t, h, "POST", "/kv", putJSON(key, val)), 200, "POST")
			rec := do(t, h, "GET", "/kv/"+key, "")
			wantStatus(t, rec, 200, "GET")
			var got map[string]string
			json.Unmarshal(rec.Body.Bytes(), &got)
			if got["value"] != val {
				t.Fatalf("value = %q (len %d), want %q (len %d)", trunc(got["value"]), len(got["value"]), trunc(val), len(val))
			}
		})
	}
}

func trunc(s string) string {
	if len(s) > 40 {
		return s[:40] + "..."
	}
	return s
}

// A key containing '/' can be written via the JSON body and read back with
// the slash percent-encoded in the path.
func TestHTTP_KeyWithSlash_EscapedInPath(t *testing.T) {
	h := newServer(t, srvOpts{isLeader: true})
	wantStatus(t, do(t, h, "POST", "/kv", putJSON("a/b", "v")), 200, "POST")
	rec := do(t, h, "GET", "/kv/a%2Fb", "")
	wantStatus(t, rec, 200, "GET /kv/a%2Fb")
	var got map[string]string
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got["key"] != "a/b" || got["value"] != "v" {
		t.Fatalf("body = %v, want key a/b value v", got)
	}
	wantStatus(t, do(t, h, "DELETE", "/kv/a%2Fb", ""), 200, "DELETE /kv/a%2Fb")
	wantStatus(t, do(t, h, "GET", "/kv/a%2Fb", ""), 404, "GET after DELETE")
}

func TestHTTP_Put_BadRequest(t *testing.T) {
	h := newServer(t, srvOpts{isLeader: true})
	cases := map[string]string{
		"malformed JSON":   `{"key": "a", "value": `,
		"not JSON":         `key=a&value=b`,
		"missing key":      `{"value": "v"}`,
		"empty key":        `{"key": "", "value": "v"}`,
		"value wrong type": `{"key": "a", "value": 5}`,
		"key wrong type":   `{"key": 5, "value": "v"}`,
		"JSON array":       `["a","b"]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			wantStatus(t, do(t, h, "POST", "/kv", body), 400, "POST "+body)
		})
	}
	t.Run("empty body", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/kv", http.NoBody)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		wantStatus(t, rec, 400, "POST with empty body")
	})
	// Nothing must have been written by any of the rejected requests.
	wantStatus(t, do(t, h, "GET", "/kv/a", ""), 404, "GET a after rejected POSTs")
}

func TestHTTP_Routes_MethodAndPath(t *testing.T) {
	h := newServer(t, srvOpts{isLeader: true})
	cases := []struct {
		method, path string
		want         int
	}{
		{"POST", "/kv/somekey", 405}, // old per-key POST route is gone
		{"PUT", "/kv", 405},          // only POST writes
		{"GET", "/kv", 405},          // GET needs a key
		{"GET", "/kv/a/b", 404},      // unescaped slash is not a key
		{"GET", "/other", 404},
	}
	for _, tc := range cases {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			wantStatus(t, do(t, h, tc.method, tc.path, ""), tc.want, tc.method+" "+tc.path)
		})
	}
}

func TestHTTP_Get_Missing_404(t *testing.T) {
	h := newServer(t, srvOpts{isLeader: true})
	wantStatus(t, do(t, h, "GET", "/kv/nope", ""), 404, "GET missing")
}

func TestHTTP_Delete_Missing_404(t *testing.T) {
	h := newServer(t, srvOpts{isLeader: true})
	rec := do(t, h, "DELETE", "/kv/never-written", "")
	if rec.Code != 404 {
		t.Fatalf("BUG B-7 (deleting a missing key succeeds): got status %d, want 404", rec.Code)
	}
}

/* ====================================================================================
	Follower / remote paths
==================================================================================== */

func TestHTTP_Follower_ProxiesWritesToLeader(t *testing.T) {
	var gotPut, gotDel string
	leader := &fakeNode{
		put:    func(_ context.Context, k string, v []byte) error { gotPut = k + "=" + string(v); return nil },
		delete: func(_ context.Context, k string) error { gotDel = k; return nil },
	}
	h := newServer(t, srvOpts{isLeader: false, leaderClient: leader})
	wantStatus(t, do(t, h, "POST", "/kv", putJSON("k", "v")), 200, "POST via follower")
	wantStatus(t, do(t, h, "DELETE", "/kv/k", ""), 200, "DELETE via follower")
	if gotPut != "k=v" || gotDel != "k" {
		t.Fatalf("leader saw put %q delete %q, want k=v and k", gotPut, gotDel)
	}
}

func TestHTTP_Follower_ProxiedDeleteNotFound_404(t *testing.T) {
	leader := &fakeNode{delete: func(context.Context, string) error { return coordinator.ErrNotFound }}
	h := newServer(t, srvOpts{isLeader: false, leaderClient: leader})
	wantStatus(t, do(t, h, "DELETE", "/kv/k", ""), 404, "DELETE via follower, leader says not found")
}

func TestHTTP_Follower_NoLeader_ReadOnlyIsNot500(t *testing.T) {
	h := newServer(t, srvOpts{isLeader: false})
	for _, r := range []struct{ method, path, body string }{
		{"POST", "/kv", putJSON("k", "v")},
		{"DELETE", "/kv/k", ""},
	} {
		rec := do(t, h, r.method, r.path, r.body)
		if rec.Code < 400 || rec.Code >= 500 {
			t.Errorf("BUG B-8 (read-only error is reported as a server error): %s %s got status %d, "+
				"want a 4xx such as 403 (ErrReadOnly is a client-side condition)", r.method, r.path, rec.Code)
		}
	}
}

func TestHTTP_Remote_GetFoundAndNotFound(t *testing.T) {
	remote := &fakeNode{get: func(_ context.Context, k string) ([]byte, bool, error) {
		if k == "zz-present" {
			return []byte("remote-v"), true, nil
		}
		return nil, false, nil
	}}
	h := newServer(t, srvOpts{isLeader: true, minKey: "", maxKey: "m",
		remote: []coordinator.ShardNodeConfig{{MinKey: "m", MaxKey: "", Leader: remote}}})

	rec := do(t, h, "GET", "/kv/zz-present", "")
	wantStatus(t, rec, 200, "remote GET present")
	var got map[string]string
	json.Unmarshal(rec.Body.Bytes(), &got)
	if got["value"] != "remote-v" {
		t.Fatalf("remote value = %q, want remote-v", got["value"])
	}
	wantStatus(t, do(t, h, "GET", "/kv/zz-absent", ""), 404, "remote GET absent")
}

func TestHTTP_Remote_ErrorIs500(t *testing.T) {
	boom := errors.New("boom")
	remote := &fakeNode{
		get:    func(context.Context, string) ([]byte, bool, error) { return nil, false, boom },
		put:    func(context.Context, string, []byte) error { return boom },
		delete: func(context.Context, string) error { return boom },
	}
	h := newServer(t, srvOpts{isLeader: true, minKey: "", maxKey: "m",
		remote: []coordinator.ShardNodeConfig{{MinKey: "m", MaxKey: "", Leader: remote}}})
	wantStatus(t, do(t, h, "GET", "/kv/z", ""), 500, "remote GET error")
	wantStatus(t, do(t, h, "POST", "/kv", putJSON("z", "v")), 500, "remote POST error")
	wantStatus(t, do(t, h, "DELETE", "/kv/z", ""), 500, "remote DELETE error")
}

/* ====================================================================================
	Timeouts
==================================================================================== */

// blockingRemote returns a server whose keys >= "m" go to a remote node that
// blocks until the request context ends. ctxErr receives what the node saw.
func blockingRemote(t *testing.T, to StoreTimeouts) (http.Handler, chan error) {
	t.Helper()
	ctxErr := make(chan error, 3)
	remote := &fakeNode{
		get: func(ctx context.Context, _ string) ([]byte, bool, error) {
			err := blockUntilDone(ctx)
			ctxErr <- err
			return nil, false, err
		},
		put: func(ctx context.Context, _ string, _ []byte) error {
			err := blockUntilDone(ctx)
			ctxErr <- err
			return err
		},
		delete: func(ctx context.Context, _ string) error {
			err := blockUntilDone(ctx)
			ctxErr <- err
			return err
		},
	}
	h := newServer(t, srvOpts{isLeader: true, minKey: "", maxKey: "m", timeouts: &to,
		remote: []coordinator.ShardNodeConfig{{MinKey: "m", MaxKey: "", Leader: remote}}})
	return h, ctxErr
}

var timeoutReqs = []struct{ method, path, body string }{
	{"GET", "/kv/z", ""},
	{"POST", "/kv", `{"key":"z","value":"v"}`},
	{"DELETE", "/kv/z", ""},
}

// The configured per-operation timeout must reach the remote call as a
// context deadline.
func TestHTTP_Timeouts_DeadlineReachesRemote(t *testing.T) {
	h, ctxErr := blockingRemote(t, StoreTimeouts{Get: 30 * time.Millisecond, Put: 30 * time.Millisecond, Delete: 30 * time.Millisecond})
	for _, r := range timeoutReqs {
		t.Run(r.method, func(t *testing.T) {
			do(t, h, r.method, r.path, r.body)
			if err := <-ctxErr; !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("remote saw ctx err %v, want context.DeadlineExceeded", err)
			}
		})
	}
}

func TestHTTP_Timeouts_StatusIs504(t *testing.T) {
	h, ctxErr := blockingRemote(t, StoreTimeouts{Get: 30 * time.Millisecond, Put: 30 * time.Millisecond, Delete: 30 * time.Millisecond})
	for _, r := range timeoutReqs {
		t.Run(r.method, func(t *testing.T) {
			rec := do(t, h, r.method, r.path, r.body)
			<-ctxErr
			if rec.Code != http.StatusGatewayTimeout {
				t.Fatalf("BUG B-8 (timeout errors are reported as 500): %s %s got status %d, want 504",
					r.method, r.path, rec.Code)
			}
		})
	}
}

/* ====================================================================================
	/metrics
==================================================================================== */

// useMetricsEngine chdirs into the TestMain engine's directory and returns a
// mux serving the KV routes over that engine plus /metrics.
func useMetricsEngine(t *testing.T) http.Handler {
	t.Helper()
	chdir(t, metricsDir)
	coord := coordinator.NewCoordinator(metricsEngine, coordinator.NewShardRouter(nil), nil, "", "", true, nil)
	mux := http.NewServeMux()
	NewStoreController(coord, defaultTimeouts).RegisterRoutes(mux)
	metricsCtrl.RegisterRoutes(mux)
	return mux
}

// scrape returns the un-labelled samples from /metrics.
func scrape(t *testing.T, h http.Handler) map[string]float64 {
	t.Helper()
	rec := do(t, h, "GET", "/metrics", "")
	wantStatus(t, rec, 200, "GET /metrics")
	out := map[string]float64{}
	sc := bufio.NewScanner(rec.Body)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") || strings.Contains(line, "{") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		v, err := strconv.ParseFloat(f[1], 64)
		if err != nil {
			continue
		}
		out[f[0]] = v
	}
	return out
}

func metric(t *testing.T, m map[string]float64, name string) float64 {
	t.Helper()
	v, ok := m[name]
	if !ok {
		t.Fatalf("metric %s missing from /metrics", name)
	}
	return v
}

func TestMetrics_ExposesAllSeries(t *testing.T) {
	h := useMetricsEngine(t)
	m := scrape(t, h)
	for _, name := range []string{
		"kv_get_requests_total", "kv_put_requests_total", "kv_delete_requests_total",
		"kv_get_latency_ms_count", "kv_put_latency_ms_count", "kv_delete_latency_ms_count",
		"kv_keys_total", "kv_wal_size_bytes", "kv_sstable_disk_usage_bytes", "kv_memtable_size_bytes",
		"kv_compaction_runs_total", "kv_flush_runs_total",
	} {
		metric(t, m, name)
	}
}

func TestMetrics_RequestCountersAndGauges(t *testing.T) {
	h := useMetricsEngine(t)
	before := scrape(t, h)

	wantStatus(t, do(t, h, "POST", "/kv", putJSON("m-counters", "v")), 200, "POST")
	wantStatus(t, do(t, h, "GET", "/kv/m-counters", ""), 200, "GET")
	wantStatus(t, do(t, h, "GET", "/kv/m-counters-missing", ""), 404, "GET missing")
	do(t, h, "POST", "/kv", `{bad`) // rejected requests are still counted

	after := scrape(t, h)
	for name, want := range map[string]float64{
		"kv_get_requests_total":   2,
		"kv_put_requests_total":   2,
		"kv_get_latency_ms_count": 2,
		"kv_put_latency_ms_count": 2,
		"kv_keys_total":           1,
	} {
		if d := metric(t, after, name) - metric(t, before, name); d != want {
			t.Errorf("%s delta = %v, want %v", name, d, want)
		}
	}
	if v := metric(t, after, "kv_wal_size_bytes"); v <= metric(t, before, "kv_wal_size_bytes") {
		t.Errorf("kv_wal_size_bytes did not grow after a write: %v", v)
	}
	if v := metric(t, after, "kv_memtable_size_bytes"); v <= metric(t, before, "kv_memtable_size_bytes") {
		t.Errorf("kv_memtable_size_bytes did not grow after a write: %v", v)
	}
}

func TestMetrics_DeleteCounter(t *testing.T) {
	h := useMetricsEngine(t)
	wantStatus(t, do(t, h, "POST", "/kv", putJSON("m-del", "v")), 200, "POST")
	before := scrape(t, h)
	wantStatus(t, do(t, h, "DELETE", "/kv/m-del", ""), 200, "DELETE")
	after := scrape(t, h)
	for name, want := range map[string]float64{
		"kv_delete_requests_total":   1,
		"kv_delete_latency_ms_count": 1,
		"kv_keys_total":              -1,
	} {
		if d := metric(t, after, name) - metric(t, before, name); d != want {
			t.Errorf("%s delta = %v, want %v", name, d, want)
		}
	}
}

func TestMetrics_KeysTotal_OverwriteCountsOnce(t *testing.T) {
	h := useMetricsEngine(t)
	before := scrape(t, h)
	for i := 0; i < 3; i++ {
		wantStatus(t, do(t, h, "POST", "/kv", putJSON("m-overwrite", fmt.Sprint(i))), 200, "POST")
	}
	after := scrape(t, h)
	if d := metric(t, after, "kv_keys_total") - metric(t, before, "kv_keys_total"); d != 1 {
		t.Fatalf("BUG B-7 (KeysTotal counts every Put, not live keys): kv_keys_total delta = %v after "+
			"writing one key 3 times, want 1", d)
	}
}

func TestMetrics_KeysTotal_DeleteMissingUnchanged(t *testing.T) {
	h := useMetricsEngine(t)
	before := scrape(t, h)
	do(t, h, "DELETE", "/kv/m-never-written", "")
	after := scrape(t, h)
	if d := metric(t, after, "kv_keys_total") - metric(t, before, "kv_keys_total"); d != 0 {
		t.Fatalf("BUG B-7 (deleting a missing key decrements KeysTotal): kv_keys_total delta = %v, want 0", d)
	}
}

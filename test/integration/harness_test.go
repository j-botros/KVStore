//go:build integration

package integration

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	kvgrpc "kvstore/pkg/kvstore/interface/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/* ====================================================================================
	SUSPECTED BUGS (see tmp-designs/design_test-coverage.md). Failures caused by one
	of these start with "BUG B-n (<description>): ".
==================================================================================== */

const (
	bugB1  = "BUG B-1 (config struct fields are unexported, so yaml.v3 loads nothing and the binary refuses to start)"
	bugB2  = "BUG B-2 (after a restart, compaction can move the newest L0 SST below older data, giving stale reads)"
	bugB3  = "BUG B-3 (async replication can apply two writes to one key out of order on a follower)"
	bugB4  = "BUG B-4 (follower seq nums never advance and are not persisted, so an empty leader never recovers)"
	bugB5  = "BUG B-5 (recovery does not delete keys the donor has deleted)"
	bugB6  = "BUG B-6 (each node gets one key range though it belongs to every shard; kvstore-2 gets the wrong role and the router is empty, so non-local keys fail)"
	bugB7  = "BUG B-7 (deleting a missing key returns 200 and the key count goes negative)"
	bugB8  = "BUG B-8 (unavailable-peer and timeout errors become HTTP 500)"
	// New bugs found by integration tests use B-I13 onward (the unit agent numbers its own from B-13).
	bugBI13 = "BUG B-I13 (GET / service info reports an empty node_id: kvstore.NewKVStore drops its nodeId argument)"
	bugBI14 = "BUG B-I14 (a DELETE of a missing key forwarded over gRPC returns 200: GRPCServer.ForwardDelete maps ErrNotFound to Success:true)"
	bugBI15 = "BUG B-I15 (kv_keys_total counts every PUT, so overwriting an existing key inflates it)"
	bugBI16 = "BUG B-I16 (kv_keys_total is in-memory only and restarts at 0, so a restarted node with data reports 0 keys)"
	bugBI17 = "BUG B-I17 (compaction can emit an SST with no entries; cascading compaction of it panics in newCompactionSrc with index out of range [-1] and kills the node)"
	bugBI18 = "BUG B-I18 (the first write on a fresh engine gets seq 0 and WAL replay skips seq <= max SST seq (0 with no SSTs), so it is lost on restart)"
)

const (
	pollInterval   = 50 * time.Millisecond
	readyTimeout   = 20 * time.Second // RecoverIfNeeded runs before serving; peers fail fast when down
	replTimeout    = 10 * time.Second // async replication convergence
	settleTimeout  = 20 * time.Second // gRPC reconnect backoff after a peer restarts
	stopGrace      = 5 * time.Second
	httpClientTime = 30 * time.Second // hard ceiling so no request can hang a test
)

// eventually polls fn until it returns nil or timeout elapses, and returns the
// last error (nil on success). Callers decide how to report, so bug-tagged
// messages can be built around it.
func eventually(t testing.TB, timeout time.Duration, fn func() error) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := fn()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("still failing after %v: %w", timeout, err)
		}
		time.Sleep(pollInterval)
	}
}

/* ====================================================================================
	TOPOLOGIES
==================================================================================== */

type shardSpec struct {
	id         string
	start, end string // [start, end), "" unbounded
	leader     int
	followers  []int
}

type topoSpec struct {
	name    string
	nodeIDs []string
	shards  []shardSpec
}

// topoOneNode: a single node owning the whole key space, no peers.
func topoOneNode() topoSpec {
	return topoSpec{
		name:    "one-node",
		nodeIDs: []string{"n0"},
		shards:  []shardSpec{{id: "shard-1", start: "", end: "", leader: 0}},
	}
}

// topoSingle (T-single): 3 nodes, one shard ["",""), leader n0, followers n1, n2.
func topoSingle() topoSpec {
	return topoSpec{
		name:    "T-single",
		nodeIDs: []string{"n0", "n1", "n2"},
		shards:  []shardSpec{{id: "shard-1", start: "", end: "", leader: 0, followers: []int{1, 2}}},
	}
}

// topoMulti (T-multi): the repo's config.yaml layout. Every node belongs to every
// shard; shards split at "I" and "Q"; kvstore-N leads shard N+1.
func topoMulti() topoSpec {
	return topoSpec{
		name:    "T-multi",
		nodeIDs: []string{"kvstore-0", "kvstore-1", "kvstore-2"},
		shards: []shardSpec{
			{id: "shard-1", start: "", end: "I", leader: 0, followers: []int{1, 2}},
			{id: "shard-2", start: "I", end: "Q", leader: 1, followers: []int{0, 2}},
			{id: "shard-3", start: "Q", end: "", leader: 2, followers: []int{0, 1}},
		},
	}
}

func (s shardSpec) owns(key string) bool {
	return (s.start == "" || key >= s.start) && (s.end == "" || key < s.end)
}

// shardFor returns the shard owning key.
func (tp topoSpec) shardFor(key string) shardSpec {
	for _, s := range tp.shards {
		if s.owns(key) {
			return s
		}
	}
	panic("no shard owns key " + strconv.Quote(key))
}

/* ====================================================================================
	CONFIG
==================================================================================== */

type nodeOpts struct {
	memtableMB, sstMB, l0MB uint64
	multiplier              int
	getS, putS, deleteS     int
}

func defaultOpts() nodeOpts {
	return nodeOpts{memtableMB: 4, sstMB: 4, l0MB: 16, multiplier: 4, getS: 3, putS: 3, deleteS: 3}
}

type ports struct{ http, grpc int }

// freePorts reserves n distinct HTTP/gRPC port pairs on 127.0.0.1. All listeners
// are held open until every port is chosen so no two pairs collide.
func freePorts(n int) ([]ports, error) {
	var ls []net.Listener
	defer func() {
		for _, l := range ls {
			l.Close()
		}
	}()
	next := func() (int, error) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return 0, err
		}
		ls = append(ls, l)
		return l.Addr().(*net.TCPAddr).Port, nil
	}
	out := make([]ports, n)
	for i := range out {
		h, err := next()
		if err != nil {
			return nil, err
		}
		g, err := next()
		if err != nil {
			return nil, err
		}
		out[i] = ports{http: h, grpc: g}
	}
	return out, nil
}

// renderConfig produces node idx's config.yaml for the given topology.
func renderConfig(tp topoSpec, idx int, ps []ports, o nodeOpts) string {
	q := strconv.Quote
	var b strings.Builder
	fmt.Fprintf(&b, "server:\n")
	fmt.Fprintf(&b, "  node_id: %s\n", q(tp.nodeIDs[idx]))
	fmt.Fprintf(&b, "  http_port: %d\n", ps[idx].http)
	fmt.Fprintf(&b, "  grpc_port: %d\n", ps[idx].grpc)
	fmt.Fprintf(&b, "  data_dir: \"data\"\n")
	fmt.Fprintf(&b, "  timeouts:\n")
	fmt.Fprintf(&b, "    get_timeout_s: %d\n", o.getS)
	fmt.Fprintf(&b, "    put_timeout_s: %d\n", o.putS)
	fmt.Fprintf(&b, "    delete_timeout_s: %d\n", o.deleteS)
	fmt.Fprintf(&b, "\nstorage:\n")
	fmt.Fprintf(&b, "  memtable_size_mb: %d\n", o.memtableMB)
	fmt.Fprintf(&b, "  sst_size_mb: %d\n", o.sstMB)
	fmt.Fprintf(&b, "  level0_max_size_mb: %d\n", o.l0MB)
	fmt.Fprintf(&b, "  level_size_multiplier: %d\n", o.multiplier)
	fmt.Fprintf(&b, "\ncluster:\n  nodes:\n")
	for i, id := range tp.nodeIDs {
		fmt.Fprintf(&b, "    - id: %s\n      grpc_addr: %s\n", q(id), q(fmt.Sprintf("127.0.0.1:%d", ps[i].grpc)))
	}
	fmt.Fprintf(&b, "\n  shards:\n")
	for _, s := range tp.shards {
		fmt.Fprintf(&b, "    - id: %s\n      start_key: %s\n      end_key: %s\n      leader: %s\n",
			q(s.id), q(s.start), q(s.end), q(tp.nodeIDs[s.leader]))
		if len(s.followers) == 0 {
			fmt.Fprintf(&b, "      followers: []\n")
			continue
		}
		fmt.Fprintf(&b, "      followers:\n")
		for _, f := range s.followers {
			fmt.Fprintf(&b, "        - %s\n", q(tp.nodeIDs[f]))
		}
	}
	return b.String()
}

/* ====================================================================================
	NODE: one OS process with its own working directory
==================================================================================== */

type node struct {
	idx      int
	id       string
	dir      string
	httpPort int
	grpcPort int

	mu      sync.Mutex
	cmd     *exec.Cmd
	done    chan struct{} // closed when the current process exits
	waitErr error
	paused  bool
	starts  int
}

func (n *node) logPath() string    { return filepath.Join(n.dir, "node.log") }
func (n *node) configPath() string { return filepath.Join(n.dir, "config.yaml") }
func (n *node) baseURL() string    { return fmt.Sprintf("http://127.0.0.1:%d", n.httpPort) }

func (n *node) writeConfig(yaml string) error {
	return os.WriteFile(n.configPath(), []byte(yaml), 0o644)
}

// start launches the binary in n.dir; stdout/stderr are appended to node.log.
func (n *node) start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd != nil && !isClosed(n.done) {
		return fmt.Errorf("%s already running", n.id)
	}
	lf, err := os.OpenFile(n.logPath(), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	n.starts++
	fmt.Fprintf(lf, "\n===== harness: start #%d of %s at %s =====\n", n.starts, n.id, time.Now().Format(time.RFC3339Nano))

	cmd := exec.Command(binPath)
	cmd.Dir = n.dir
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		lf.Close()
		return err
	}
	track(cmd.Process)
	done := make(chan struct{})
	n.cmd, n.done, n.waitErr, n.paused = cmd, done, nil, false
	go func() {
		err := cmd.Wait()
		untrack(cmd.Process)
		lf.Close()
		n.mu.Lock()
		if n.cmd == cmd {
			n.waitErr = err
		}
		n.mu.Unlock()
		close(done)
	}()
	return nil
}

func isClosed(ch chan struct{}) bool {
	if ch == nil {
		return true
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (n *node) running() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cmd != nil && !isClosed(n.done)
}

// exitErr reports why the current process exited (nil if it is still running).
func (n *node) exitErr() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil || !isClosed(n.done) {
		return nil
	}
	if n.waitErr == nil {
		return errors.New("exited with status 0")
	}
	return n.waitErr
}

// waitReady polls GET / until it answers 200, failing early if the process exits.
func (n *node) waitReady(timeout time.Duration) error {
	n.mu.Lock()
	done := n.done
	n.mu.Unlock()
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(timeout)
	for {
		if isClosed(done) {
			return fmt.Errorf("%s exited before serving HTTP: %v", n.id, n.exitErr())
		}
		resp, err := client.Get(n.baseURL() + "/")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s not ready after %v (last error: %v)", n.id, timeout, err)
		}
		time.Sleep(pollInterval)
	}
}

// waitExit waits up to timeout for the current process to exit.
func (n *node) waitExit(timeout time.Duration) bool {
	n.mu.Lock()
	done := n.done
	n.mu.Unlock()
	if done == nil {
		return true
	}
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (n *node) signal(sig syscall.Signal) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd != nil && !isClosed(n.done) {
		_ = n.cmd.Process.Signal(sig)
	}
}

// kill sends SIGKILL and waits for exit.
func (n *node) kill() {
	n.signal(syscall.SIGKILL)
	n.waitExit(stopGrace)
	n.mu.Lock()
	n.paused = false
	n.mu.Unlock()
}

// term sends SIGTERM, waits a grace period, then SIGKILLs.
func (n *node) term() {
	n.signal(syscall.SIGTERM)
	n.signal(syscall.SIGCONT) // a SIGSTOPped process only acts on SIGTERM once continued
	if !n.waitExit(stopGrace) {
		n.kill()
	}
	n.mu.Lock()
	n.paused = false
	n.mu.Unlock()
}

// pause freezes the process (SIGSTOP): its sockets stay open but nothing answers.
func (n *node) pause() {
	n.signal(syscall.SIGSTOP)
	n.mu.Lock()
	n.paused = true
	n.mu.Unlock()
}

func (n *node) resume() {
	n.signal(syscall.SIGCONT)
	n.mu.Lock()
	n.paused = false
	n.mu.Unlock()
}

func (n *node) isPaused() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.paused
}

// wipe removes the node's data directory (WAL + SSTables). The node must be stopped.
func (n *node) wipe() error {
	if n.running() {
		return fmt.Errorf("wipe %s: still running", n.id)
	}
	return os.RemoveAll(filepath.Join(n.dir, "data"))
}

func (n *node) logs() string {
	b, err := os.ReadFile(n.logPath())
	if err != nil {
		return fmt.Sprintf("<no log: %v>", err)
	}
	return string(b)
}

/* ====================================================================================
	HTTP + gRPC CLIENT HELPERS
==================================================================================== */

var httpClient = &http.Client{Timeout: httpClientTime}

type result struct {
	status int
	body   string
	dur    time.Duration
}

func (r result) String() string {
	return fmt.Sprintf("status %d body %q (%v)", r.status, strings.TrimSpace(r.body), r.dur.Round(time.Millisecond))
}

func (n *node) do(method, path string, body []byte) (result, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, n.baseURL()+path, rd)
	if err != nil {
		return result{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		return result{dur: time.Since(start)}, fmt.Errorf("%s %s on %s: %w", method, path, n.id, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return result{status: resp.StatusCode, body: string(b), dur: time.Since(start)}, nil
}

func kvPath(key string) string { return "/kv/" + url.PathEscape(key) }

// put issues POST /kv with a JSON {"key","value"} body.
func (n *node) put(key, value string) (result, error) {
	body, _ := json.Marshal(map[string]string{"key": key, "value": value})
	return n.do(http.MethodPost, "/kv", body)
}

func (n *node) get(key string) (result, error) { return n.do(http.MethodGet, kvPath(key), nil) }
func (n *node) del(key string) (result, error) { return n.do(http.MethodDelete, kvPath(key), nil) }

// getValue returns (status, value). value is only meaningful when status == 200.
func (n *node) getValue(key string) (int, string, error) {
	r, err := n.get(key)
	if err != nil {
		return 0, "", err
	}
	if r.status != http.StatusOK {
		return r.status, "", nil
	}
	var out struct{ Key, Value string }
	if err := json.Unmarshal([]byte(r.body), &out); err != nil {
		return r.status, "", fmt.Errorf("GET %q on %s: bad JSON %q: %w", key, n.id, r.body, err)
	}
	if out.Key != key {
		return r.status, out.Value, fmt.Errorf("GET %q on %s: response key %q", key, n.id, out.Key)
	}
	return r.status, out.Value, nil
}

// mustPut fails the test unless the PUT returns 200. prefix (e.g. a bug tag) is
// prepended to the failure message when non-empty.
func (n *node) mustPut(t testing.TB, prefix, key, value string) {
	t.Helper()
	r, err := n.put(key, value)
	if err != nil || r.status != http.StatusOK {
		prefix, note := n.crashInfo(prefix)
		t.Fatalf("%sPUT %q via %s: got %v err=%v, want 200%s", tag(prefix), key, n.id, r, err, note)
	}
}

func (n *node) mustDel(t testing.TB, prefix, key string) {
	t.Helper()
	r, err := n.del(key)
	if err != nil || r.status != http.StatusOK {
		prefix, note := n.crashInfo(prefix)
		t.Fatalf("%sDELETE %q via %s: got %v err=%v, want 200%s", tag(prefix), key, n.id, r, err, note)
	}
}

// crashInfo explains a failed request when the node process died: it returns a
// note with the exit status and, when the log shows a known crash signature and
// no prefix was given, the matching bug tag.
func (n *node) crashInfo(prefix string) (string, string) {
	if !n.waitExit(500 * time.Millisecond) {
		return prefix, ""
	}
	note := fmt.Sprintf(" (node %s exited: %v)", n.id, n.exitErr())
	log := n.logs()
	if prefix == "" && strings.Contains(log, "index out of range [-1]") && strings.Contains(log, "newCompactionSrc") {
		prefix = bugBI17
	}
	return prefix, note
}

func tag(prefix string) string {
	if prefix == "" {
		return ""
	}
	return prefix + ": "
}

// hasValue returns nil when GET key on n is 200 with the wanted value.
func (n *node) hasValue(key, want string) error {
	st, v, err := n.getValue(key)
	if err != nil {
		return err
	}
	if st != http.StatusOK {
		return fmt.Errorf("GET %q on %s: got status %d, want 200 with %s", key, n.id, st, short(want))
	}
	if v != want {
		return fmt.Errorf("GET %q on %s: got value %s, want %s", key, n.id, short(v), short(want))
	}
	return nil
}

// isAbsent returns nil when GET key on n is 404.
func (n *node) isAbsent(key string) error {
	st, v, err := n.getValue(key)
	if err != nil {
		return err
	}
	if st != http.StatusNotFound {
		return fmt.Errorf("GET %q on %s: got status %d value %s, want 404", key, n.id, st, short(v))
	}
	return nil
}

func short(s string) string {
	if len(s) > 40 {
		return fmt.Sprintf("%q...(%d bytes)", s[:40], len(s))
	}
	return strconv.Quote(s)
}

// metric scrapes /metrics and returns the value of an un-labelled metric.
func (n *node) metric(name string) (float64, error) {
	r, err := n.do(http.MethodGet, "/metrics", nil)
	if err != nil {
		return 0, err
	}
	if r.status != http.StatusOK {
		return 0, fmt.Errorf("/metrics on %s: %v", n.id, r)
	}
	sc := bufio.NewScanner(strings.NewReader(r.body))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, name+" ") {
			return strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name+" ")), 64)
		}
	}
	return 0, fmt.Errorf("/metrics on %s: metric %s not found", n.id, name)
}

// seqNum calls the node's GetSeqNum RPC directly.
func (n *node) seqNum() (uint64, error) {
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", n.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := kvgrpc.NewKVStoreClient(conn).GetSeqNum(ctx, &kvgrpc.GetSeqNumRequest{})
	if err != nil {
		return 0, fmt.Errorf("GetSeqNum on %s: %w", n.id, err)
	}
	return resp.SeqNum, nil
}

/* ====================================================================================
	CLUSTER
==================================================================================== */

type cluster struct {
	t     *testing.T
	topo  topoSpec
	opts  nodeOpts
	nodes []*node
	probe int // counter for unique settle probe keys
}

// newCluster creates one temp dir and config per node (not started). Cleanup kills
// every process and dumps all node logs if the test failed.
func newCluster(t *testing.T, tp topoSpec, o nodeOpts) *cluster {
	t.Helper()
	ps, err := freePorts(len(tp.nodeIDs))
	if err != nil {
		t.Fatalf("harness: free ports: %v", err)
	}
	c := &cluster{t: t, topo: tp, opts: o}
	for i, id := range tp.nodeIDs {
		n := &node{idx: i, id: id, dir: t.TempDir(), httpPort: ps[i].http, grpcPort: ps[i].grpc}
		if err := n.writeConfig(renderConfig(tp, i, ps, o)); err != nil {
			t.Fatalf("harness: write config for %s: %v", id, err)
		}
		c.nodes = append(c.nodes, n)
	}
	// Registered after every t.TempDir, so it runs before the dirs are removed.
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.kill()
		}
		if t.Failed() {
			for _, n := range c.nodes {
				t.Logf("----- %s log (%s) -----\n%s", n.id, n.dir, n.logs())
			}
		}
	})
	return c
}

func (c *cluster) node(id string) *node {
	for _, n := range c.nodes {
		if n.id == id {
			return n
		}
	}
	c.t.Fatalf("harness: no node %q", id)
	return nil
}

// leaderOf returns the leader node of the shard owning key.
func (c *cluster) leaderOf(key string) *node { return c.nodes[c.topo.shardFor(key).leader] }

// startAll starts every node concurrently and waits until all serve HTTP.
func (c *cluster) startAll() {
	c.t.Helper()
	for _, n := range c.nodes {
		if err := n.start(); err != nil {
			c.t.Fatalf("harness: start %s: %v", n.id, err)
		}
	}
	for _, n := range c.nodes {
		if err := n.waitReady(readyTimeout); err != nil {
			c.t.Fatalf("harness: %v", err)
		}
	}
}

// startNode starts (or restarts after a stop) one node and waits for readiness.
func (c *cluster) startNode(n *node) {
	c.t.Helper()
	if err := n.start(); err != nil {
		c.t.Fatalf("harness: start %s: %v", n.id, err)
	}
	if err := n.waitReady(readyTimeout); err != nil {
		c.t.Fatalf("harness: %v", err)
	}
}

func (c *cluster) restart(n *node) {
	c.t.Helper()
	n.term()
	c.startNode(n)
}

func (c *cluster) wipe(n *node) {
	c.t.Helper()
	if err := n.wipe(); err != nil {
		c.t.Fatalf("harness: %v", err)
	}
}

func (c *cluster) live() []*node {
	var out []*node
	for _, n := range c.nodes {
		if n.running() && !n.isPaused() {
			out = append(out, n)
		}
	}
	return out
}

// probeKey returns a fresh key inside shard s.
func (c *cluster) probeKey(s shardSpec) string {
	c.probe++
	prefix := s.start
	if prefix == "" {
		prefix = "0" // sorts before "I", so it stays in a lower-unbounded shard
	}
	return fmt.Sprintf("%s__settle_%d", prefix, c.probe)
}

// settle waits until the live nodes are wired up: every live node can write every
// shard (exercising its connection to the shard leader), and every live shard
// leader's writes reach every live node (exercising replication). It exists
// because gRPC channels to a peer that was down sit in reconnect backoff for a
// while after the peer comes back. prefix tags the failure (e.g. a bug), or "".
func (c *cluster) settle(prefix string) {
	c.t.Helper()
	live := c.live()
	isLive := map[*node]bool{}
	for _, n := range live {
		isLive[n] = true
	}
	for _, s := range c.topo.shards {
		for _, n := range live {
			err := eventually(c.t, settleTimeout, func() error {
				k := c.probeKey(s)
				r, err := n.put(k, "probe")
				if err != nil {
					return err
				}
				if r.status != http.StatusOK {
					return fmt.Errorf("PUT %q via %s: %v", k, n.id, r)
				}
				return nil
			})
			if err != nil {
				c.t.Fatalf("%scluster did not settle: %s cannot write shard %s: %v", tag(prefix), n.id, s.id, err)
			}
		}
		leader := c.nodes[s.leader]
		if !isLive[leader] {
			continue
		}
		err := eventually(c.t, settleTimeout, func() error {
			k := c.probeKey(s)
			if r, err := leader.put(k, "probe"); err != nil || r.status != http.StatusOK {
				return fmt.Errorf("PUT %q via leader %s: %v err=%v", k, leader.id, r, err)
			}
			// Short inner window: a write sent while the channel was in backoff
			// is dropped (fire-and-forget), so retry with a fresh key instead.
			return eventually(c.t, 2*time.Second, func() error {
				for _, n := range live {
					if err := n.hasValue(k, "probe"); err != nil {
						return err
					}
				}
				return nil
			})
		})
		if err != nil {
			c.t.Fatalf("%scluster did not settle: writes to shard %s leader %s do not reach every live node: %v",
				tag(prefix), s.id, leader.id, err)
		}
	}
}

/* ====================================================================================
	BOOT CHECK (I0): runs once; every scenario calls requireBoot first.
==================================================================================== */

var (
	bootOnce sync.Once
	bootMsg  string // "" when the binary boots
)

// requireBoot fails the test immediately with the I0 result if the binary cannot
// boot a valid single-node config, instead of letting the test hit timeouts.
func requireBoot(t testing.TB) {
	t.Helper()
	if msg := bootCheck(); msg != "" {
		t.Fatal(msg)
	}
}

func bootCheck() string {
	bootOnce.Do(func() { bootMsg = runBootCheck() })
	return bootMsg
}

func runBootCheck() string {
	dir, err := os.MkdirTemp("", "kvstore-it-boot-")
	if err != nil {
		return "harness: boot check: " + err.Error()
	}
	defer os.RemoveAll(dir)
	ps, err := freePorts(1)
	if err != nil {
		return "harness: boot check: " + err.Error()
	}
	tp := topoOneNode()
	n := &node{id: tp.nodeIDs[0], dir: dir, httpPort: ps[0].http, grpcPort: ps[0].grpc}
	if err := n.writeConfig(renderConfig(tp, 0, ps, defaultOpts())); err != nil {
		return "harness: boot check: " + err.Error()
	}
	if err := n.start(); err != nil {
		return "harness: boot check: " + err.Error()
	}
	defer n.kill()

	readyErr := n.waitReady(readyTimeout)
	if readyErr == nil {
		return ""
	}
	exited := !n.running()
	n.kill()
	log := n.logs()
	if exited && strings.Contains(log, "level0_max_size_mb must be greater than 0") {
		return fmt.Sprintf("%s: node %s with a valid config (level0_max_size_mb: %d) %v; want it to boot and answer GET / with 200\n--- node log ---\n%s",
			bugB1, n.id, defaultOpts().l0MB, readyErr, log)
	}
	return fmt.Sprintf("node %s failed to boot with a valid config (not attributed to a known bug): %v\n--- node log ---\n%s",
		n.id, readyErr, log)
}

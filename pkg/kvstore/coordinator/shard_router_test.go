package coordinator

import (
	"sync"
	"testing"
)

// named returns distinct NodeClients that can be told apart by pointer identity.
func named(n int) []*mockNodeClient {
	out := make([]*mockNodeClient, n)
	for i := range out {
		out[i] = &mockNodeClient{}
	}
	return out
}

func TestShardRouter_FindShard_Bounds(t *testing.T) {
	n := named(3)
	r := NewShardRouter([]ShardNodeConfig{
		{MinKey: "", MaxKey: "I", Leader: n[0]},
		{MinKey: "I", MaxKey: "Q", Leader: n[1]},
		{MinKey: "Q", MaxKey: "", Leader: n[2]},
	})
	cases := []struct {
		key  string
		want NodeClient
	}{
		{"", n[0]},
		{"A", n[0]},
		{"H", n[0]},
		{"Hzzz", n[0]}, // just below "I"
		{"I", n[1]},    // equal to start: inclusive
		{"I\x00", n[1]},
		{"Pzzz", n[1]},
		{"Q", n[2]}, // equal to end of previous shard: exclusive there
		{"z", n[2]},
		{"\xff\xff", n[2]},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			got, err := r.RouteWrite(tc.key)
			if err != nil {
				t.Fatalf("RouteWrite(%q): %v", tc.key, err)
			}
			if got != tc.want {
				t.Fatalf("RouteWrite(%q) picked the wrong shard", tc.key)
			}
			got, err = r.RouteRead(tc.key)
			if err != nil {
				t.Fatalf("RouteRead(%q): %v", tc.key, err)
			}
			if got != tc.want {
				t.Fatalf("RouteRead(%q) picked the wrong shard", tc.key)
			}
		})
	}
}

func TestShardRouter_NoShardForKey(t *testing.T) {
	n := named(1)
	cases := map[string]*ShardRouter{
		"nil configs":  NewShardRouter(nil),
		"gap in range": NewShardRouter([]ShardNodeConfig{{MinKey: "m", MaxKey: "", Leader: n[0]}}),
	}
	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			if c, err := r.RouteRead("a"); err == nil {
				t.Errorf("RouteRead(a) = (%v, nil), want error", c)
			}
			if c, err := r.RouteWrite("a"); err == nil {
				t.Errorf("RouteWrite(a) = (%v, nil), want error", c)
			}
		})
	}
}

func TestShardRouter_RoundRobinReads_LeaderAndFollowers(t *testing.T) {
	n := named(3)
	r := NewShardRouter([]ShardNodeConfig{
		{MinKey: "", MaxKey: "", Leader: n[0], Followers: []NodeClient{n[1], n[2]}},
	})
	want := []NodeClient{n[0], n[1], n[2], n[0], n[1], n[2], n[0]}
	for i, w := range want {
		got, err := r.RouteRead("k")
		if err != nil {
			t.Fatal(err)
		}
		if got != w {
			t.Fatalf("read #%d went to node %v, want node %v (leader first, then followers, cycling)", i, indexOf(n, got), indexOf(n, w))
		}
	}
}

func TestShardRouter_RoundRobinIsPerShard(t *testing.T) {
	a, b := named(2), named(2)
	r := NewShardRouter([]ShardNodeConfig{
		{MinKey: "", MaxKey: "m", Leader: a[0], Followers: []NodeClient{a[1]}},
		{MinKey: "m", MaxKey: "", Leader: b[0], Followers: []NodeClient{b[1]}},
	})
	// Reads to shard A must not advance shard B's cursor.
	r.RouteRead("a")
	r.RouteRead("a")
	r.RouteRead("a")
	if got, _ := r.RouteRead("z"); got != b[0] {
		t.Fatalf("first read on shard B went to node %d, want its leader", indexOf(b, got))
	}
}

func TestShardRouter_WritesAlwaysGoToLeader(t *testing.T) {
	n := named(3)
	r := NewShardRouter([]ShardNodeConfig{
		{MinKey: "", MaxKey: "", Leader: n[0], Followers: []NodeClient{n[1], n[2]}},
	})
	for i := 0; i < 10; i++ {
		r.RouteRead("k") // interleaved reads must not affect write routing
		got, err := r.RouteWrite("k")
		if err != nil {
			t.Fatal(err)
		}
		if got != n[0] {
			t.Fatalf("write #%d went to node %d, want leader", i, indexOf(n, got))
		}
	}
}

func TestShardRouter_NoLeader(t *testing.T) {
	n := named(2)
	r := NewShardRouter([]ShardNodeConfig{
		{MinKey: "", MaxKey: "", Leader: nil, Followers: []NodeClient{n[0], n[1]}},
	})
	if c, err := r.RouteWrite("k"); err == nil {
		t.Fatalf("RouteWrite with no leader = (%v, nil), want error", c)
	}
	// Reads still work over the followers.
	for i, w := range []NodeClient{n[0], n[1], n[0]} {
		got, err := r.RouteRead("k")
		if err != nil {
			t.Fatalf("RouteRead #%d: %v", i, err)
		}
		if got != w {
			t.Fatalf("read #%d went to node %d, want %d", i, indexOf(n, got), indexOf(n, w))
		}
	}
}

func TestShardRouter_NoNodes(t *testing.T) {
	r := NewShardRouter([]ShardNodeConfig{{MinKey: "", MaxKey: ""}})
	if c, err := r.RouteRead("k"); err == nil {
		t.Fatalf("RouteRead with no nodes = (%v, nil), want error", c)
	}
	if c, err := r.RouteWrite("k"); err == nil {
		t.Fatalf("RouteWrite with no nodes = (%v, nil), want error", c)
	}
}

// Concurrent reads must be spread exactly evenly (atomic cursor, no lost
// increments). Run with -race.
func TestShardRouter_ConcurrentReadsAreFair(t *testing.T) {
	n := named(3)
	r := NewShardRouter([]ShardNodeConfig{
		{MinKey: "", MaxKey: "", Leader: n[0], Followers: []NodeClient{n[1], n[2]}},
	})
	const goroutines, perG = 8, 300
	var mu sync.Mutex
	counts := map[NodeClient]int{}
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := map[NodeClient]int{}
			for i := 0; i < perG; i++ {
				c, err := r.RouteRead("k")
				if err != nil {
					t.Error(err)
					return
				}
				local[c]++
			}
			mu.Lock()
			for k, v := range local {
				counts[k] += v
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	want := goroutines * perG / len(n)
	for i, c := range n {
		if counts[c] != want {
			t.Errorf("node %d served %d reads, want exactly %d", i, counts[c], want)
		}
	}
}

func indexOf(ns []*mockNodeClient, c NodeClient) int {
	for i, n := range ns {
		if NodeClient(n) == c {
			return i
		}
	}
	return -1
}

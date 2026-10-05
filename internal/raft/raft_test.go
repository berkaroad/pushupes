package raft

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// ---- test scaffolding ----

type testLogger struct{ t testing.TB }

func (l testLogger) Debugf(f string, a ...any) {}
func (l testLogger) Infof(f string, a ...any)  {}
func (l testLogger) Warnf(f string, a ...any)  { l.t.Logf("WARN "+f, a...) }
func (l testLogger) Errorf(f string, a ...any) { l.t.Logf("ERROR "+f, a...) }

// testNet is a real TCP listener: the tests exercise framing, handshakes and
// the first-byte demux magic, not an in-memory shortcut.
type testNet struct {
	ln   net.Listener
	addr string
}

func newTestNet(t testing.TB) *testNet {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return &testNet{ln: ln, addr: ln.Addr().String()}
}

func (n *testNet) Accept() (net.Conn, error) { return n.ln.Accept() }
func (n *testNet) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}
func (n *testNet) Addr() string { return n.addr }
func (n *testNet) Close() error { return n.ln.Close() }

func (n *testNet) rebind() error {
	_ = n.ln.Close()
	ln, err := net.Listen("tcp", n.addr)
	if err != nil {
		return err
	}
	n.ln = ln
	return nil
}

type testFSM struct {
	mu      sync.Mutex
	data    map[string]string
	applied uint64
}

func newTestFSM() *testFSM { return &testFSM{data: map[string]string{}} }

func (f *testFSM) Apply(e Entry) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied = e.Index
	parts := strings.Fields(string(e.Data))
	switch {
	case len(parts) == 3 && parts[0] == "set":
		f.data[parts[1]] = parts[2]
	case len(parts) == 2 && parts[0] == "del":
		delete(f.data, parts[1])
	}
	return nil
}

func (f *testFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.data))
	for k := range f.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, f.data[k])
	}
	return []byte(b.String()), nil
}

func (f *testFSM) Restore(data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data = map[string]string{}
	sc := bufio.NewScanner(strings.NewReader(string(data)))
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		kv := strings.SplitN(line, "=", 2)
		if len(kv) == 2 {
			f.data[kv[0]] = kv[1]
		}
	}
	return nil
}

func (f *testFSM) get(k string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[k]
	return v, ok
}

type testNode struct {
	id   string
	dir  string
	net  *testNet
	node *Node
	fsm  *testFSM
}

type testCluster struct {
	t      testing.TB
	voters []Voter
	nodes  map[string]*testNode
	order  []string
	mutate func(*Config)
}

func testConfig(id string, dir string, voters []Voter, log Logger) Config {
	return Config{
		NodeID:            id,
		DataDir:           dir,
		Voters:            voters,
		ApplyTimeout:      5 * time.Second,
		SnapshotThreshold: 1 << 30, // effectively off unless a test lowers it
		SnapshotInterval:  time.Hour,
		HeartbeatTimeout:  30 * time.Millisecond,
		ElectionTimeout:   120 * time.Millisecond,
		FlushInterval:     200 * time.Microsecond,
		Logger:            log,
	}
}

func startCluster(t testing.TB, n int) *testCluster {
	return startClusterCfg(t, n, nil)
}

func startClusterCfg(t testing.TB, n int, mutate func(*Config)) *testCluster {
	t.Helper()
	c := &testCluster{t: t, nodes: map[string]*testNode{}, mutate: mutate}
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		tn := newTestNet(t)
		c.voters = append(c.voters, Voter{ID: id, Addr: tn.addr})
		c.nodes[id] = &testNode{id: id, net: tn}
		c.order = append(c.order, id)
	}
	for _, id := range c.order {
		c.start(id, false)
	}
	return c
}

func (c *testCluster) start(id string, reuseDir bool) {
	c.t.Helper()
	tn := c.nodes[id]
	dir := tn.dir
	if !reuseDir || dir == "" {
		dir = c.t.TempDir()
	}
	fsm := newTestFSM()
	cfg := testConfig(id, dir, c.voters, testLogger{c.t})
	if c.mutate != nil {
		c.mutate(&cfg)
	}
	node, err := NewNode(cfg, fsm, tn.net)
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	tn.dir = dir
	tn.fsm = fsm
	tn.node = node
}

func (c *testCluster) stop(id string) {
	c.t.Helper()
	if n := c.nodes[id].node; n != nil {
		n.Close()
		c.nodes[id].node = nil
	}
}

func (c *testCluster) leader(within time.Duration) string {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		leaders := []string{}
		for _, id := range c.order {
			if n := c.nodes[id].node; n != nil && n.IsLeader() {
				leaders = append(leaders, id)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %s", within)
	return ""
}

func (c *testCluster) apply(id, cmd string) (any, error) {
	return c.nodes[id].node.Apply([]byte(cmd), 5*time.Second)
}

func (c *testCluster) waitApplied(want uint64, within time.Duration, except string) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range c.order {
			if id == except {
				continue
			}
			tn := c.nodes[id]
			if tn.node == nil {
				continue
			}
			if tn.node.AppliedIndex() < want {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("not all nodes applied index %d within %s", want, within)
}

// ---- tests ----

func TestSingleNodeElectsAndApplies(t *testing.T) {
	c := startCluster(t, 1)
	defer c.stopAll()
	id := c.leader(3 * time.Second)
	if _, err := c.apply(id, "set a 1"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if v, ok := c.nodes[id].fsm.get("a"); !ok || v != "1" {
		t.Fatalf("fsm did not apply: %q %v", v, ok)
	}
}

func TestThreeNodesElectOneLeaderAndReplicate(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)
	for i := 0; i < 10; i++ {
		if _, err := c.apply(leader, fmt.Sprintf("set k%d v%d", i, i)); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	last := c.nodes[leader].node.LastIndex()
	c.waitApplied(last, 3*time.Second, "")
	for _, id := range c.order {
		if v, ok := c.nodes[id].fsm.get("k9"); !ok || v != "v9" {
			t.Fatalf("%s missing k9: %q %v", id, v, ok)
		}
	}
}

func TestFollowerRefusesApply(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)
	follower := ""
	for _, id := range c.order {
		if id != leader {
			follower = id
			break
		}
	}
	if _, err := c.apply(follower, "set x 1"); err == nil {
		t.Fatalf("follower accepted an apply")
	}
}

func TestLeaderFailoverKeepsCommittedData(t *testing.T) {
	c := startCluster(t, 3)
	defer c.stopAll()
	leader := c.leader(3 * time.Second)
	for i := 0; i < 5; i++ {
		if _, err := c.apply(leader, fmt.Sprintf("set f%d v", i)); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	c.stop(leader)
	next := c.leaderAmong(3*time.Second, leader)
	if _, err := c.apply(next, "set after failover 1"); err != nil {
		t.Fatalf("apply after failover: %v", err)
	}
	drain := c.nodes[next].node.LastIndex()
	c.waitApplied(drain, 5*time.Second, leader)
	for _, id := range c.order {
		if id == leader || c.nodes[id].node == nil {
			continue
		}
		if v, ok := c.nodes[id].fsm.get("f4"); !ok || v != "v" {
			t.Fatalf("%s lost committed data after failover: %q %v", id, v, ok)
		}
	}
}

func (c *testCluster) leaderAmong(within time.Duration, exclude ...string) string {
	c.t.Helper()
	deadline := time.Now().Add(within)
	skip := map[string]bool{}
	for _, e := range exclude {
		skip[e] = true
	}
	for time.Now().Before(deadline) {
		for _, id := range c.order {
			if skip[id] {
				continue
			}
			if n := c.nodes[id].node; n != nil && n.IsLeader() {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("no leader among survivors within %s", within)
	return ""
}

func TestTermIsPersistedAcrossRestart(t *testing.T) {
	c := startCluster(t, 1)
	defer c.stopAll()
	id := c.leader(3 * time.Second)
	if _, err := c.apply(id, "set a 1"); err != nil {
		t.Fatalf("apply: %v", err)
	}
	term := c.nodes[id].node.Term()
	if term == 0 {
		t.Fatalf("term is zero")
	}
	c.stop(id)
	if err := c.nodes[id].net.rebind(); err != nil {
		t.Fatalf("rebind: %v", err)
	}
	c.start(id, true)
	got := c.nodes[id].node.Term()
	if got < term {
		t.Fatalf("term went backwards: %d -> %d", term, got)
	}
	c.leader(3 * time.Second)
	if v, ok := c.nodes[id].fsm.get("a"); !ok || v != "1" {
		t.Fatalf("committed data did not come back after restart: %q %v", v, ok)
	}
}

func TestConfigMismatchIsRefused(t *testing.T) {
	dir := t.TempDir()
	voters := []Voter{{ID: "a", Addr: "127.0.0.1:1"}, {ID: "b", Addr: "127.0.0.1:2"}}
	netA := newTestNet(t)
	defer netA.Close()
	node, err := NewNode(testConfig("a", dir, voters, testLogger{t}), newTestFSM(), netA)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	node.Close()

	changed := []Voter{{ID: "a", Addr: "127.0.0.1:1"}, {ID: "b", Addr: "127.0.0.1:9"}}
	if _, err := NewNode(testConfig("a", dir, changed, testLogger{t}), newTestFSM(), netA); err == nil {
		t.Fatalf("started with a changed membership")
	}
}

func TestSnapshotKeepsLogBoundedAndSurvivesRestart(t *testing.T) {
	c := startClusterCfg(t, 1, func(cfg *Config) {
		cfg.SnapshotThreshold = 8
		cfg.SnapshotInterval = 50 * time.Millisecond
	})
	defer c.stopAll()
	id := c.leader(3 * time.Second)
	tn := c.nodes[id]
	for i := 0; i < 60; i++ {
		if _, err := c.apply(id, fmt.Sprintf("set s%d v%d", i, i)); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	if err := tn.node.Barrier(5 * time.Second); err != nil {
		t.Fatalf("barrier: %v", err)
	}
	// A snapshot must have happened and the in-memory log must start above it.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if snapIdx, _, _ := tn.node.log.Snapshot(); snapIdx > 0 {
			first := tn.node.log.FirstIndex()
			if first <= snapIdx {
				t.Fatalf("log first index %d is not above snapshot %d", first, snapIdx)
			}
			if got := first - snapIdx; got > 8 {
				t.Fatalf("memory log kept %d entries above the snapshot, want <= 8", got)
			}
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if snapIdx, _, _ := tn.node.log.Snapshot(); snapIdx == 0 {
		t.Fatalf("no snapshot was taken")
	}

	c.stop(id)
	if err := tn.net.rebind(); err != nil {
		t.Fatalf("rebind: %v", err)
	}
	c.start(id, true)
	c.leader(3 * time.Second)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if v, ok := c.nodes[id].fsm.get("s59"); ok && v == "v59" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("state lost across restart")
}

func (c *testCluster) stopAll() {
	for _, id := range c.order {
		if n := c.nodes[id].node; n != nil {
			n.Close()
		}
		if c.nodes[id].net != nil {
			c.nodes[id].net.Close()
		}
	}
}

func TestClusterSizesRepeatedly(t *testing.T) {
	for _, size := range []int{1, 3, 5, 7} {
		size := size
		t.Run(fmt.Sprintf("n%d", size), func(t *testing.T) {
			for round := 0; round < 3; round++ {
				c := startCluster(t, size)
				leader := c.leader(5 * time.Second)
				for i := 0; i < 5; i++ {
					if _, err := c.apply(leader, fmt.Sprintf("set r%d v%d", i, i)); err != nil {
						t.Fatalf("round %d apply: %v", round, err)
					}
				}
				last := c.nodes[leader].node.LastIndex()
				c.waitApplied(last, 5*time.Second, "")
				if size > 1 {
					c.stop(leader)
					next := c.leaderAmong(5*time.Second, leader)
					if _, err := c.apply(next, "set after 1"); err != nil {
						t.Fatalf("round %d apply after failover: %v", round, err)
					}
					last = c.nodes[next].node.LastIndex()
					c.waitApplied(last, 5*time.Second, leader)
				}
				c.stopAll()
			}
		})
	}
}

func TestWALFlockIsExclusive(t *testing.T) {
	dir := t.TempDir()
	w1, err := OpenWAL(dir, WALOptions{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w1.Close()
	if _, err := OpenWAL(dir, WALOptions{}); err == nil {
		t.Fatalf("second open succeeded despite the lock")
	}
	w1.Close()
	w2, err := OpenWAL(dir, WALOptions{})
	if err != nil {
		t.Fatalf("reopen after close: %v", err)
	}
	w2.Close()
}

func TestWALRecoversAndTruncatesTail(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenWAL(dir, WALOptions{SegmentBytes: 256})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for i := 0; i < 20; i++ {
		lsn, err := w.Append(RecordEntry, encodeEntry(Entry{Index: uint64(i + 1), Term: 1, Kind: KindCommand, Data: []byte("payload")}))
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		if err := w.Wait(lsn); err != nil {
			t.Fatalf("wait: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	files, _ := walSegFiles(dir)
	if len(files) < 2 {
		t.Fatalf("expected segment rotation, got %d files", len(files))
	}
	// Chop bytes off the tail of the last segment: the final record becomes
	// partial and only it may be dropped.
	last := files[len(files)-1]
	info, err := os.Stat(last)
	if err != nil {
		t.Fatalf("stat last: %v", err)
	}
	if err := os.Truncate(last, info.Size()-5); err != nil {
		t.Fatalf("truncate last: %v", err)
	}

	w2, err := OpenWAL(dir, WALOptions{SegmentBytes: 256})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	recs := w2.Records()
	if len(recs) != 19 {
		t.Fatalf("expected 19 records after dropping the partial tail, got %d", len(recs))
	}
	for i, r := range recs {
		e, err := decodeEntry(r.Payload)
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if e.Index != uint64(i+1) {
			t.Fatalf("record %d has index %d, want %d", i, e.Index, i+1)
		}
	}
}

func TestWALDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	w, _ := OpenWAL(dir, WALOptions{})
	lsn, _ := w.Append(RecordEntry, encodeEntry(Entry{Index: 1, Term: 1, Data: []byte("hello world")}))
	w.Wait(lsn)
	w.Close()

	files, _ := walSegFiles(dir)
	raw, _ := os.ReadFile(files[0])
	raw[recordHeaderLen+2] ^= 0xff // flip a payload bit
	os.WriteFile(files[0], raw, 0o644)

	w2, err := OpenWAL(dir, WALOptions{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if n := len(w2.Records()); n != 0 {
		t.Fatalf("corrupt record was accepted: %d records", n)
	}
	// A fresh append must still work after the bad tail was dropped.
	lsn, err = w2.Append(RecordEntry, encodeEntry(Entry{Index: 1, Term: 2, Data: []byte("again")}))
	if err != nil {
		t.Fatalf("append after truncate: %v", err)
	}
	if err := w2.Wait(lsn); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

func TestHandshakeMagicIsNotGRPCPreface(t *testing.T) {
	if wireMagic == 'P' {
		t.Fatalf("handshake magic collides with the HTTP/2 preface byte")
	}
}

package chaos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"quorum/internal/kv"
	"quorum/internal/raft"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type chaosCluster struct {
	t        *testing.T
	nodes    []*raft.Node
	servers  []*raft.RPCServer
	stores   []*kv.Store
	applyChs []chan raft.ApplyMsg
	dataDir  string
	mu       sync.Mutex
	basePort int
	peers    [][]string
}

func newChaosCluster(t *testing.T, n int) *chaosCluster {
	dataDir, err := os.MkdirTemp("", "chaos-test-*")
	require.NoError(t, err)

	cc := &chaosCluster{
		t:        t,
		nodes:    make([]*raft.Node, n),
		servers:  make([]*raft.RPCServer, n),
		stores:   make([]*kv.Store, n),
		applyChs: make([]chan raft.ApplyMsg, n),
		dataDir:  dataDir,
		basePort: 17000 + cryptoRandIntn(1000),
	}

	cc.peers = make([][]string, n)
	for i := 0; i < n; i++ {
		cc.peers[i] = make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				cc.peers[i] = append(cc.peers[i], fmt.Sprintf("localhost:%d", cc.basePort+j))
			}
		}
	}

	for i := 0; i < n; i++ {
		cc.startNode(i)
	}

	return cc
}

func (cc *chaosCluster) startNode(i int) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	nodeID := fmt.Sprintf("node-%d", i)
	nodeDir := fmt.Sprintf("%s/%s", cc.dataDir, nodeID)

	persister, err := raft.NewPersister(nodeDir)
	require.NoError(cc.t, err)

	cc.applyChs[i] = make(chan raft.ApplyMsg, 1000)
	cc.nodes[i] = raft.NewNode(nodeID, cc.peers[i], cc.applyChs[i], persister)

	cc.servers[i], err = raft.NewRPCServer(cc.nodes[i], cc.basePort+i)
	require.NoError(cc.t, err)

	cc.nodes[i].Start()
	cc.stores[i] = kv.NewStore(cc.nodes[i], cc.applyChs[i])
}

func (cc *chaosCluster) stopNode(i int) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.nodes[i] != nil {
		cc.nodes[i].Stop()
		cc.nodes[i] = nil
	}
	if cc.servers[i] != nil {
		err := cc.servers[i].Close()
		if err != nil {
			return
		}
		cc.servers[i] = nil
	}
	cc.stores[i] = nil
}

func (cc *chaosCluster) cleanup() {
	for i := range cc.nodes {
		cc.stopNode(i)
	}
	err := os.RemoveAll(cc.dataDir)
	if err != nil {
		return
	}
}

func (cc *chaosCluster) waitForLeader(timeout time.Duration) (leader *kv.Store, idx int) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cc.mu.Lock()
		for i, store := range cc.stores {
			if store != nil && store.IsLeader() {
				cc.mu.Unlock()
				return store, i
			}
		}
		cc.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
	return nil, -1
}

func (cc *chaosCluster) getLeader() (leader *kv.Store, idx int) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for i, store := range cc.stores {
		if store != nil && store.IsLeader() {
			return store, i
		}
	}
	return nil, -1
}

func (cc *chaosCluster) getAnyStore() *kv.Store {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	for _, store := range cc.stores {
		if store != nil {
			return store
		}
	}
	return nil
}

func (cc *chaosCluster) countAlive() int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	count := 0
	for _, node := range cc.nodes {
		if node != nil {
			count++
		}
	}
	return count
}

func cryptoRandIntn(n int) int {
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	return int(v.Int64())
}

// --- Chaos Tests ---

func TestChaos_RandomNodeFailures(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cc := newChaosCluster(t, 5)
	defer cc.cleanup()

	leader, _ := cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Track successful writes
	var successCount atomic.Int64
	var failCount atomic.Int64
	written := sync.Map{}

	// Writer goroutine
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stopCh:
				return
			default:
			}

			store, _ := cc.getLeader()
			if store == nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}

			key := fmt.Sprintf("chaos-key-%d", i)
			value := fmt.Sprintf("chaos-value-%d", i)

			err := store.Put(key, value)
			if err == nil {
				successCount.Add(1)
				written.Store(key, value)
			} else {
				failCount.Add(1)
			}

			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Chaos goroutine - randomly kill and restart nodes
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			select {
			case <-stopCh:
				return
			default:
			}

			// Only kill if we have a majority
			if cc.countAlive() <= 3 {
				time.Sleep(200 * time.Millisecond)
				continue
			}

			victim := cryptoRandIntn(5)
			t.Logf("Killing node %d", victim)
			cc.stopNode(victim)

			time.Sleep(300 * time.Millisecond)

			t.Logf("Restarting node %d", victim)
			cc.startNode(victim)

			time.Sleep(500 * time.Millisecond)
		}
	}()

	// Run for a while
	time.Sleep(10 * time.Second)
	close(stopCh)
	wg.Wait()

	t.Logf("Writes succeeded: %d, failed: %d", successCount.Load(), failCount.Load())

	// Wait for the cluster to stabilize
	time.Sleep(2 * time.Second)
	leader, _ = cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Verify all successful writes are readable
	var verified int
	written.Range(func(k, v interface{}) bool {
		key := k.(string)
		expected := v.(string)

		value, ok, err := leader.Get(key)
		if err != nil {
			t.Errorf("Error reading %s: %v", key, err)
			return true
		}
		if !ok {
			t.Errorf("Key %s not found", key)
			return true
		}
		if value != expected {
			t.Errorf("Key %s: expected %s, got %s", key, expected, value)
			return true
		}
		verified++
		return true
	})

	t.Logf("Verified %d keys", verified)
	assert.Greater(t, verified, 0)
}

func TestChaos_RapidLeaderChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cc := newChaosCluster(t, 5)
	defer cc.cleanup()

	leader, _ := cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Write some initial data
	for i := 0; i < 10; i++ {
		err := leader.Put(fmt.Sprintf("initial-%d", i), fmt.Sprintf("value-%d", i))
		require.NoError(t, err)
	}

	// Repeatedly kill the leader
	for round := 0; round < 5; round++ {
		leader, leaderIdx := cc.getLeader()
		if leader == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		t.Logf("Round %d: killing leader node-%d", round, leaderIdx)
		cc.stopNode(leaderIdx)

		// Wait for a new leader
		time.Sleep(500 * time.Millisecond)
		newLeader, _ := cc.waitForLeader(5 * time.Second)
		require.NotNil(t, newLeader, "no new leader after killing node-%d", leaderIdx)

		// Write more data
		err := newLeader.Put(fmt.Sprintf("round-%d", round), fmt.Sprintf("value-%d", round))
		require.NoError(t, err)

		// Restart the old leader
		cc.startNode(leaderIdx)
		time.Sleep(300 * time.Millisecond)
	}

	// Final verification
	leader, _ = cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Check initial data
	for i := 0; i < 10; i++ {
		value, ok, err := leader.Get(fmt.Sprintf("initial-%d", i))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("value-%d", i), value)
	}

	// Check round data
	for i := 0; i < 5; i++ {
		value, ok, err := leader.Get(fmt.Sprintf("round-%d", i))
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprintf("value-%d", i), value)
	}
}

func TestChaos_ConcurrentClients(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cc := newChaosCluster(t, 5)
	defer cc.cleanup()

	leader, _ := cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	numClients := 10
	writesPerClient := 50
	var wg sync.WaitGroup
	written := sync.Map{}
	var totalSuccess atomic.Int64

	// Spawn concurrent writers
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			for i := 0; i < writesPerClient; i++ {
				store, _ := cc.getLeader()
				if store == nil {
					time.Sleep(50 * time.Millisecond)
					continue
				}

				key := fmt.Sprintf("client-%d-key-%d", clientID, i)
				value := fmt.Sprintf("client-%d-value-%d", clientID, i)

				err := store.Put(key, value)
				if err == nil {
					written.Store(key, value)
					totalSuccess.Add(1)
				}
			}
		}(c)
	}

	wg.Wait()
	t.Logf("Total successful writes: %d", totalSuccess.Load())

	// Verify
	time.Sleep(1 * time.Second)
	leader, _ = cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	var verified int
	written.Range(func(k, v interface{}) bool {
		key := k.(string)
		expected := v.(string)

		value, ok, err := leader.Get(key)
		assert.NoError(t, err)
		assert.True(t, ok, "missing key: %s", key)
		assert.Equal(t, expected, value)
		verified++
		return true
	})

	t.Logf("Verified %d keys", verified)
	assert.Equal(t, int(totalSuccess.Load()), verified)
}

func TestChaos_MinorityPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cc := newChaosCluster(t, 5)
	defer cc.cleanup()

	leader, _ := cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Write initial data
	err := leader.Put("before-partition", "value")
	require.NoError(t, err)

	// Kill minority (2 nodes)
	cc.stopNode(3)
	cc.stopNode(4)
	t.Log("Killed nodes 3 and 4 (minority)")

	// Cluster should still work with 3 nodes
	time.Sleep(500 * time.Millisecond)
	leader, _ = cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "cluster should work with 3/5 nodes")

	// Write during partition
	err = leader.Put("during-partition", "value")
	require.NoError(t, err)

	// Bring minority back
	cc.startNode(3)
	cc.startNode(4)
	t.Log("Restarted nodes 3 and 4")

	// Wait for them to catch up
	time.Sleep(2 * time.Second)

	// All nodes should have all data
	for i, store := range cc.stores {
		if store == nil {
			continue
		}
		v1, ok1 := store.GetLocal("before-partition")
		v2, ok2 := store.GetLocal("during-partition")

		assert.True(t, ok1, "node %d missing before-partition", i)
		assert.True(t, ok2, "node %d missing during-partition", i)
		assert.Equal(t, "value", v1)
		assert.Equal(t, "value", v2)
	}
}

func TestChaos_MajorityPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cc := newChaosCluster(t, 5)
	defer cc.cleanup()

	leader, _ := cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Write initial data
	err := leader.Put("before-majority-loss", "value")
	require.NoError(t, err)

	// Kill majority (3 nodes)
	cc.stopNode(2)
	cc.stopNode(3)
	cc.stopNode(4)
	t.Log("Killed nodes 2, 3, 4 (majority)")

	// Cluster should NOT be able to elect leader or write
	time.Sleep(1 * time.Second)
	leader, _ = cc.waitForLeader(2 * time.Second)
	if leader != nil {
		// If there's a "leader", writes should fail
		err := leader.Put("should-fail", "value")
		// This might timeout or fail - that's expected
		t.Logf("Write during majority loss: %v (expected to fail or timeout)", err)
	}

	// Bring majority back
	cc.startNode(2)
	cc.startNode(3)
	cc.startNode(4)
	t.Log("Restarted nodes 2, 3, 4")

	// Now the cluster should work
	time.Sleep(2 * time.Second)
	leader, _ = cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Original data should still be there
	value, ok, err := leader.Get("before-majority-loss")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "value", value)
}

type stressCounters struct {
	writeSuccess, writeFail, readSuccess, readFail atomic.Int64
}

func stressWriter(cc *chaosCluster, id int, stopCh <-chan struct{}, written *sync.Map, counters *stressCounters) {
	for i := 0; ; i++ {
		select {
		case <-stopCh:
			return
		default:
		}

		store, _ := cc.getLeader()
		if store == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		key := fmt.Sprintf("w%d-k%d", id, i)
		value := fmt.Sprintf("v%d", i)

		if store.Put(key, value) == nil {
			counters.writeSuccess.Add(1)
			written.Store(key, value)
		} else {
			counters.writeFail.Add(1)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func stressReader(cc *chaosCluster, stopCh <-chan struct{}, written *sync.Map, counters *stressCounters) {
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		store := cc.getAnyStore()
		if store == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}

		var key string
		written.Range(func(k, v interface{}) bool {
			key = k.(string)
			return false
		})

		if key != "" {
			_, ok := store.GetLocal(key)
			if ok {
				counters.readSuccess.Add(1)
			} else {
				counters.readFail.Add(1)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func stressChaos(cc *chaosCluster, stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		if cc.countAlive() <= 3 {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		victim := cryptoRandIntn(5)
		cc.stopNode(victim)
		time.Sleep(time.Duration(200+cryptoRandIntn(500)) * time.Millisecond)
		cc.startNode(victim)
		time.Sleep(time.Duration(500+cryptoRandIntn(500)) * time.Millisecond)
	}
}

func TestChaos_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cc := newChaosCluster(t, 5)
	defer cc.cleanup()

	leader, _ := cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	counters := &stressCounters{}
	written := &sync.Map{}

	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(id int) { defer wg.Done(); stressWriter(cc, id, stopCh, written, counters) }(w)
	}

	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() { defer wg.Done(); stressReader(cc, stopCh, written, counters) }()
	}

	wg.Add(1)
	go func() { defer wg.Done(); stressChaos(cc, stopCh) }()

	time.Sleep(30 * time.Second)
	close(stopCh)
	wg.Wait()

	t.Logf("Writes: %d success, %d fail", counters.writeSuccess.Load(), counters.writeFail.Load())
	t.Logf("Reads: %d success, %d fail", counters.readSuccess.Load(), counters.readFail.Load())

	time.Sleep(3 * time.Second)
	leader, _ = cc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	var verified, missing int
	written.Range(func(k, v interface{}) bool {
		key := k.(string)
		expected := v.(string)

		value, ok, _ := leader.Get(key)
		switch {
		case !ok:
			missing++
		case value != expected:
			t.Errorf("corruption: %s expected %s got %s", key, expected, value)
		default:
			verified++
		}
		return true
	})

	t.Logf("Final: %d verified, %d missing", verified, missing)
	assert.Equal(t, 0, missing, "data loss detected")
}

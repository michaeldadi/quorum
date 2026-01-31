package kv

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"quorum/internal/raft"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testKVCluster struct {
	t        *testing.T
	nodes    []*raft.Node
	servers  []*raft.RPCServer
	stores   []*Store
	applyChs []chan raft.ApplyMsg
	dataDir  string
}

func newTestKVCluster(t *testing.T, n int) *testKVCluster {
	dataDir, err := os.MkdirTemp("", "kv-test-*")
	require.NoError(t, err)

	tc := &testKVCluster{
		t:        t,
		nodes:    make([]*raft.Node, n),
		servers:  make([]*raft.RPCServer, n),
		stores:   make([]*Store, n),
		applyChs: make([]chan raft.ApplyMsg, n),
		dataDir:  dataDir,
	}

	// First pass: create servers with port 0 to get OS-assigned ports
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		tc.applyChs[i] = make(chan raft.ApplyMsg, 100)
		tc.nodes[i] = raft.NewNode(fmt.Sprintf("node-%d", i), nil, tc.applyChs[i], nil)

		tc.servers[i], err = raft.NewRPCServer(tc.nodes[i], 0)
		require.NoError(t, err)

		ports[i] = tc.servers[i].Addr().(*net.TCPAddr).Port
	}

	// Second pass: rebuild nodes with correct peer lists and persisters
	for i := 0; i < n; i++ {
		var peers []string
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, fmt.Sprintf("localhost:%d", ports[j]))
			}
		}

		nodeID := fmt.Sprintf("node-%d", i)
		nodeDir := fmt.Sprintf("%s/%s", dataDir, nodeID)

		persister, err := raft.NewPersister(nodeDir)
		require.NoError(t, err)

		node := raft.NewNode(nodeID, peers, tc.applyChs[i], persister)
		tc.servers[i].SetNode(node)
		tc.nodes[i] = node
	}

	for i, node := range tc.nodes {
		node.Start()
		tc.stores[i] = NewStore(node, tc.applyChs[i])
	}

	return tc
}

func (tc *testKVCluster) cleanup() {
	for _, node := range tc.nodes {
		if node != nil {
			node.Stop()
		}
	}
	for _, server := range tc.servers {
		if server != nil {
			err := server.Close()
			if err != nil {
				return
			}
		}
	}
	err := os.RemoveAll(tc.dataDir)
	if err != nil {
		return
	}
}

func (tc *testKVCluster) waitForLeader(timeout time.Duration) (leader *Store, idx int) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, store := range tc.stores {
			if store != nil && store.IsLeader() {
				return store, i
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, -1
}

func (tc *testKVCluster) stopNode(i int) {
	if tc.nodes[i] != nil {
		tc.nodes[i].Stop()
		tc.nodes[i] = nil
	}
	if tc.servers[i] != nil {
		err := tc.servers[i].Close()
		if err != nil {
			return
		}
		tc.servers[i] = nil
	}
	tc.stores[i] = nil
}

// --- KV Tests ---

func TestKVPutGet(t *testing.T) {
	tc := newTestKVCluster(t, 3)
	defer tc.cleanup()

	leader, _ := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Put
	err := leader.Put("foo", "bar")
	require.NoError(t, err)

	// Get
	value, ok, err := leader.Get("foo")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "bar", value)
}

func TestKVDelete(t *testing.T) {
	tc := newTestKVCluster(t, 3)
	defer tc.cleanup()

	leader, _ := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Put then delete
	err := leader.Put("foo", "bar")
	require.NoError(t, err)

	err = leader.Delete("foo")
	require.NoError(t, err)

	// Should be gone
	_, ok, err := leader.Get("foo")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestKVReplication(t *testing.T) {
	tc := newTestKVCluster(t, 3)
	defer tc.cleanup()

	leader, leaderIdx := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Put via leader
	err := leader.Put("replicated", "value")
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(300 * time.Millisecond)

	// All nodes should have it via local read
	for i, store := range tc.stores {
		if i == leaderIdx {
			continue
		}
		value, ok := store.GetLocal("replicated")
		assert.True(t, ok, "node %d missing key", i)
		assert.Equal(t, "value", value)
	}
}

func TestKVPutToFollowerFails(t *testing.T) {
	tc := newTestKVCluster(t, 3)
	defer tc.cleanup()

	_, leaderIdx := tc.waitForLeader(5 * time.Second)

	// Find follower
	followerIdx := (leaderIdx + 1) % 3
	follower := tc.stores[followerIdx]

	err := follower.Put("foo", "bar")
	assert.Equal(t, ErrNotLeader, err)
}

func TestKVSurvivesLeaderFailure(t *testing.T) {
	tc := newTestKVCluster(t, 3)
	defer tc.cleanup()

	leader, leaderIdx := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Write some data
	err := leader.Put("survive", "crash")
	require.NoError(t, err)

	// Wait for replication
	time.Sleep(300 * time.Millisecond)

	// Kill leader
	tc.stopNode(leaderIdx)

	// Wait for a new leader
	newLeader, _ := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, newLeader)

	// Data should still be there
	value, ok, err := newLeader.Get("survive")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "crash", value)
}

func TestKVLinearizableRead(t *testing.T) {
	tc := newTestKVCluster(t, 3)
	defer tc.cleanup()

	leader, _ := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Write
	err := leader.Put("linear", "read")
	require.NoError(t, err)

	// Linearizable read should work
	value, ok, err := leader.Get("linear")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "read", value)
}

func TestKVMultipleWrites(t *testing.T) {
	tc := newTestKVCluster(t, 3)
	defer tc.cleanup()

	leader, _ := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Multiple writes
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		err := leader.Put(key, value)
		require.NoError(t, err)
	}

	// Verify all
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		expected := fmt.Sprintf("value-%d", i)
		value, ok, err := leader.Get(key)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, expected, value)
	}
}

// internal/raft/raft_test.go
package raft

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper to create a cluster
type testCluster struct {
	t        *testing.T
	nodes    []*Node
	servers  []*RPCServer
	applyChs []chan ApplyMsg
	dataDir  string
}

func newTestCluster(t *testing.T, n int) *testCluster {
	dataDir, err := os.MkdirTemp("", "raft-test-*")
	require.NoError(t, err)

	tc := &testCluster{
		t:        t,
		nodes:    make([]*Node, n),
		servers:  make([]*RPCServer, n),
		applyChs: make([]chan ApplyMsg, n),
		dataDir:  dataDir,
	}

	// First pass: create servers with port 0 to get OS-assigned ports
	// We need a temporary node just to create the server, so create placeholder nodes first
	ports := make([]int, n)
	for i := 0; i < n; i++ {
		tc.applyChs[i] = make(chan ApplyMsg, 100)
		// Create a temporary node with no peers; we'll replace it after we know all ports
		tc.nodes[i] = NewNode(fmt.Sprintf("node-%d", i), nil, tc.applyChs[i], nil)

		tc.servers[i], err = NewRPCServer(tc.nodes[i], 0)
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

		persister, err := NewPersister(nodeDir)
		require.NoError(t, err)

		node := NewNode(nodeID, peers, tc.applyChs[i], persister)
		// Point the existing server at the new node
		tc.servers[i].SetNode(node)
		tc.nodes[i] = node
	}

	// Start all nodes
	for _, node := range tc.nodes {
		node.Start()
	}

	return tc
}

func (tc *testCluster) cleanup() {
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

func (tc *testCluster) waitForLeader(timeout time.Duration) (leader *Node, idx int) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for i, node := range tc.nodes {
			if node != nil {
				_, state := node.GetState()
				if state == Leader {
					return node, i
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, -1
}

func (tc *testCluster) getLeaderCount() int {
	count := 0
	for _, node := range tc.nodes {
		if node != nil {
			_, state := node.GetState()
			if state == Leader {
				count++
			}
		}
	}
	return count
}

func (tc *testCluster) stopNode(i int) {
	if tc.nodes[i] != nil {
		tc.nodes[i].Stop()
	}
	if tc.servers[i] != nil {
		err := tc.servers[i].Close()
		if err != nil {
			return
		}
	}
}

// --- Unit Tests ---

func TestNodeStartsAsFollower(t *testing.T) {
	tc := newTestCluster(t, 1)
	defer tc.cleanup()

	_, state := tc.nodes[0].GetState()
	assert.Equal(t, Follower, state)
}

func TestSingleNodeBecomesLeader(t *testing.T) {
	applyCh := make(chan ApplyMsg, 100)
	node := NewNode("single", []string{}, applyCh, nil)
	node.Start()
	defer node.Stop()

	// A single node should elect itself
	time.Sleep(500 * time.Millisecond)

	_, state := node.GetState()
	assert.Equal(t, Leader, state)
}

func TestLeaderElection(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.cleanup()

	// Wait for the leader
	leader, _ := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "no leader elected")

	// Should have exactly one leader
	assert.Equal(t, 1, tc.getLeaderCount())
}

func TestLeaderElectionAfterFailure(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.cleanup()

	// Wait for the initial leader
	leader, leaderIdx := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader, "no leader elected")

	oldTerm, _ := leader.GetState()

	// Kill the leader
	tc.stopNode(leaderIdx)
	tc.nodes[leaderIdx] = nil

	// Wait for a new leader
	time.Sleep(500 * time.Millisecond)
	newLeader, newLeaderIdx := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, newLeader, "no new leader elected")
	assert.NotEqual(t, leaderIdx, newLeaderIdx)

	newTerm, _ := newLeader.GetState()
	assert.Greater(t, newTerm, oldTerm)
}

func TestLogReplication(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.cleanup()

	// Wait for the leader
	leader, _ := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Submit a command
	index, term, isLeader := leader.Submit("command-1")
	assert.True(t, isLeader)
	assert.Equal(t, 1, index)
	assert.Greater(t, term, 0)

	// Wait for replication
	time.Sleep(200 * time.Millisecond)

	// All nodes should have received the "apply" message
	for i, applyCh := range tc.applyChs {
		select {
		case msg := <-applyCh:
			assert.True(t, msg.CommandValid)
			assert.Equal(t, "command-1", msg.Command)
			assert.Equal(t, 1, msg.CommandIndex)
		case <-time.After(1 * time.Second):
			t.Errorf("node %d did not apply command", i)
		}
	}
}

func TestSubmitToFollowerFails(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.cleanup()

	// Wait for the leader
	leader, leaderIdx := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	// Find a follower
	followerIdx := (leaderIdx + 1) % 3
	follower := tc.nodes[followerIdx]

	// Submit to a follower should fail
	_, _, isLeader := follower.Submit("command-1")
	assert.False(t, isLeader)
}

func TestTermIncrementsOnElection(t *testing.T) {
	tc := newTestCluster(t, 3)
	defer tc.cleanup()

	leader, leaderIdx := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, leader)

	term1, _ := leader.GetState()

	// Kill leader
	tc.stopNode(leaderIdx)
	tc.nodes[leaderIdx] = nil

	// Wait for a new election
	newLeader, _ := tc.waitForLeader(5 * time.Second)
	require.NotNil(t, newLeader)

	term2, _ := newLeader.GetState()
	assert.Greater(t, term2, term1)
}

func TestNoSplitBrain(t *testing.T) {
	tc := newTestCluster(t, 5)
	defer tc.cleanup()

	// Wait for a stable state
	_, _ = tc.waitForLeader(5 * time.Second)
	time.Sleep(1 * time.Second)

	// Check multiple times
	for i := 0; i < 10; i++ {
		leaderCount := tc.getLeaderCount()
		assert.LessOrEqual(t, leaderCount, 1, "split brain detected: %d leaders", leaderCount)
		time.Sleep(100 * time.Millisecond)
	}
}

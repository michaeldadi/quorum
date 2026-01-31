// Package raft raft/node.go
package raft

import (
	"crypto/rand"
	"math/big"
	"quorum/pkg/logger"
	"sync"
	"time"
)

const (
	minElectionTimeout = 150 * time.Millisecond
	maxElectionTimeout = 300 * time.Millisecond
)

type Node struct {
	mu sync.Mutex

	// Persistent state
	id          string
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state
	state       NodeState
	commitIndex int
	lastApplied int

	// Leader state
	nextIndex  map[string]int
	matchIndex map[string]int

	// Cluster
	peers []string

	// Channels
	resetElection chan struct{}
	stopCh        chan struct{}
}

func NewNode(id string, peers []string) *Node {
	return &Node{
		id:            id,
		currentTerm:   0,
		votedFor:      "",
		log:           make([]LogEntry, 0),
		state:         Follower,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make(map[string]int),
		matchIndex:    make(map[string]int),
		peers:         peers,
		resetElection: make(chan struct{}),
		stopCh:        make(chan struct{}),
	}
}

func (n *Node) Start() {
	logger.Info("node starting", "id", n.id, "peers", n.peers)
	go n.electionLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) electionLoop() {
	for {
		timeout := randomElectionTimeout()

		select {
		case <-time.After(timeout):
			n.mu.Lock()
			state := n.state
			n.mu.Unlock()

			if state != Leader {
				n.startElection()
			}

		case <-n.resetElection:
			// Reset timer, continue loop

		case <-n.stopCh:
			logger.Info("election loop stopping")
			return
		}
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	logger.Info("became leader", "term", n.currentTerm)

	// Initialize leader state
	for _, peer := range n.peers {
		n.nextIndex[peer] = len(n.log) + 1
		n.matchIndex[peer] = 0
	}

	// TODO: start sending heartbeats
}

func (n *Node) becomeFollower(term int) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	logger.Info("became follower", "term", n.currentTerm)
}

// ResetElectionTimer Call this when receiving a valid heartbeat or granting a vote
func (n *Node) ResetElectionTimer() {
	select {
	case n.resetElection <- struct{}{}:
	default:
		// Channel is not ready, no action needed
	}
}

func (n *Node) GetState() (int, NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.state
}

func randomElectionTimeout() time.Duration {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(maxElectionTimeout-minElectionTimeout)))
	return minElectionTimeout + time.Duration(n.Int64())
}

func (n *Node) lastLogInfo() (index, term int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

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
	applyCh       chan ApplyMsg

	// Persistence
	persister *Persister
}

func NewNode(id string, peers []string, applyCh chan ApplyMsg, persister *Persister) *Node {
	n := &Node{
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
		applyCh:       applyCh,
		persister:     persister,
	}

	// Load persisted state if any
	if persister != nil {
		state, err := persister.Load()
		if err != nil {
			logger.Error("failed to load persisted state", "err", err)
		} else {
			n.currentTerm = state.CurrentTerm
			n.votedFor = state.VotedFor
			n.log = state.Log
			n.commitIndex = len(n.log)
		}
	}

	return n
}

func (n *Node) Start() {
	logger.Info("node starting",
		"id", n.id,
		"peers", n.peers,
		"term", n.currentTerm,
		"logLen", len(n.log))

	go n.electionLoop()
	go n.applyLoop()
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) persist() {
	if n.persister == nil {
		return
	}

	state := PersistedState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         n.log,
	}

	if err := n.persister.Save(state); err != nil {
		logger.Error("failed to persist state", "err", err)
	}
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

		case <-n.stopCh:
			logger.Info("election loop stopping")
			return
		}
	}
}

func (n *Node) applyLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.mu.Lock()
		for n.commitIndex > n.lastApplied {
			n.lastApplied++
			entry := n.log[n.lastApplied-1]

			msg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}

			n.mu.Unlock()
			n.applyCh <- msg
			n.mu.Lock()

			logger.Debug("applied entry", "index", entry.Index, "term", entry.Term)
		}
		n.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) Submit(command interface{}) (index, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return -1, -1, false
	}

	index = len(n.log) + 1
	term = n.currentTerm

	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}

	n.log = append(n.log, entry)
	n.matchIndex[n.id] = index
	n.persist()

	logger.Info("leader accepted command", "index", index, "term", term)

	return index, term, true
}

func (n *Node) becomeLeader() {
	n.state = Leader
	logger.Info("became leader", "term", n.currentTerm)

	for _, peer := range n.peers {
		n.nextIndex[peer] = len(n.log) + 1
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = len(n.log)

	go n.heartbeatLoop()
}

func (n *Node) becomeFollower(term int) {
	n.state = Follower
	n.currentTerm = term
	n.votedFor = ""
	n.persist()
	logger.Info("became follower", "term", n.currentTerm)
}

func (n *Node) ResetElectionTimer() {
	select {
	case n.resetElection <- struct{}{}:
	default:
	}
}

func (n *Node) GetState() (int, NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm, n.state
}

func (n *Node) lastLogInfo() (index, term int) {
	if len(n.log) == 0 {
		return 0, 0
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

func randomElectionTimeout() time.Duration {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(maxElectionTimeout-minElectionTimeout)))
	return minElectionTimeout + time.Duration(n.Int64())
}

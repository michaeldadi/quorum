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

	// Snapshot state
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

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
		id:                id,
		currentTerm:       0,
		votedFor:          "",
		log:               make([]LogEntry, 0),
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		state:             Follower,
		commitIndex:       0,
		lastApplied:       0,
		nextIndex:         make(map[string]int),
		matchIndex:        make(map[string]int),
		peers:             peers,
		resetElection:     make(chan struct{}),
		stopCh:            make(chan struct{}),
		applyCh:           applyCh,
		persister:         persister,
	}

	if persister != nil {
		state, err := persister.Load()
		if err != nil {
			logger.Error("failed to load persisted state", "err", err)
		} else {
			n.currentTerm = state.CurrentTerm
			n.votedFor = state.VotedFor
			n.log = state.Log
			n.lastIncludedIndex = state.LastIncludedIndex
			n.lastIncludedTerm = state.LastIncludedTerm
		}

		snapshot, err := persister.LoadSnapshot()
		if err != nil {
			logger.Error("failed to load snapshot", "err", err)
		} else {
			n.snapshot = snapshot
		}
	}

	return n
}

func (n *Node) Start() {
	logger.Info("node starting",
		"id", n.id,
		"peers", n.peers,
		"term", n.currentTerm,
		"logLen", len(n.log),
		"snapshotIndex", n.lastIncludedIndex)

	// Send snapshot to state machine first if we have one
	if len(n.snapshot) > 0 {
		n.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      n.snapshot,
			SnapshotTerm:  n.lastIncludedTerm,
			SnapshotIndex: n.lastIncludedIndex,
		}
		n.lastApplied = n.lastIncludedIndex
		n.commitIndex = n.lastIncludedIndex
	}

	// Replay any log entries beyond the snapshot
	if len(n.log) > 0 {
		n.commitIndex = n.lastIncludedIndex + len(n.log)
	}

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
		CurrentTerm:       n.currentTerm,
		VotedFor:          n.votedFor,
		Log:               n.log,
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
	}

	if err := n.persister.Save(state); err != nil {
		logger.Error("failed to persist state", "err", err)
	}
}

// Snapshot is called by the state machine when it wants to compact state
func (n *Node) Snapshot(index int, data []byte) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if index <= n.lastIncludedIndex {
		return // already snapshotted past this point
	}

	// Find the log entry at index
	logIndex := index - n.lastIncludedIndex - 1
	if logIndex < 0 || logIndex >= len(n.log) {
		logger.Error("snapshot index out of range", "index", index)
		return
	}

	n.lastIncludedTerm = n.log[logIndex].Term
	n.lastIncludedIndex = index
	n.snapshot = data

	// Discard log entries up to and including index
	n.log = n.log[logIndex+1:]

	// Persist both state and snapshot
	n.persist()
	if n.persister != nil {
		if err := n.persister.SaveSnapshot(data); err != nil {
			logger.Error("failed to save snapshot", "err", err)
		}
	}

	logger.Info("created snapshot",
		"index", index,
		"term", n.lastIncludedTerm,
		"remainingLog", len(n.log))
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

			// Convert to log array index
			logIndex := n.lastApplied - n.lastIncludedIndex - 1
			if logIndex < 0 || logIndex >= len(n.log) {
				n.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				n.mu.Lock()
				continue
			}

			entry := n.log[logIndex]

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

	index = n.lastIncludedIndex + len(n.log) + 1
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

	lastLogIndex := n.lastIncludedIndex + len(n.log)
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastLogIndex + 1
		n.matchIndex[peer] = 0
	}
	n.matchIndex[n.id] = lastLogIndex

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
		return n.lastIncludedIndex, n.lastIncludedTerm
	}
	last := n.log[len(n.log)-1]
	return last.Index, last.Term
}

// Get term of log entry at the given index
func (n *Node) logTerm(index int) int {
	if index == n.lastIncludedIndex {
		return n.lastIncludedTerm
	}
	logIndex := index - n.lastIncludedIndex - 1
	if logIndex < 0 || logIndex >= len(n.log) {
		return -1
	}
	return n.log[logIndex].Term
}

func randomElectionTimeout() time.Duration {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(maxElectionTimeout-minElectionTimeout)))
	return minElectionTimeout + time.Duration(n.Int64())
}

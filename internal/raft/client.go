// Package raft raft/client.go
package raft

import (
	"net/rpc"
	"quorum/pkg/logger"
	"sort"
	"sync"
	"time"
)

func (n *Node) sendRequestVote(peer string, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	client, err := rpc.Dial("tcp", peer)
	if err != nil {
		logger.Debug("failed to connect", "peer", peer, "err", err)
		return nil, false
	}
	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			logger.Debug("failed to close client", "peer", peer, "err", err)
		}
	}(client)

	reply := &RequestVoteReply{}
	call := client.Go("RPCServer.RequestVote", args, reply, nil)

	select {
	case <-call.Done:
		if call.Error != nil {
			logger.Debug("RequestVote failed", "peer", peer, "err", call.Error)
			return nil, false
		}
		return reply, true
	case <-time.After(100 * time.Millisecond):
		logger.Debug("RequestVote timeout", "peer", peer)
		return nil, false
	}
}

func (n *Node) sendAppendEntries(peer string, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	client, err := rpc.Dial("tcp", peer)
	if err != nil {
		return nil, false
	}
	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			logger.Debug("failed to close client", "peer", peer, "err", err)
		}
	}(client)

	reply := &AppendEntriesReply{}
	call := client.Go("RPCServer.AppendEntries", args, reply, nil)

	select {
	case <-call.Done:
		if call.Error != nil {
			return nil, false
		}
		return reply, true
	case <-time.After(100 * time.Millisecond):
		return nil, false
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	currentTerm := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	n.mu.Unlock()

	logger.Info("starting election", "term", currentTerm)

	votes := 1
	var voteMu sync.Mutex
	done := make(chan struct{})

	args := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peer := range n.peers {
		go func(peer string) {
			reply, ok := n.sendRequestVote(peer, args)
			if !ok {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if n.state != Candidate || n.currentTerm != currentTerm {
				return
			}

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
				return
			}

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				if votes > (len(n.peers)+1)/2 {
					select {
					case done <- struct{}{}:
					default:
					}
				}
				voteMu.Unlock()
			}
		}(peer)
	}

	select {
	case <-done:
		n.mu.Lock()
		if n.state == Candidate && n.currentTerm == currentTerm {
			n.becomeLeader()
		}
		n.mu.Unlock()
	case <-time.After(100 * time.Millisecond):
	}
}

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mu.Lock()
			if n.state != Leader {
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()

			n.replicateToAll()

		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) replicateToAll() {
	n.mu.Lock()
	peers := n.peers
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateTo(peer)
	}
}

func (n *Node) replicateTo(peer string) {
	n.mu.Lock()

	if n.state != Leader {
		n.mu.Unlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	prevLogIndex := nextIdx - 1
	prevLogTerm := 0

	if prevLogIndex > 0 && prevLogIndex <= len(n.log) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	// Get entries to send
	var entries []LogEntry
	if nextIdx <= len(n.log) {
		entries = make([]LogEntry, len(n.log)-nextIdx+1)
		copy(entries, n.log[nextIdx-1:])
	}

	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderId:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	currentTerm := n.currentTerm

	n.mu.Unlock()

	reply, ok := n.sendAppendEntries(peer, args)
	if !ok {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader || n.currentTerm != currentTerm {
		return
	}

	if reply.Term > n.currentTerm {
		n.becomeFollower(reply.Term)
		return
	}

	if reply.Success {
		// Update nextIndex and matchIndex
		n.nextIndex[peer] = nextIdx + len(entries)
		n.matchIndex[peer] = n.nextIndex[peer] - 1

		n.updateCommitIndex()
	} else if n.nextIndex[peer] > 1 {
		// Decrement nextIndex and retry
		n.nextIndex[peer]--
	}
}

func (n *Node) updateCommitIndex() {
	// Find the highest index replicated on a majority
	matches := make([]int, 0, len(n.peers)+1)
	matches = append(matches, len(n.log)) // leader's own match

	for _, peer := range n.peers {
		matches = append(matches, n.matchIndex[peer])
	}

	sort.Sort(sort.Reverse(sort.IntSlice(matches)))

	// Majority index
	majorityIdx := matches[(len(matches)-1)/2]

	// Only commit if it's from current term (Figure 8 safety)
	if majorityIdx > n.commitIndex && majorityIdx <= len(n.log) {
		if n.log[majorityIdx-1].Term == n.currentTerm {
			logger.Info("updating commit index", "from", n.commitIndex, "to", majorityIdx)
			n.commitIndex = majorityIdx
		}
	}
}

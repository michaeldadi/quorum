// Package raft raft/client.go
package raft

import (
	"net/rpc"
	"quorum/pkg/logger"
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

	votes := 1 // vote for self
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

			// Check if we're still a candidate in the same term
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

	// Wait for majority or timeout
	select {
	case <-done:
		n.mu.Lock()
		if n.state == Candidate && n.currentTerm == currentTerm {
			n.becomeLeader()
			go n.heartbeatLoop()
		}
		n.mu.Unlock()
	case <-time.After(100 * time.Millisecond):
		// Election timeout, will retry in electionLoop
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
			currentTerm := n.currentTerm
			n.mu.Unlock()

			n.sendHeartbeats(currentTerm)

		case <-n.stopCh:
			return
		}
	}
}

func (n *Node) sendHeartbeats(term int) {
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderId:     n.id,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: n.commitIndex,
	}

	for _, peer := range n.peers {
		go func(peer string) {
			reply, ok := n.sendAppendEntries(peer, args)
			if !ok {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.becomeFollower(reply.Term)
			}
		}(peer)
	}
}

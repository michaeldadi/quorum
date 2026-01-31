// Package raft raft/server.go
package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"quorum/pkg/logger"
)

type RPCServer struct {
	node     *Node
	listener net.Listener
}

func NewRPCServer(node *Node, port int) (*RPCServer, error) {
	server := &RPCServer{node: node}

	err := rpc.Register(server)
	if err != nil {
		return nil, err
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}
	server.listener = listener

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				select {
				case <-node.stopCh:
					return
				default:
					logger.Error("accept error", "err", err)
					continue
				}
			}
			go rpc.ServeConn(conn)
		}
	}()

	logger.Info("RPC server listening", "port", port)
	return server, nil
}

func (s *RPCServer) Close() error {
	return s.listener.Close()
}

func (s *RPCServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	logger.Debug("received RequestVote",
		"from", args.CandidateId,
		"term", args.Term,
		"myTerm", s.node.currentTerm)

	reply.Term = s.node.currentTerm
	reply.VoteGranted = false

	if args.Term < s.node.currentTerm {
		return nil
	}

	if args.Term > s.node.currentTerm {
		s.node.becomeFollower(args.Term)
	}

	logOk := s.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)

	if (s.node.votedFor == "" || s.node.votedFor == args.CandidateId) && logOk {
		s.node.votedFor = args.CandidateId
		reply.VoteGranted = true
		s.node.persist()
		s.node.ResetElectionTimer()
		logger.Info("granted vote", "to", args.CandidateId, "term", args.Term)
	}

	return nil
}

func (s *RPCServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	reply.Term = s.node.currentTerm
	reply.Success = false

	if args.Term < s.node.currentTerm {
		return nil
	}

	s.node.ResetElectionTimer()

	if args.Term > s.node.currentTerm {
		s.node.becomeFollower(args.Term)
	} else if s.node.state == Candidate {
		s.node.becomeFollower(args.Term)
	}

	// Log consistency check
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex < s.node.lastIncludedIndex {
			// Leader is behind our snapshot, this shouldn't happen normally
			reply.Success = true
			return nil
		}

		prevLogTerm := s.node.logTerm(args.PrevLogIndex)
		if prevLogTerm == -1 || prevLogTerm != args.PrevLogTerm {
			return nil
		}
	}

	// Append entries
	modified := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		logIndex := idx - s.node.lastIncludedIndex - 1

		if logIndex < 0 {
			continue // already in snapshot
		}

		if logIndex < len(s.node.log) {
			if s.node.log[logIndex].Term != entry.Term {
				s.node.log = s.node.log[:logIndex]
				s.node.log = append(s.node.log, entry)
				modified = true
			}
		} else {
			s.node.log = append(s.node.log, entry)
			modified = true
		}
	}

	if modified {
		s.node.persist()
	}

	if args.LeaderCommit > s.node.commitIndex {
		lastNewIndex := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewIndex {
			s.node.commitIndex = args.LeaderCommit
		} else {
			s.node.commitIndex = lastNewIndex
		}
		logger.Debug("updated commit index", "commitIndex", s.node.commitIndex)
	}

	reply.Success = true
	return nil
}

func (s *RPCServer) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	reply.Term = s.node.currentTerm

	if args.Term < s.node.currentTerm {
		return nil
	}

	s.node.ResetElectionTimer()

	if args.Term > s.node.currentTerm {
		s.node.becomeFollower(args.Term)
	}

	if args.LastIncludedIndex <= s.node.lastIncludedIndex {
		return nil // we already have a newer snapshot
	}

	logger.Info("installing snapshot",
		"index", args.LastIncludedIndex,
		"term", args.LastIncludedTerm)

	// Discard entire log if snapshot is ahead of our log
	lastLogIndex := s.node.lastIncludedIndex + len(s.node.log)
	if args.LastIncludedIndex >= lastLogIndex {
		s.node.log = make([]LogEntry, 0)
	} else {
		// Keep log entries after snapshot
		logIndex := args.LastIncludedIndex - s.node.lastIncludedIndex
		s.node.log = s.node.log[logIndex:]
	}

	s.node.lastIncludedIndex = args.LastIncludedIndex
	s.node.lastIncludedTerm = args.LastIncludedTerm
	s.node.snapshot = args.Data

	s.node.persist()
	if s.node.persister != nil {
		err := s.node.persister.SaveSnapshot(args.Data)
		if err != nil {
			return err
		}
	}

	// Apply snapshot to state machine
	if args.LastIncludedIndex > s.node.lastApplied {
		s.node.mu.Unlock()
		s.node.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		s.node.mu.Lock()
		s.node.lastApplied = args.LastIncludedIndex
		s.node.commitIndex = args.LastIncludedIndex
	}

	return nil
}

func (s *RPCServer) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastIndex, myLastTerm := s.node.lastLogInfo()

	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

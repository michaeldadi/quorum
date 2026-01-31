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
		s.node.persist() // <-- add this
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

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > len(s.node.log) {
			return nil
		}
		if s.node.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			s.node.log = s.node.log[:args.PrevLogIndex-1]
			s.node.persist() // <-- add this
			return nil
		}
	}

	modified := false
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		if idx <= len(s.node.log) {
			if s.node.log[idx-1].Term != entry.Term {
				s.node.log = s.node.log[:idx-1]
				s.node.log = append(s.node.log, entry)
				modified = true
			}
		} else {
			s.node.log = append(s.node.log, entry)
			modified = true
		}
	}

	if modified {
		s.node.persist() // <-- add this
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

func (s *RPCServer) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastIndex, myLastTerm := s.node.lastLogInfo()

	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

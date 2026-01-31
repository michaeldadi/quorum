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

// RequestVote RPC handler
func (s *RPCServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	logger.Debug("received RequestVote",
		"from", args.CandidateId,
		"term", args.Term,
		"myTerm", s.node.currentTerm)

	reply.Term = s.node.currentTerm
	reply.VoteGranted = false

	// Reply false if term < currentTerm (§5.1)
	if args.Term < s.node.currentTerm {
		return nil
	}

	// If RPC request contains term > currentTerm, convert to follower (§5.1)
	if args.Term > s.node.currentTerm {
		s.node.becomeFollower(args.Term)
	}

	// If votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote (§5.2, §5.4)
	logOk := s.isLogUpToDate(args.LastLogIndex, args.LastLogTerm)

	if (s.node.votedFor == "" || s.node.votedFor == args.CandidateId) && logOk {
		s.node.votedFor = args.CandidateId
		reply.VoteGranted = true
		s.node.ResetElectionTimer()
		logger.Info("granted vote", "to", args.CandidateId, "term", args.Term)
	}

	return nil
}

// AppendEntries RPC handler (heartbeats for now)
func (s *RPCServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	s.node.mu.Lock()
	defer s.node.mu.Unlock()

	reply.Term = s.node.currentTerm
	reply.Success = false

	// Reply false if term < currentTerm (§5.1)
	if args.Term < s.node.currentTerm {
		return nil
	}

	// Valid leader, reset election timer
	s.node.ResetElectionTimer()

	// If RPC request contains term > currentTerm, convert to follower
	if args.Term > s.node.currentTerm {
		s.node.becomeFollower(args.Term)
	} else if s.node.state == Candidate {
		// Candidate discovers current leader
		s.node.becomeFollower(args.Term)
	}

	reply.Success = true
	return nil
}

func (s *RPCServer) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	myLastIndex, myLastTerm := s.node.lastLogInfo()

	// §5.4.1: Compare terms first, then index
	if lastLogTerm != myLastTerm {
		return lastLogTerm > myLastTerm
	}
	return lastLogIndex >= myLastIndex
}

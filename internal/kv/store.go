// Package kv internal/kv/store.go
package kv

import (
	"encoding/json"
	"quorum/internal/raft"
	"quorum/pkg/logger"
	"sync"
)

const snapshotThreshold = 100 // snapshot every 100 entries

type OpType string

const (
	OpPut    OpType = "put"
	OpGet    OpType = "get"
	OpDelete OpType = "delete"
)

type Command struct {
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type Store struct {
	mu   sync.RWMutex
	data map[string]string
	node *raft.Node

	pending      map[int]chan Result
	pendingMu    sync.Mutex
	lastApplied  int
	appliedSince int // entries applied since last snapshot
}

type Result struct {
	Value string
	Ok    bool
	Err   string
}

func NewStore(node *raft.Node, applyCh chan raft.ApplyMsg) *Store {
	s := &Store{
		data:    make(map[string]string),
		node:    node,
		pending: make(map[int]chan Result),
	}

	go s.applyLoop(applyCh)

	return s
}

func (s *Store) applyLoop(applyCh chan raft.ApplyMsg) {
	for msg := range applyCh {
		if msg.SnapshotValid {
			s.applySnapshot(msg.Snapshot)
			s.lastApplied = msg.SnapshotIndex
			s.appliedSince = 0
			continue
		}

		if !msg.CommandValid {
			continue
		}

		var cmd Command
		switch c := msg.Command.(type) {
		case string:
			if err := json.Unmarshal([]byte(c), &cmd); err != nil {
				logger.Error("failed to unmarshal command", "err", err)
				continue
			}
		case Command:
			cmd = c
		default:
			logger.Error("unknown command type", "type", msg.Command)
			continue
		}

		result := s.apply(cmd)
		s.lastApplied = msg.CommandIndex
		s.appliedSince++

		logger.Info("applied command",
			"index", msg.CommandIndex,
			"op", cmd.Op,
			"key", cmd.Key)

		s.pendingMu.Lock()
		if ch, ok := s.pending[msg.CommandIndex]; ok {
			ch <- result
			delete(s.pending, msg.CommandIndex)
		}
		s.pendingMu.Unlock()

		// Check if we should be taking a snapshot
		if s.appliedSince >= snapshotThreshold {
			s.maybeSnapshot()
		}
	}
}

func (s *Store) apply(cmd Command) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		return Result{Ok: true}

	case OpGet:
		value, exists := s.data[cmd.Key]
		return Result{Value: value, Ok: exists}

	case OpDelete:
		_, exists := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		return Result{Ok: exists}

	default:
		return Result{Err: "unknown operation"}
	}
}

func (s *Store) maybeSnapshot() {
	s.mu.RLock()
	data, err := json.Marshal(s.data)
	lastApplied := s.lastApplied
	s.mu.RUnlock()

	if err != nil {
		logger.Error("failed to marshal snapshot", "err", err)
		return
	}

	s.node.Snapshot(lastApplied, data)
	s.appliedSince = 0
}

func (s *Store) applySnapshot(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var newData map[string]string
	if err := json.Unmarshal(data, &newData); err != nil {
		logger.Error("failed to unmarshal snapshot", "err", err)
		return
	}

	s.data = newData
	logger.Info("restored from snapshot", "keys", len(s.data))
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.data[key]
	return value, ok
}

func (s *Store) Put(key, value string) error {
	return s.submit(Command{Op: OpPut, Key: key, Value: value})
}

func (s *Store) Delete(key string) error {
	return s.submit(Command{Op: OpDelete, Key: key})
}

func (s *Store) submit(cmd Command) error {
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	index, _, isLeader := s.node.Submit(string(cmdBytes))
	if !isLeader {
		return ErrNotLeader
	}

	ch := make(chan Result, 1)
	s.pendingMu.Lock()
	s.pending[index] = ch
	s.pendingMu.Unlock()

	result := <-ch

	if result.Err != "" {
		return &OpError{msg: result.Err}
	}

	return nil
}

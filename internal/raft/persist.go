// Package raft internal/raft/persist.go
package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"quorum/pkg/logger"
)

type Persister struct {
	dir string
}

type PersistedState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`

	// Snapshot metadata
	LastIncludedIndex int `json:"lastIncludedIndex"`
	LastIncludedTerm  int `json:"lastIncludedTerm"`
}

func NewPersister(dataDir string) (*Persister, error) {
	if err := os.MkdirAll(dataDir, 0o750); err != nil {
		return nil, err
	}
	return &Persister{dir: dataDir}, nil
}

func (p *Persister) statePath() string {
	return filepath.Join(p.dir, "raft_state.json")
}

func (p *Persister) snapshotPath() string {
	return filepath.Join(p.dir, "snapshot.bin")
}

func (p *Persister) Save(state PersistedState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	tmp := p.statePath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}

	if err := os.Rename(tmp, p.statePath()); err != nil {
		return err
	}

	logger.Debug("persisted state",
		"term", state.CurrentTerm,
		"logLen", len(state.Log),
		"snapshotIndex", state.LastIncludedIndex)
	return nil
}

func (p *Persister) Load() (PersistedState, error) {
	data, err := os.ReadFile(p.statePath())
	if os.IsNotExist(err) {
		return PersistedState{}, nil
	}
	if err != nil {
		return PersistedState{}, err
	}

	var state PersistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return PersistedState{}, err
	}

	logger.Info("loaded persisted state",
		"term", state.CurrentTerm,
		"logLen", len(state.Log),
		"snapshotIndex", state.LastIncludedIndex)
	return state, nil
}

func (p *Persister) SaveSnapshot(data []byte) error {
	tmp := p.snapshotPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, p.snapshotPath()); err != nil {
		return err
	}
	logger.Debug("persisted snapshot", "size", len(data))
	return nil
}

func (p *Persister) LoadSnapshot() ([]byte, error) {
	data, err := os.ReadFile(p.snapshotPath())
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	logger.Info("loaded snapshot", "size", len(data))
	return data, nil
}

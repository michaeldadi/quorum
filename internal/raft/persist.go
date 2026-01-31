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

func (p *Persister) Save(state PersistedState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	// Write to temp file first, then rename (atomic)
	tmp := p.statePath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}

	if err := os.Rename(tmp, p.statePath()); err != nil {
		return err
	}

	logger.Debug("persisted state", "term", state.CurrentTerm, "logLen", len(state.Log))
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

	logger.Info("loaded persisted state", "term", state.CurrentTerm, "logLen", len(state.Log))
	return state, nil
}

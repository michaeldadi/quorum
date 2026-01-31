// main.go
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"quorum/internal/kv"
	"quorum/internal/raft"
	"quorum/pkg/logger"
	"syscall"
	"time"
)

func main() {
	nodeID := flag.String("id", "node-1", "node ID")
	port := flag.Int("port", 9001, "RPC port")
	httpPort := flag.Int("http", 8001, "HTTP port")
	dataDir := flag.String("data", "./data", "data directory")
	flag.Parse()

	logger.Init(*nodeID)

	allNodes := map[string]int{
		"node-1": 9001,
		"node-2": 9002,
		"node-3": 9003,
	}

	var peers []string
	for id, p := range allNodes {
		if id != *nodeID {
			peers = append(peers, fmt.Sprintf("localhost:%d", p))
		}
	}

	// Each node gets its own data directory
	nodeDataDir := filepath.Join(*dataDir, *nodeID)
	persister, err := raft.NewPersister(nodeDataDir)
	if err != nil {
		logger.Error("failed to create persister", "err", err)
		os.Exit(1)
	}

	applyCh := make(chan raft.ApplyMsg)
	node := raft.NewNode(*nodeID, peers, applyCh, persister)

	rpcServer, err := raft.NewRPCServer(node, *port)
	if err != nil {
		logger.Error("failed to start RPC server", "err", err)
		os.Exit(1)
	}

	node.Start()

	store := kv.NewStore(node, applyCh)

	kv.NewHTTPServer(store, fmt.Sprintf(":%d", *httpPort))

	go func() {
		for {
			time.Sleep(5 * time.Second)
			term, state := node.GetState()
			logger.Info("status", "term", term, "state", state)
		}
	}()

	logger.Info("node ready",
		"id", *nodeID,
		"rpc", *port,
		"http", *httpPort,
		"data", nodeDataDir)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	if err := rpcServer.Close(); err != nil {
		logger.Error("failed to close RPC server", "err", err)
	}
	node.Stop()
	logger.Info("shutdown complete")
}

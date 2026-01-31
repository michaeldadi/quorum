// main.go
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"quorum/internal/raft"
	"quorum/pkg/logger"
	"syscall"
	"time"
)

func main() {
	nodeID := flag.String("id", "node-1", "node ID")
	port := flag.Int("port", 9001, "RPC port")
	flag.Parse()

	logger.Init(*nodeID)

	// Hardcoded 3-node cluster for now
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

	node := raft.NewNode(*nodeID, peers)

	rpcServer, err := raft.NewRPCServer(node, *port)
	if err != nil {
		logger.Error("failed to start RPC server", "err", err)
		os.Exit(1)
	}

	node.Start()

	go func() {
		for {
			time.Sleep(2 * time.Second)
			term, state := node.GetState()
			logger.Info("status", "term", term, "state", state)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	if err := rpcServer.Close(); err != nil {
		logger.Error("failed to close RPC server", "err", err)
	}
	node.Stop()
}

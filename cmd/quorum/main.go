// main.go
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"quorum/internal/raft"
	"quorum/pkg/logger"
	"strings"
	"syscall"
	"time"
)

func main() {
	nodeID := flag.String("id", "node-1", "node ID")
	port := flag.Int("port", 9001, "RPC port")
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

	applyCh := make(chan raft.ApplyMsg)
	node := raft.NewNode(*nodeID, peers, applyCh)

	rpcServer, err := raft.NewRPCServer(node, *port)
	if err != nil {
		logger.Error("failed to start RPC server", "err", err)
		os.Exit(1)
	}

	node.Start()

	// Log applied commands
	go func() {
		for msg := range applyCh {
			logger.Info("state machine applied", "index", msg.CommandIndex, "command", msg.Command)
		}
	}()

	// Status printer
	go func() {
		for {
			time.Sleep(3 * time.Second)
			term, state := node.GetState()
			logger.Info("status", "term", term, "state", state)
		}
	}()

	// Simple command input
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("Enter commands (or 'quit' to exit):")
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "quit" {
				err := syscall.Kill(syscall.Getpid(), syscall.SIGINT)
				if err != nil {
					return
				}
				return
			}
			if line == "" {
				continue
			}

			index, term, isLeader := node.Submit(line)
			if !isLeader {
				fmt.Println("Not leader, command rejected")
			} else {
				fmt.Printf("Command accepted: index=%d term=%d\n", index, term)
			}
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

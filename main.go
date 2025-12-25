package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os/signal"
	"syscall"
)

func main() {
	var (
		maxAgentProbingRate = flag.Uint("max_agent_probing_rate", 1, "Maximum number of probes an agent can send.")
	)

	flag.Parse()

	// Create a root context that is canceled on SIGINT or SIGTERM.
	// This allows the generator to shut down gracefully.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := Config{
		MaxAgentProbingRate: *maxAgentProbingRate,
	}

	// Run the generator until completion or context cancellation.
	// Ignore the context cancellation error, as it represents an expected shutdown path.
	if err := RunOrchestrator(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
		log.Fatal("Generator failed: ", err)
	}
}

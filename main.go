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
		messengerBuffer     = flag.Uint("messenger_buffer", 1, "Number of elements in messenger's buffer.")
	)
	flag.Parse()

	// Create a root context that is canceled on SIGINT or SIGTERM.
	// This allows the orchestrator to shut down gracefully.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := Config{
		MaxAgentProbingRate: *maxAgentProbingRate,
		MessengerBuffer:     *messengerBuffer,
	}

	// Run the orchestrator until an error occurs.
	// Ignore the context cancellation error, as it represents an expected shutdown.
	if err := RunOrchestrator(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
		log.Fatal("Orchestrator failed: ", err)
	}
}

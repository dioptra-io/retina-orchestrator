package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/dioptra-io/retina-orchestrator/internal/retina"
)

func main() {
	var (
		httpAddr  = flag.String("http-addr", "localhost:8080", "Listening address of HTTP server")
		jsonlAddr = flag.String("jsonl-addr", "localhost:50050", "Listening address of JSONL server")
	)
	flag.Parse()

	// Setup the context from the signal handlers.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	orch, err := retina.NewOrchFromConfig(&retina.Config{
		JSONLServerAddress: *jsonlAddr,
		HTTPServerAddress:  *httpAddr,
	})
	if err != nil {
		log.Fatalf("cannot create orchestrator with the provided config: %v", err)
	}

	// Run the orchestrator until context cancellation.
	if err := orch.Run(ctx); !errors.Is(err, ctx.Err()) {
		log.Fatalf("orchestrator failed: %v", err)
	}

	log.Println("Shutting down orchestrator.")
}

package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/dioptra-io/retina-orchestrator/internal/retina"
)

func main() {
	var (
		httpAddr  = flag.String("http-addr", "localhost:8080", "Listening address of HTTP server")
		jsonlAddr = flag.String("jsonl-addr", "localhost:50050", "Listening address of JSONL server")
		pdPath    = flag.String("pd-path", "", "PDFile name for reading the probing directives")
		issueRate = flag.Float64("issue-rate", 1.0, "Issue rate of a single probing directive")
		impactCap = flag.Float64("impact-cap", 1.0, "Impact capacity for a single address")
		seed      = flag.Uint64("seed", 42, "Seed for the randomizer")
		secret    = flag.String("secret", "", "Secret string to share with the agent")
	)
	flag.Parse()

	// Setup the context from the signal handlers.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	orch, err := retina.NewOrch(&retina.Config{
		JSONLServerAddress: *jsonlAddr,
		HTTPServerAddress:  *httpAddr,
		PDPath:             *pdPath,
		IssueRate:          *issueRate,
		Seed:               *seed,
		ImpactCap:          *impactCap,
		SecretString:       *secret,
		JSONLBufferLength:  8192, // hard-coded for now
		JSONLTimeout:       time.Hour,
		HTTPTimeout:        time.Hour,
	})
	if err != nil {
		log.Printf("cannot create orchestrator with the provided config: %v", err)
		return
	}

	// Run the orchestrator until context cancellation.
	if err := orch.Run(ctx); !errors.Is(err, ctx.Err()) {
		log.Printf("orchestrator failed: %v", err)
		return
	}

	log.Println("Shutting down orchestrator.")
}

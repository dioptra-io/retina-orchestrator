package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/dioptra-io/retina-orchestrator/pkg/orchestrator"
)

func main() {
	address := flag.String("address", ":50050", "Address of the orchestrator")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := orchestrator.Run(ctx, *address); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}

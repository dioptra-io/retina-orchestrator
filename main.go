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
		httpAddr            = flag.String("http-addr", "localhost:8080", "Listening address of the http server")
		jsonlAddr           = flag.String("jsonl-addr", "localhost:50050", "Listening address JSONL server")
		tcpBuffer           = flag.Int("jsonl-buffer", 8*1024, "JSONL connection buffer size")
		tcpTimeout          = flag.Duration("jsonl-timeout", 5*time.Minute, "JSONL connection timeout")
		ringBufferCapacity  = flag.Uint64("rb-cap", 64*1024, "Capacity of the ring buffer")
		pdSchedulerCooldown = flag.Duration("scheduler-cooldown", time.Second, "Probing directive scheduler's cooldown")
		agentAuthSecret     = flag.String("agent-auth-secret", "", "Secret used by the agents to authenticate.")
	)
	flag.Parse()

	// Setup the context from the signal handlers.
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	orch := retina.NewOrchFromConfig(&retina.Config{
		HTTPAddress:         *httpAddr,
		JSONLAddress:        *jsonlAddr,
		AgentTCPBufferSize:  *tcpBuffer,
		AgentTCPTimeout:     *tcpTimeout,
		RingBufferCapacity:  *ringBufferCapacity,
		PDSchedulerCooldown: *pdSchedulerCooldown,
		AgentAuthSecret:     *agentAuthSecret,
	})

	if err := orch.Run(ctx); err != nil && !errors.Is(err, ctx.Err()) {
		log.Printf("orchestrator failed: %v", err)
		return
	}

	log.Println("Shutting down gracefully")
}

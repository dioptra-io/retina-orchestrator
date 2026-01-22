package retina

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"golang.org/x/sync/errgroup"
)

// Config is the configuration struct used by Orch.
type Config struct {
	// HTTPAddress is the address to listen.
	HTTPAddress string
	// JSONLAddress is the address orchestrator and agents communicates.
	JSONLAddress string
	// AgentTCPBufferSize is the TCP read and write buffer size between the
	// orchestrator and the agents.
	AgentTCPBufferSize int
	// AgentTCPTimeout is the timeout used in read and write operations between
	// the orchestrator and the agents.
	AgentTCPTimeout time.Duration
	// RingBufferCapacity is the number of elements that can be hold in the
	// ring buffer.
	RingBufferCapacity uint64
	// PDSchedulerCooldown is the cooldown period for the scheduler.
	PDSchedulerCooldown time.Duration
}

// orch is the implementation of the retina orchestrator.
type orch struct {
	// config is the configuration used in orchestrator.
	config *Config
	// server is the http server used to expose the orchestrator api.
	server *http.Server
	// tcpserver handles communication with the agents.
	jsonlServer *JSONLServer[api.ProbingDirective, api.ForwardingInfoElement]
	// connectedAgents holds a set of connected agents.
	connectedAgents *SafeMap[api.AgentInfo]
	// ringBuffer is the rungbuffer used by the clients to stream
	// ForwardingInfoElement.
	ringBuffer *RingBuffer[api.ForwardingInfoElement]
	// pdScheduler is the generator that picks new PDs.
	pdScheduler *ProbingDirectiveScheduler
}

// NewOrchFromConfig creates a new orchestrator from the config.
func NewOrchFromConfig(config *Config) *orch {
	// Setups the http handers.
	mux := http.NewServeMux()

	orch := orch{
		config: config,
		server: &http.Server{
			Addr:    config.HTTPAddress,
			Handler: mux,
		},
		jsonlServer: &JSONLServer[api.ProbingDirective, api.ForwardingInfoElement]{
			Address:         config.JSONLAddress,
			TCPBufferLength: config.AgentTCPBufferSize,
			TCPDeadline:     config.AgentTCPTimeout,
		},
		connectedAgents: NewSafeMap[api.AgentInfo](),
		ringBuffer:      NewRingBuffer[api.ForwardingInfoElement](config.RingBufferCapacity),
		pdScheduler:     NewProbingDirectiveScheduler(config.PDSchedulerCooldown),
	}

	// Add the http handlers.
	mux.HandleFunc("/stream", orch.handleStream)

	// Add the tcp handler.
	orch.jsonlServer.HandleFunc(orch.handleTCPStream)

	return &orch
}

// Run starts the http and tcp listener.
// Returns context error if context is cancelled.
// Returns the error if an irrecoverable error occurs.
func (o *orch) Run(parentCtx context.Context) error {
	group, ctx := errgroup.WithContext(parentCtx)

	// Goroutine: Starts the http server.
	group.Go(func() error {
		log.Printf("Started HTTP Server on %s", o.config.HTTPAddress)

		if err := o.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("http server failed: %w", err)
		}
		return nil
	})

	// Goroutine: Starts the tcp server.
	group.Go(func() error {
		log.Printf("Started JSONL Server on %s", o.config.JSONLAddress)

		if err := o.jsonlServer.ListenAndServe(); err != nil && err != ErrServerClosed {
			return fmt.Errorf("tcp server failed: %w", err)
		}
		return nil
	})

	// Goroutine: Starts generating go routines.
	group.Go(func() error {
		return o.handleScheduler(ctx)
	})

	// Goroutine: Waits for the ctx context to cancel. If it get's cancelled
	// then performs a shutdown on the http listeners.
	group.Go(func() error {
		<-ctx.Done()

		// Graceful shutdown (gives handlers time to exit)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := o.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown failed: %w", err)
		}

		return nil
	})

	// Goroutine: Waits for the ctx context to cancel. If it get's cancelled
	// then performs a shutdown on the tcp listeners.
	group.Go(func() error {
		<-ctx.Done()

		// Graceful shutdown (gives handlers time to exit)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := o.jsonlServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("graceful shutdown failed: %w", err)
		}

		return nil
	})

	return group.Wait()
}

// handleTCPStream handles a newly connected agent and streams Probing
// Directives and Forwarding Info Elements.
func (o *orch) handleTCPStream(agentInfo *api.AgentInfo, s *JSONLStreamer[api.ProbingDirective, api.ForwardingInfoElement]) {
	// Add and remove the currently connected agent.
	o.connectedAgents.Add(agentInfo.AgentID, agentInfo)
	defer o.connectedAgents.Pop(agentInfo.AgentID)

	group, _ := errgroup.WithContext(s.Context())

	// Goroutine: Receives the ForwardingInfoElements from the agent and pushes
	// it to the ring buffer.
	group.Go(func() error {
		// TODO: Implement.
		panic("not implemented")
	})

	// Goroutine: Pops from the agents queue, and sends it to agent.
	group.Go(func() error {
		// TODO: Implement.
		panic("not implemented")
	})

	if err := group.Wait(); err != nil {
		log.Printf("Error on the connection with agent %q: %v.\n", agentInfo.AgentID, err)
	}
}

// handleScheduler invokes scheduler to generate new probing directives and
// assigns them to the agents.
func (o *orch) handleScheduler(ctx context.Context) error {
	// TODO: Implement.
	panic("not implemented")
}

// stream godoc
// @Summary Stream server-sent events
// @Description Streams ForwardingInfoElement the latest updates to the connected clients as SSE.
// @Tags stream
// @Produce text/event-stream
// @Success 200
// @Router /stream [get]
func (o *orch) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	_, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	log.Println("SSE client connected")

	// TODO: In a loop get the ForwardingInfoElement from the ring buffer and
	// flush.
	panic("not implemented")
}

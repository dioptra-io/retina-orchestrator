package retina

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/dioptra-io/retina-orchestrator/docs"
	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/dioptra-io/retina-commons/api/v1"
	"golang.org/x/sync/errgroup"
)

// ForwardingInfoElementWithSeq represents a ForwardingInfoElement with a
// sequence number. It is used on the streaming to clients.
type ForwardingInfoElementWithSeq struct {
	api.ForwardingInfoElement

	// SequenceNumber is used to communicate if there is a skip on the stream.
	// This skip can be caused because of a slow reader. Since we don't want the
	// whole system to throttle down by a slow client, this is a solution we
	// decided.
	SequenceNumber uint64
}

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
	// PDSleepDuration is the time to sleep if there are no probing directives.
	PDSleepDuration time.Duration
	// AgentAuthSecret is the secret used by agents to authenticate.
	AgentAuthSecret string
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
	connectedAgents *AgentQueue[api.ProbingDirective]
	// ringBuffer is the rungbuffer used by the clients to stream
	// ForwardingInfoElement.
	ringBuffer *RingBuffer[api.ForwardingInfoElement]
	// pdScheduler is the generator that picks new PDs.
	pdScheduler *PDScheduler
}

// NewOrchFromConfig creates a new orchestrator from the config.
func NewOrchFromConfig(config *Config) *orch {
	// Setups the http handers.
	mux := http.NewServeMux()

	orch := orch{
		config: config,
		server: &http.Server{
			Addr:              config.HTTPAddress,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		},
		jsonlServer: &JSONLServer[api.ProbingDirective, api.ForwardingInfoElement]{
			Address:         config.JSONLAddress,
			TCPBufferLength: config.AgentTCPBufferSize,
			TCPDeadline:     config.AgentTCPTimeout,
		},
		connectedAgents: NewSafeMap[api.ProbingDirective](),
		ringBuffer:      NewRingBuffer[api.ForwardingInfoElement](config.RingBufferCapacity),
		pdScheduler:     NewProbingDirectiveScheduler(config.PDSchedulerCooldown),
	}

	// Add the http handlers.
	mux.HandleFunc("/stream", orch.handleStream)
	mux.HandleFunc("/directives", orch.handleInsertDirectives)
	mux.HandleFunc("/swagger/", httpSwagger.WrapHandler)

	// Add the JSONLServer stream handler.
	orch.jsonlServer.HandleFunc(orch.handleJSONLStream)

	// Add the JSONLServer's auth handler.
	orch.jsonlServer.AuthHandleFunc(orch.handleJSONLAuth)

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
		log.Printf("Swagger can be reached on http://%s/swagger/index.html", o.config.HTTPAddress)

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

// handleJSONLAuth is used to handle the incoming connections from agents. It
// checks for two conditions. First, it checks is the secrets match. Second, it
// checks if the AgentID on the request is already connected.
func (o *orch) handleJSONLAuth(req api.AuthRequest) api.AuthResponse {
	if req.Secret != o.config.AgentAuthSecret {
		return api.AuthResponse{
			Authenticated: false,
			Message:       "Secrets does not match.",
		}
	}

	if _, err := o.connectedAgents.Contains(req.AgentID); err == nil {
		return api.AuthResponse{
			Authenticated: false,
			Message:       "Agent with that AgentID is already connected.",
		}
	}

	return api.AuthResponse{
		Authenticated: true,
		Message:       "Succes.",
	}
}

// handleJSONLStream handles a newly connected agent and streams Probing
// Directives and Forwarding Info Elements.
func (o *orch) handleJSONLStream(agentInfo *AgentInfo, s *JSONLStreamer[api.ProbingDirective, api.ForwardingInfoElement]) {
	// Add and remove the currently connected agent.
	if err := o.connectedAgents.AddAgent(agentInfo.AgentID, agentInfo); err != nil {
		log.Printf("Agent with ID %s is already connected, dropping second connection.\n", agentInfo.AgentID)
		return
	}
	defer func() {
		if err := o.connectedAgents.RemoveAgent(agentInfo.AgentID); err != nil {
			log.Printf("Cannot remove active agent with ID %s, it is already removed.\n", agentInfo.AgentID)
		}
	}()

	group, ctx := errgroup.WithContext(s.Context())

	// Goroutine: Receives the ForwardingInfoElements from the agent and pushes
	// it to the ring buffer.
	group.Go(func() error {
		var (
			fie *api.ForwardingInfoElement
			err error
		)

		// Do this until the context is cancelled.
		for {
			if fie, err = s.Receive(); err != nil {
				return err
			}

			if err = o.ringBuffer.Push(ctx, fie); err != nil {
				return err
			}
		}
	})

	// Goroutine: Pops from the agents queue, and sends it to agent.
	group.Go(func() error {
		var (
			pd  *api.ProbingDirective
			err error
		)

		// Do this until the context is cancelled.
		for {
			if pd, err = o.connectedAgents.Receive(ctx, agentInfo.AgentID); err != nil {
				return err
			}

			if err = s.Send(pd); err != nil {
				return err
			}
		}
	})

	// Note: Here the stream send and receive does not take a context because
	// their cancellation is connected to the streams own context.

	if err := group.Wait(); err != nil {
		log.Printf("Error on the connection with agent %q: %v.\n", agentInfo.AgentID, err)
	}
}

// handleScheduler invokes scheduler to generate new probing directives and
// assigns them to the agents.
func (o *orch) handleScheduler(ctx context.Context) error {
	for {
		// Check for the context cancellation.
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
		}

		// If the Select fails the only other option is to wait until there are
		// probing directives. Instead of making this event driven make it sleep
		// for a duration. If the context is cancelled then return the cancelled
		// error.
		probingDirective, err := o.pdScheduler.Select(ctx)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case <-time.After(o.config.PDSleepDuration):
				continue
			}
		}

		// The agent can be disconnected, in this case we ignore the error. Note
		// that if an agent us disconnected the probing directive scheduler
		// needs to be notified. Otherwise the logs will be flodded with this
		// message.
		if err := o.connectedAgents.Send(ctx, probingDirective.AgentID, probingDirective); err != nil {
			log.Printf("Cannot assign probing directive to any of the active agents. Required ID is %v.\n", probingDirective.AgentID)
		}
	}
}

// stream godoc
// @Summary Stream server-sent events
// @Description Streams ForwardingInfoElementWithSeq the latest updates to the connected clients as SSE.
// @Tags stream
// @Produce text/event-stream
// @Success 200
// @Router /stream [get]
func (o *orch) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	ringBufferClient := o.ringBuffer.NewConsumer()
	defer ringBufferClient.Close()

	encoder := json.NewEncoder(w)

	var (
		fie         *api.ForwardingInfoElement
		fieSeq      *ForwardingInfoElementWithSeq
		err         error
		sequenceNum uint64
	)

	for {
		fie, sequenceNum, err = ringBufferClient.Next(ctx)
		if err != nil {
			return
		}

		// Add the sequence number as an additional field.
		fieSeq = &ForwardingInfoElementWithSeq{
			ForwardingInfoElement: *fie,
			SequenceNumber:        sequenceNum,
		}

		if err = encoder.Encode(fieSeq); err != nil {
			return
		}

		// Send the encoded data after every write.
		flusher.Flush()
	}
}

// insertDirectives godoc
// @Summary Insert probing directives
// @Description Inserts a list of probing directives into the scheduler.
// @Tags scheduler
// @Accept json
// @Produce json
// @Param directives body []api.ProbingDirective true "List of probing directives"
// @Success 200 {string} string "OK"
// @Failure 400 {string} string "Bad Request"
// @Failure 500 {string} string "Internal Server Error"
// @Router /directives [post]
func (o *orch) handleInsertDirectives(w http.ResponseWriter, r *http.Request) {
	var directives []*api.ProbingDirective

	if err := json.NewDecoder(r.Body).Decode(&directives); err != nil {
		http.Error(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	if err := o.pdScheduler.Set(directives); err != nil {
		http.Error(w, "failed to insert directives: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

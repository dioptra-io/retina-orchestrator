// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"github.com/dioptra-io/retina-commons/pkg/queue/v1"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

// orchestrator manages the routing of messages between generators, agents, and streaming clients. It maintains separate
// queues for each message type and handles the lifecycle of the HTTP server.
//
// The orchestrator is not safe for concurrent modification after Run() is called, but handles concurrent connections
// safely through its internal queue implementations.
type orchestrator struct {
	// pdQueue holds probing directives waiting to be delivered to specific agents. Messages are routed using the agent
	// ID as the subscriber identifier.
	pdQueue *queue.Queue[api.ProbingDirective]

	// fieQueue holds forwarding information elements collected from agents. These are broadcast to all subscribed
	// streaming clients.
	fieQueue *queue.Queue[api.ForwardingInfoElement]

	// settingsQueue distributes configuration updates to all connected generators.
	settingsQueue *queue.Queue[api.SystemStatus]

	// httpServer is the underlying HTTP server that handles WebSocket upgrades and SSE connections.
	httpServer *http.Server
}

// Run starts the orchestrator HTTP server and blocks until the context is cancelled or an unrecoverable error occurs.
//
// The server exposes three endpoints:
//   - GET /generator: WebSocket endpoint for generator connections
//   - GET /agent?agent_id=<id>: WebSocket endpoint for agent connections
//   - GET /stream: SSE endpoint for streaming forwarding information elements
//
// When the parent context is cancelled, the server initiates a graceful shutdown with a 1-second timeout, allowing
// in-flight requests to complete.
//
// Parameters:
//   - parent: Context that controls the server lifecycle. Cancelling this context
//     triggers a graceful shutdown.
//   - address: Network address to listen on (e.g., ":8080" or "127.0.0.1:8080")
//
// Returns an error if the server fails to start or encounters an error during shutdown. Returns nil if shutdown
// completes successfully after context cancellation.
func Run(parent context.Context, address string) error {
	g, ctx := errgroup.WithContext(parent)

	mux := http.NewServeMux()
	o := &orchestrator{
		pdQueue:       queue.NewQueue[api.ProbingDirective](1024),
		settingsQueue: queue.NewQueue[api.SystemStatus](1024),
		fieQueue:      queue.NewQueue[api.ForwardingInfoElement](1024),
		httpServer: &http.Server{
			Addr:    address,
			Handler: mux,
			// BaseContext injects the errgroup context into all request contexts, ensuring handlers are notified when
			// shutdown begins.
			BaseContext: func(_ net.Listener) context.Context {
				return ctx
			},
		},
	}

	// Adding handlers here.
	mux.HandleFunc("/generator", o.handleGenerator)
	mux.HandleFunc("/agent", o.handleAgent)
	mux.HandleFunc("/stream", o.handleStream)

	// Start the HTTP server in a separate goroutine.
	g.Go(func() error {
		log.Printf("orchestrator listening on %s", o.httpServer.Addr)

		if err := o.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	// Wait for context cancellation and initiate graceful shutdown.
	g.Go(func() error {
		<-ctx.Done()

		log.Println("orchestrator shutting down")

		// Use a fresh context for shutdown since the parent is already cancelled.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		return o.httpServer.Shutdown(shutdownCtx)
	})

	return g.Wait()
}

// handleGenerator manages WebSocket connections from generator clients.
//
// Generators are responsible for creating probing directives based on current settings. This handler:
//  1. Upgrades the HTTP connection to a WebSocket
//  2. Subscribes the generator to receive settings updates
//  3. Forwards received probing directives to the appropriate agent queues
//
// The handler runs two concurrent goroutines:
//   - One reads settings from the queue and writes them to the WebSocket
//   - One reads probing directives from the WebSocket and routes them to agents
//
// The connection is closed when either goroutine encounters an error or when the request context is cancelled.
//
// Probing directives targeting non-existent agents are logged and discarded rather than causing an error, allowing
// generators to speculatively target agents that may connect later.
func (o *orchestrator) handleGenerator(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWS(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	generatorID := uuid.New().String()
	if err := o.settingsQueue.SubscribeWithID(generatorID); err != nil {
		log.Printf("failed to register generator %s to settings queue: %v", generatorID, err)
		return
	}
	defer o.settingsQueue.Unsubscribe(generatorID)

	log.Printf("generator %s connected", generatorID)

	g, ctx := errgroup.WithContext(r.Context())

	// Goroutine: Send settings updates to the generator.
	g.Go(func() error {
		for {
			settings, err := o.settingsQueue.Get(ctx, generatorID)
			if err != nil {
				return err
			}

			if err := conn.WriteJSON(settings); err != nil {
				return ErrDisconnected
			}
		}
	})

	// Goroutine: Receive probing directives and route to target agents.
	g.Go(func() error {
		for {
			_, pdJSONString, err := conn.ReadMessage()
			if err != nil {
				return ErrDisconnected
			}

			pd := new(api.ProbingDirective)
			if err := json.Unmarshal(pdJSONString, pd); err != nil {
				return err
			}

			// Whisper sends the directive only to the specific agent identified by pd.AgentID. If the agent doesn't
			// exist, log and continue rather than failing the generator connection.
			if err := o.pdQueue.Whisper(ctx, pd.AgentID, pd); err != nil {
				if err == queue.ErrSubscriberDoesNotExist {
					log.Printf("generator %s generated pd for a non existing agent, ignoring...", generatorID)
					continue
				}
				return err
			}
		}
	})

	if err := g.Wait(); err != nil && err != ctx.Err() && err != ErrDisconnected {
		log.Printf("generator %s disconnected with error: %v", generatorID, err)
		return
	}

	log.Printf("generator %s disconnected", generatorID)
}

// handleAgent manages WebSocket connections from measurement agent clients.
//
// Agents execute probing directives and report forwarding information elements. This handler:
//  1. Upgrades the HTTP connection to a WebSocket
//  2. Validates the required agent_id query parameter
//  3. Subscribes the agent to receive probing directives targeted at its ID
//  4. Broadcasts received forwarding information elements to all stream subscribers
//
// The handler runs two concurrent goroutines:
//   - One reads probing directives from the queue and writes them to the WebSocket
//   - One reads forwarding info elements from the WebSocket and broadcasts them
//
// The agent_id query parameter is required and must be unique among connected agents. If an agent connects with an ID
// that is already subscribed, the subscription will fail and the connection will be closed.
//
// URL format: /agent?agent_id=<unique-agent-identifier>
func (o *orchestrator) handleAgent(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWS(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	agentID := r.URL.Query().Get("agent_id")
	if agentID == "" {
		log.Println("agent without an agent_id tried to establish connection.")
		return
	}

	if err := o.pdQueue.SubscribeWithID(agentID); err != nil {
		log.Printf("failed to register agent %s to pd queue: %v", agentID, err)
		return
	}
	defer o.pdQueue.Unsubscribe(agentID)
	log.Printf("agent %s connected", agentID)

	g, ctx := errgroup.WithContext(r.Context())

	// Goroutine: Send probing directives to the agent.
	g.Go(func() error {
		for {
			pd, err := o.pdQueue.Get(ctx, agentID)
			if err != nil {
				return err
			}

			if err := conn.WriteJSON(pd); err != nil {
				return err
			}
		}
	})

	// Goroutine: Receive forwarding info elements and broadcast to all stream clients.
	g.Go(func() error {
		for {
			_, fieJSONString, err := conn.ReadMessage()
			if err != nil {
				return ErrDisconnected
			}

			fie := new(api.ForwardingInfoElement)
			if err := json.Unmarshal(fieJSONString, fie); err != nil {
				return err
			}

			// Broadcast sends the element to all subscribed stream clients.
			if err := o.fieQueue.Broadcast(ctx, fie); err != nil {
				return err
			}
		}
	})

	if err := g.Wait(); err != nil && err != ctx.Err() && err != ErrDisconnected {
		log.Printf("agent %s disconnected with error: %v", agentID, err)
		return
	}

	log.Printf("agent %s disconnected", agentID)
}

// handleStream provides a Server-Sent Events (SSE) endpoint for consuming forwarding information elements in real-time.
//
// This handler:
//  1. Upgrades the HTTP connection to support SSE streaming
//  2. Subscribes the client to receive all broadcasted forwarding info elements
//  3. Streams each element as a newline-delimited JSON object
//
// The stream remains open until either the client disconnects or the server shuts down. Each forwarding information
// element is written as a single line of JSON followed by a newline character, then immediately flushed to the client.
//
// Output format: One JSON object per line (newline-delimited JSON / NDJSON)
//
// Note: This endpoint requires the ResponseWriter to implement http.Flusher. If streaming is not supported, an error
// is returned during the SSE upgrade.
func (o *orchestrator) handleStream(w http.ResponseWriter, r *http.Request) {
	f, err := upgradeToSSE(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	clientID := uuid.New().String()

	if err := o.fieQueue.SubscribeWithID(clientID); err != nil {
		log.Printf("failed to register client %s to fie queue: %v", clientID, err)
		return
	}
	defer o.fieQueue.Unsubscribe(clientID)
	log.Printf("client %s connected", clientID)

	g, ctx := errgroup.WithContext(r.Context())

	// Goroutine: Stream forwarding info elements to the client as NDJSON.
	g.Go(func() error {
		for {
			fie, err := o.fieQueue.Get(ctx, clientID)
			if err != nil {
				return err
			}

			fieJSONBytes, err := json.Marshal(fie)
			if err != nil {
				return err
			}

			// Write the JSON line and flush immediately to ensure low-latency delivery. This can be improved by
			// chunking the fies.
			fmt.Fprintf(w, "%s\n", string(fieJSONBytes))
			f.Flush()
		}
	})

	if err := g.Wait(); err != nil && err != ctx.Err() {
		log.Printf("client %s disconnected with error: %v", clientID, err)
		return
	}

	log.Printf("client %s disconnected", clientID)
}

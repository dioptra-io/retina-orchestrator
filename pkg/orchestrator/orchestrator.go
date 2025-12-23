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
	"slices"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
)

type orchestrator struct {
	httpServer *http.Server

	// messengers
	messengerSystemStatus          *Messenger[api.SystemStatus]
	messengerForwardingInfoElement *Messenger[api.ForwardingInfoElement]
	messengerProbingDirective      *Messenger[api.ProbingDirective]

	currentStatus api.SystemStatus
	mu            sync.RWMutex
}

func Run(parent context.Context, address string) error {
	g, ctx := errgroup.WithContext(parent)

	r := mux.NewRouter()
	o := &orchestrator{
		// Initialize the messengers.
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),

		// Defult values of the system status us given here, they are defined in the FSD and DSD.
		currentStatus: api.SystemStatus{
			GlobalProbingRatePSPA:          1,
			ProbingImpactLimitMS:           1000,
			DisallowedDestinationAddresses: []net.IP{},
			ActiveAgentIDs:                 []string{},
		},
		httpServer: &http.Server{
			Addr:    address,
			Handler: r,
			// BaseContext injects the errgroup context into all request contexts, ensuring handlers are notified when
			// shutdown begins.
			BaseContext: func(_ net.Listener) context.Context {
				return ctx
			},
		},
	}

	// SSE: clients stream FIEs
	r.HandleFunc("/stream", o.handleStream).Methods(http.MethodGet)

	// WebSocket: generator streams SS, receives FIEs
	r.HandleFunc("/generator", o.handleGenerator).Methods(http.MethodGet)

	// WebSocket: agents stream PDs, receive FIEs
	r.HandleFunc("/agent/{id}", o.handleAgent).Methods(http.MethodGet)

	// Admin: settings
	r.HandleFunc("/settings/{name}", o.handleGetSetting).Methods(http.MethodGet)
	r.HandleFunc("/settings/{name}", o.handleSetSetting).Methods(http.MethodPut)

	g.Go(func() error {
		log.Printf("orchestrator listening on %s", o.httpServer.Addr)

		if err := o.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-ctx.Done()

		log.Println("orchestrator shutting down")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		return o.httpServer.Shutdown(shutdownCtx)
	})

	return g.Wait()
}

func (o *orchestrator) handleStream(w http.ResponseWriter, r *http.Request) {
	f, err := upgradeToSSE(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	clientID := uuid.New().String()

	if err := o.messengerForwardingInfoElement.RegisterAs(clientID); err != nil {
		log.Printf("failed to register client %s to fie queue: %v", clientID, err)
		return
	}
	defer o.messengerForwardingInfoElement.UnregisterAs(clientID)
	log.Printf("client %s connected", clientID)

	g, ctx := errgroup.WithContext(r.Context())

	// Goroutine: Stream forwarding info elements to the client as NDJSON.
	g.Go(func() error {
		for {
			fie, err := o.messengerForwardingInfoElement.GetAs(ctx, clientID)
			if err != nil {
				return err
			}

			fieJSONBytes, err := json.Marshal(fie)
			if err != nil {
				return err
			}

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

func (o *orchestrator) handleGenerator(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWS(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	generatorID := uuid.New().String()

	if err := o.messengerSystemStatus.RegisterAs(generatorID); err != nil {
		log.Printf("failed to register generator %s to settings queue: %v", generatorID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer o.messengerSystemStatus.UnregisterAs(generatorID)
	log.Printf("generator %s connected", generatorID)

	g, ctx := errgroup.WithContext(r.Context())

	// Goroutine: Send settings updates to the generator.
	g.Go(func() error {
		for {
			settings, err := o.messengerSystemStatus.GetAs(ctx, generatorID)
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

			o.messengerProbingDirective.SendTo(ctx, pd.AgentID, pd)
		}
	})

	o.notifyGenerator(ctx)

	if err := g.Wait(); err != nil && err != ctx.Err() && err != ErrDisconnected {
		log.Printf("generator %s disconnected with error: %v", generatorID, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("generator %s disconnected", generatorID)
}

func (o *orchestrator) handleAgent(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWS(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	agentID := mux.Vars(r)["id"]
	if agentID == "" {
		log.Println("agent without an agent_id tried to establish connection.")
		return
	}

	if err := o.messengerProbingDirective.RegisterAs(agentID); err != nil {
		log.Printf("failed to register agent %s to pd queue: %v", agentID, err)
		return
	}
	defer o.messengerProbingDirective.UnregisterAs(agentID)
	log.Printf("agent %s connected", agentID)

	g, ctx := errgroup.WithContext(r.Context())

	// Goroutine: Send probing directives to the agent.
	g.Go(func() error {
		for {
			pd, err := o.messengerProbingDirective.GetAs(ctx, agentID)
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
			o.messengerForwardingInfoElement.SendAll(ctx, fie)
		}
	})

	o.updateAgentConnection(ctx, agentID, true)
	defer o.updateAgentConnection(ctx, agentID, false)

	if err := g.Wait(); err != nil && err != ctx.Err() && err != ErrDisconnected {
		log.Printf("agent %s disconnected with error: %v", agentID, err)
		return
	}

	log.Printf("agent %s disconnected", agentID)
}

func (o *orchestrator) handleGetSetting(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	var value any

	switch name {
	case "global_probing_rate_pspa":
		o.mu.RLock()
		defer o.mu.RUnlock()
		value = o.currentStatus.GlobalProbingRatePSPA

	case "probing_impact_limit_ms":
		o.mu.RLock()
		defer o.mu.RUnlock()
		value = o.currentStatus.ProbingImpactLimitMS

	case "disallowed_destination_addresses":
		o.mu.RLock()
		defer o.mu.RUnlock()
		value = o.currentStatus.DisallowedDestinationAddresses

	default:
		http.Error(w, "unknown or missing setting name", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (o *orchestrator) handleSetSetting(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := mux.Vars(r)["name"]
	defer r.Body.Close()

	var value any
	updatedSetting := false

	if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	switch name {
	case "global_probing_rate_pspa":
		o.mu.Lock()
		if newValue, ok := castJSONValueToUInt(value); ok {
			o.currentStatus.GlobalProbingRatePSPA = newValue
			updatedSetting = true
		}
		o.mu.Unlock()

	case "probing_impact_limit_ms":
		o.mu.Lock()
		if newValue, ok := castJSONValueToUInt(value); ok {
			o.currentStatus.ProbingImpactLimitMS = newValue
			updatedSetting = true
		}
		o.mu.Unlock()

	case "disallowed_destination_addresses":
		o.mu.Lock()
		if newValue, ok := castJSONValueToIPArray(value); ok {
			o.currentStatus.DisallowedDestinationAddresses = newValue
			updatedSetting = true
		}
		o.mu.Unlock()

	default:
		http.Error(w, "unknown or missing setting name", http.StatusBadRequest)
		return
	}

	if !updatedSetting {
		http.Error(w, "type mismatch", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
	defer o.notifyGenerator(ctx)
}

func (o *orchestrator) notifyGenerator(ctx context.Context) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	// manual deepcopy, we need to have Deepcopy in retina-commons?
	statusCopy := o.currentStatus
	statusCopy.DisallowedDestinationAddresses = append([]net.IP(nil), o.currentStatus.DisallowedDestinationAddresses...)
	statusCopy.ActiveAgentIDs = append([]string(nil), o.currentStatus.ActiveAgentIDs...)

	o.messengerSystemStatus.SendAll(ctx, &statusCopy)
}

func (o *orchestrator) updateAgentConnection(ctx context.Context, agentID string, connected bool) {
	o.mu.Lock()

	if connected {
		if !slices.Contains(o.currentStatus.ActiveAgentIDs, agentID) {
			o.currentStatus.ActiveAgentIDs = append(o.currentStatus.ActiveAgentIDs, agentID)
		}
	} else {
		o.currentStatus.ActiveAgentIDs = slices.DeleteFunc(o.currentStatus.ActiveAgentIDs, func(id string) bool {
			return id == agentID
		})
	}
	o.mu.Unlock()

	o.notifyGenerator(ctx)
}

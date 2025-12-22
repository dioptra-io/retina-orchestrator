// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
)

type orchestrator struct {
	httpServer *http.Server

	currentStatus api.SystemStatus
	mu            sync.RWMutex
}

func Run(parent context.Context, address string) error {
	g, ctx := errgroup.WithContext(parent)

	r := mux.NewRouter()
	o := &orchestrator{
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
	// r.HandleFunc("/stream", o.handleStream).Methods(http.MethodGet)

	// WebSocket: generator streams SS, receives FIEs
	// r.HandleFunc("/generator", o.handleGenerator).Methods(http.MethodGet)

	// WebSocket: agents stream PDs, receive FIEs
	// r.HandleFunc("/agent/{id}", o.handleAgent).Methods(http.MethodGet)

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
		defer o.mu.Unlock()
		if newValue, ok := castJSONValueToUInt(value); ok {
			o.currentStatus.GlobalProbingRatePSPA = newValue
			updatedSetting = true
		}

	case "probing_impact_limit_ms":
		o.mu.Lock()
		defer o.mu.Unlock()
		if newValue, ok := castJSONValueToUInt(value); ok {
			o.currentStatus.ProbingImpactLimitMS = newValue
			updatedSetting = true
		}

	case "disallowed_destination_addresses":
		o.mu.Lock()
		defer o.mu.Unlock()
		if newValue, ok := castJSONValueToIPArray(value); ok {
			o.currentStatus.DisallowedDestinationAddresses = newValue
			updatedSetting = true
		}

	default:
		http.Error(w, "unknown or missing setting name", http.StatusBadRequest)
		return
	}

	if !updatedSetting {
		http.Error(w, "type mismatch", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

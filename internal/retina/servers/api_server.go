// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

package servers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	httpSwagger "github.com/swaggo/http-swagger"

	// _ "github.com/dioptra-io/retina-orchestrator/docs"
	"github.com/dioptra-io/retina-commons/api/v1"
)

// SequencedFIE wraps a ForwardingInfoElement with a sequence number
// for ordered delivery to HTTP clients.
type SequencedFIE struct {
	api.ForwardingInfoElement
	SequenceNumber uint64 `json:"sequence_number"`
}

// FIEHandleFunc is called for each incoming request to the /stream endpoint.
type FIEHandleFunc func(s *FIEClient)

// APIServerConfig configures the HTTP API server.
type APIServerConfig struct {
	// Address is the TCP listening address in the form "host:port".
	Address string
	// ReadHeaderTimeout is the timeout for reading HTTP request headers.
	ReadHeaderTimeout time.Duration
	FIEHandler        FIEHandleFunc
	Logger            *slog.Logger
}

// APIServer exposes the FIE streaming endpoint and Swagger UI over HTTP.
type APIServer struct {
	config  *APIServerConfig
	logger  *slog.Logger
	server  *http.Server
	mutex   sync.Mutex
	clients map[*FIEClient]struct{}
}

// NewAPIServer creates a new APIServer from the provided config.
// FIEHandler is passed via config rather than as a direct parameter to
// keep the constructor signature stable as new handlers are added.
func NewAPIServer(config *APIServerConfig) (*APIServer, error) {
	if config.FIEHandler == nil {
		return nil, fmt.Errorf("FIEHandler cannot be nil")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	s := &APIServer{
		config:  config,
		logger:  config.Logger,
		clients: make(map[*FIEClient]struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/stream", s.handleStream)
	mux.HandleFunc("/swagger/", httpSwagger.WrapHandler)

	s.server = &http.Server{
		Addr:              config.Address,
		Handler:           mux,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
	}

	return s, nil
}

// ListenAndServe starts the HTTP server and blocks until Shutdown is called.
func (s *APIServer) ListenAndServe() error {
	if err := s.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown gracefully stops the HTTP server, respecting the provided timeout.
func (s *APIServer) Shutdown(timeout time.Duration) error {
	s.logger.Info("Shutting down API server")

	exitCtx, exitCancel := context.WithTimeout(context.Background(), timeout)
	defer exitCancel()

	if err := s.server.Shutdown(exitCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.logger.Warn("API server shutdown timed out", slog.Duration("timeout", timeout))
		return err
	}
	return nil
}

// @Summary		Stream forwarding info elements
// @Description	Opens a long-lived NDJSON stream of FIEs from connected agents.
// @Tags			stream
// @Produce		application/x-ndjson
// @Success		200	{object}	apiOrch.SequencedFIE
// @Failure		500	{string}	string	"internal server error"
// @Router			/stream [get]
func (s *APIServer) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.logger.Error("streaming unsupported: ResponseWriter does not implement http.Flusher")
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	client := &FIEClient{
		ctx:     r.Context(),
		flusher: flusher,
		encoder: json.NewEncoder(w),
	}
	s.addClient(client)
	s.logger.Debug("Client connected", slog.String("remote_addr", r.RemoteAddr))
	defer func() {
		s.removeClient(client)
		s.logger.Debug("Client disconnected", slog.String("remote_addr", r.RemoteAddr))
	}()

	s.config.FIEHandler(client)
}

func (s *APIServer) addClient(client *FIEClient) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.clients[client] = struct{}{}
}

func (s *APIServer) removeClient(client *FIEClient) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.clients, client)
}

// FIEClient represents an active HTTP client consuming the FIE stream.
type FIEClient struct {
	ctx     context.Context
	flusher http.Flusher
	encoder *json.Encoder
}

// SendFIE encodes and sends a SequencedFIE to the client.
func (s *FIEClient) SendFIE(fie *SequencedFIE) error {
	if err := s.encoder.Encode(fie); err != nil {
		return fmt.Errorf("failed to send FIE: %w", err)
	}
	s.flusher.Flush()
	return nil
}

func (s *FIEClient) Context() context.Context {
	return s.ctx
}

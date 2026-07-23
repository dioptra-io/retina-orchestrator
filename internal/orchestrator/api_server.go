// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

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

	"github.com/dioptra-io/retina-commons/api/v1"
	_ "github.com/dioptra-io/retina-orchestrator/docs"
)

// SequencedFIE is a ForwardingInfoElement with a sequence number for ordered delivery to HTTP clients.
type SequencedFIE struct {
	api.ForwardingInfoElement
	SequenceNumber uint64 `json:"sequence_number"`
}

type fieHandleFunc func(s *fieClient)
type sseHandleFunc func(s *sseClient)
type insertHanleFunc func(*api.ProbingDirective) (uint64, error)
type insertHanleAfterFunc func([]*api.ProbingDirective)

type apiServerConfig struct {
	// address is the TCP listening address in the form "host:port".
	address string
	// readHeaderTimeout is the timeout for reading HTTP request headers.
	readHeaderTimeout time.Duration
	fieHandler        fieHandleFunc
	sseHandler        sseHandleFunc
	insertHandler     insertHanleFunc
	insertAfterHanler insertHanleAfterFunc

	// eventBuffer is the ring buffer for SSE events.
	logger *slog.Logger
}

type apiServer struct {
	config  *apiServerConfig
	logger  *slog.Logger
	server  *http.Server
	mutex   sync.Mutex
	clients map[*fieClient]struct{}
}

func newAPIServer(config *apiServerConfig) (*apiServer, error) {
	if config.fieHandler == nil {
		return nil, fmt.Errorf("fieHandler cannot be nil")
	}
	if config.insertHandler == nil {
		return nil, fmt.Errorf("insertHandler cannot be nil")
	}
	if config.insertAfterHanler == nil {
		return nil, fmt.Errorf("insertAfterHanler cannot be nil")
	}
	if config.sseHandler == nil {
		return nil, fmt.Errorf("sseHandler cannot be nil")
	}
	if config.logger == nil {
		config.logger = slog.Default()
	}

	s := &apiServer{
		config:  config,
		logger:  config.logger,
		clients: make(map[*fieClient]struct{}),
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/api/v1/sse", s.handleSSE)
	mux.HandleFunc("/api/v1/pds", s.handleBulkInsert)
	mux.HandleFunc("/api/v1/stream", s.handleStream)
	mux.HandleFunc("/api/v1/swagger/", httpSwagger.WrapHandler)

	s.server = &http.Server{
		Addr:              config.address,
		Handler:           mux,
		ReadHeaderTimeout: config.readHeaderTimeout,
	}

	return s, nil
}

func (s *apiServer) listenAndServe() error {
	if err := s.server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *apiServer) close(timeout time.Duration) error {
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
// @Success		200	{object}	SequencedFIE
// @Failure		500	{string}	string	"internal server error"
// @Router			/stream [get]
func (s *apiServer) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.logger.Error("Streaming unsupported: ResponseWriter does not implement http.Flusher")
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	client := &fieClient{
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

	s.config.fieHandler(client)
}

// @Summary		Stream server-sent events
// @Description	Opens a long-lived SSE stream of system events.
// @Tags			sse
// @Produce		text/event-stream
// @Success		200	{object}	RetinaEvent
// @Failure		500	{string}	string	"internal server error"
// @Router			/sse [get]
func (s *apiServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		s.logger.Error("Streaming unsupported: ResponseWriter does not implement http.Flusher")
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	client := &sseClient{
		ctx:     r.Context(),
		flusher: flusher,
		encoder: json.NewEncoder(w),
	}
	s.logger.Debug("Client connected", slog.String("remote_addr", r.RemoteAddr))
	defer func() {
		s.logger.Debug("Client disconnected", slog.String("remote_addr", r.RemoteAddr))
	}()

	s.config.sseHandler(client)
}

func (s *apiServer) addClient(client *fieClient) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.clients[client] = struct{}{}
}

func (s *apiServer) removeClient(client *fieClient) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.clients, client)
}

// BulkInsertRequest is the payload for admitting multiple PDs in one call.
type BulkInsertRequest struct {
	ProbingDirectives []*api.ProbingDirective `json:"probing_directives"`
}

// BulkInsertResponse reports the result of a bulk insertion. AssignedIDs is
// in the same order as the request's ProbingDirectives.
type BulkInsertResponse struct {
	InsertedCount int      `json:"inserted_count"`
	AssignedIDs   []uint64 `json:"assigned_ids"`
}

// @Summary		Bulk-insert probing directives
// @Description	Admits multiple PDs into the scheduler in one call, blocking until each is handed off. Aborts on the first insertion error (e.g. scheduler shutdown) and reports how many succeeded.
// @Tags			pds
// @Accept			json
// @Produce		json
// @Success		200	{object}	BulkInsertResponse
// @Failure		400	{string}	string	"invalid request body"
// @Failure		500	{string}	string	"internal server error"
// @Router			/pds [post]
func (s *apiServer) handleBulkInsert(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req BulkInsertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if len(req.ProbingDirectives) == 0 {
		http.Error(w, "probing_directives must not be empty", http.StatusBadRequest)
		return
	}

	ids := make([]uint64, 0, len(req.ProbingDirectives))
	for _, pd := range req.ProbingDirectives {
		id, err := s.config.insertHandler(pd)
		if err != nil {
			s.logger.Error("bulk insert aborted", slog.Int("inserted", len(ids)), slog.Any("error", err))
			break
		}
		ids = append(ids, id)
	}
	s.config.insertAfterHanler(req.ProbingDirectives)

	s.logger.Info("Inserted new PDs into the scheduler",
		slog.Int("num_inserted", len(req.ProbingDirectives)))

	w.Header().Set("Content-Type", "application/json")
	if len(ids) < len(req.ProbingDirectives) {
		w.WriteHeader(http.StatusInternalServerError)
	}
	if err := json.NewEncoder(w).Encode(&BulkInsertResponse{
		InsertedCount: len(ids),
		AssignedIDs:   ids,
	}); err != nil {
		s.logger.Error("failed to encode bulk insert response", slog.Any("error", err))
	}
}

type fieClient struct {
	ctx     context.Context
	flusher http.Flusher
	encoder *json.Encoder
}

func (s *fieClient) sendFIE(fie *SequencedFIE) error {
	if err := s.encoder.Encode(fie); err != nil {
		return fmt.Errorf("failed to send FIE: %w", err)
	}
	s.flusher.Flush()
	return nil
}

func (s *fieClient) context() context.Context {
	return s.ctx
}

// sseClient is a client for SSE streaming.
type sseClient struct {
	ctx     context.Context
	flusher http.Flusher
	encoder *json.Encoder
}

func (s *sseClient) context() context.Context {
	return s.ctx
}

func (s *sseClient) sendEvent(event RetinaEvent) error {
	if err := s.encoder.Encode(event); err != nil {
		return fmt.Errorf("failed to send FIE: %w", err)
	}
	s.flusher.Flush()
	return nil
}

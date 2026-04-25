// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	httpSwagger "github.com/swaggo/http-swagger"

	"github.com/dioptra-io/retina-commons/api/v1"
	_ "github.com/dioptra-io/retina-orchestrator/docs"
	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
)

// SequencedFIE is a ForwardingInfoElement with a sequence number for ordered delivery to HTTP clients.
type SequencedFIE struct {
	api.ForwardingInfoElement
	SequenceNumber uint64 `json:"sequence_number"`
}

type fieHandleFunc func(s *fieClient)
type sseHandleFunc func(s *sseClient)

type apiServerConfig struct {
	// address is the TCP listening address in the form "host:port".
	address string
	// readHeaderTimeout is the timeout for reading HTTP request headers.
	readHeaderTimeout time.Duration
	fieHandler        fieHandleFunc
	sseHandler        sseHandleFunc
	// eventBuffer is the ring buffer for SSE events.
	eventBuffer *structures.RingBuffer[SSEEvent]
	logger      *slog.Logger
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
	if config.logger == nil {
		config.logger = slog.Default()
	}

	s := &apiServer{
		config:  config,
		logger:  config.logger,
		clients: make(map[*fieClient]struct{}),
	}

	// Create default event buffer if not provided
	if config.eventBuffer == nil && config.sseHandler != nil {
		rb, err := structures.NewRingBuffer[SSEEvent](256)
		if err != nil {
			return nil, fmt.Errorf("failed to create default event buffer: %w", err)
		}
		config.eventBuffer = rb
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/stream", s.handleStream)
	if config.sseHandler != nil {
		mux.HandleFunc("/sse", s.handleSSE)
	}
	mux.HandleFunc("/swagger/", httpSwagger.WrapHandler)

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
// @Success		200	{object}	SSEEvent
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

	// Wrap ResponseWriter to implement io.WriteCloser
	ww := &sseResponseWriter{ResponseWriter: w, flusher: flusher}

	client := &sseClient{
		ctx:    r.Context(),
		writer: ww,
	}
	s.logger.Debug("SSE client connected", slog.String("remote_addr", r.RemoteAddr))

	// No client tracking for SSE yet - just call handler
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

// sseResponseWriter wraps http.ResponseWriter to implement io.WriteCloser.
type sseResponseWriter struct {
	http.ResponseWriter
	flusher http.Flusher
}

func (w *sseResponseWriter) Write(b []byte) (int, error) {
	return w.ResponseWriter.Write(b)
}

func (w *sseResponseWriter) Close() error {
	return nil
}

// sseClient is a client for SSE streaming.
type sseClient struct {
	ctx    context.Context
	writer io.WriteCloser
}

func (s *sseClient) sendEvent(event *SSEEvent) error {
	data, err := json.Marshal(event.Data)
	if err != nil {
		return fmt.Errorf("failed to marshal event data: %w", err)
	}

	// SSE format: event: <type>\ndata: <data>\n\n
	_, err = fmt.Fprintf(s.writer, "event: %s\ndata: %s\n\n", event.Type, data)
	if err != nil {
		return fmt.Errorf("failed to write SSE event: %w", err)
	}
	s.writer.(*sseResponseWriter).flusher.Flush()
	return nil
}

func (s *sseClient) context() context.Context {
	return s.ctx
}

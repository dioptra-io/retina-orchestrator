package servers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	httpSwagger "github.com/swaggo/http-swagger"

	_ "github.com/dioptra-io/retina-orchestrator/docs"
	apiOrch "github.com/dioptra-io/retina-orchestrator/internal/retina/api"
)

// StreamForwardingInfoFunc is the handler for the /stream endpoint. It is
// called for each incoming request and is responsible for writing
// SequencedForwardingInfoElement objects to the response writer.
type StreamForwardingInfoFunc func(info *FIEStreamerInfo, s *FIEStreamer)

// HTTPServerConfig is the config struct for the HTTP server.
type HTTPServerConfig struct {
	// Address is the listening address of the http server.
	Address string
	// StreamHandler is the handler for the /stream endpoint.
	StreamHandler StreamForwardingInfoFunc
}

// HTTPServer is a thin HTTP wrapper around the JSONLServer that exposes a
// streaming endpoint and a Swagger UI.
type HTTPServer struct {
	config    *HTTPServerConfig
	server    *http.Server
	mutex     sync.Mutex
	streamers map[*FIEStreamer]struct{}
}

// NewHTTPServer creates a new HTTPServer from the provided config.
func NewHTTPServer(config *HTTPServerConfig) (*HTTPServer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.Address == "" {
		return nil, fmt.Errorf("http server address cannot be empty")
	}
	if config.StreamHandler == nil {
		return nil, fmt.Errorf("stream handler cannot be nil")
	}

	s := &HTTPServer{
		config:    config,
		streamers: make(map[*FIEStreamer]struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/stream", s.handleStream)
	mux.HandleFunc("/swagger/", httpSwagger.WrapHandler)

	s.server = &http.Server{
		Addr:              config.Address,
		Handler:           mux,
		ReadHeaderTimeout: 60 * time.Second,
	}

	return s, nil
}

// ListenAndServe starts the HTTP server. It blocks until the server is shut
// down, at which point it returns http.ErrServerClosed.
func (s *HTTPServer) ListenAndServe() error {
	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Shutdown gracefully stops the HTTP server, respecting the provided context.
func (s *HTTPServer) Shutdown(timeout time.Duration) error {
	exitCtx, exitCancel := context.WithTimeout(context.Background(), timeout)
	defer exitCancel()

	if err := s.server.Shutdown(exitCtx); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// handleStream handles the /stream endpoint.
//
//	@Summary		Stream forwarding info elements
//	@Description	Opens a long-lived connection that streams
//
// SequencedForwardingInfoElement objects encoded as newline-delimited JSON
// (JSONL) as they are produced by the connected agents.
//
//	@Tags			stream
//	@Produce		application/event-stream
//
//	@Success		200	{object}	apiOrch.SequencedForwardingInfoElement
//	@Failure		500	{string}	string	"internal server error"
//	@Router			/stream [get]
func (s *HTTPServer) handleStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Setup the streamer
	streamer := &FIEStreamer{
		ctx:     r.Context(),
		flusher: flusher,
		encoder: json.NewEncoder(w),
	}
	s.addStreamer(streamer)
	defer s.removeStreamer(streamer)

	// Setup the client info, for now this is empty, in the future we can get
	// some other info about the connected clients.
	info := &FIEStreamerInfo{
		UserAgent:     r.UserAgent(),
		RemoteAddress: r.RemoteAddr,
	}

	s.config.StreamHandler(info, streamer)
}

func (s *HTTPServer) addStreamer(streamer *FIEStreamer) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.streamers[streamer] = struct{}{}
}

func (s *HTTPServer) removeStreamer(streamer *FIEStreamer) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.streamers, streamer)
}

type FIEStreamer struct {
	ctx     context.Context
	flusher http.Flusher
	encoder *json.Encoder
}

// Send encodes and sends the given SequencedForwardingInfoElement to the
// client.
func (s *FIEStreamer) Send(fie *apiOrch.SequencedForwardingInfoElement) error {
	if err := s.encoder.Encode(fie); err != nil {
		return fmt.Errorf("error on sending the fie: %w", err)
	}
	s.flusher.Flush()
	return nil
}

func (s *FIEStreamer) Context() context.Context {
	return s.ctx
}

type FIEStreamerInfo struct {
	UserAgent     string
	RemoteAddress string
}

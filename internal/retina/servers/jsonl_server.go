package servers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// JSONLAuthStatus contains the information after agent's authentication.
type JSONLAuthStatus struct {
	AgentID       string
	RemoteAddress net.Addr
	LocalAddress  net.Addr
}

// StreamHandleFunc is the handling function for the stream. It is called in a
// separate goroutine for each new connection.
type StreamHandleFunc func(status *JSONLAuthStatus, s *JSONLStream)

// AuthHandleFunc is the handling function for the stream. It gets the
// api.AuthRequest from the agent and returns the api.AuthResponse. If the
// Authenticated field is false then the connection closed. The api.AuthResponse
// object is send to the agent all the times.
type AuthHandleFunc func(req api.AuthRequest) api.AuthResponse

// JSONLServerConfig is the config struct given to the JSONL Server.
type JSONLServerConfig struct {
	// TCPBufferLength is the size if the tcp read and write buffer.
	TCPBufferLength int
	// TCPTimeout is the timeout duration.
	TCPTimeout time.Duration
	// Address is the listening address of the tcp socket.
	Address string
	// StreamHandler is the function called for each new connection.
	StreamHandler StreamHandleFunc
	// AuthHandler is the function called for each new connection's
	// authentification handling.
	AuthHandler AuthHandleFunc
}

// JSONLServer is the implementation of the tcp socket where the type S is sent,
// and R is received as json lines.
type JSONLServer struct {
	config *JSONLServerConfig
	// closed indicates if the server has been shut down.
	closed atomic.Bool
	// mutex protects the server state.
	mutex sync.Mutex
	// activeStreamers tracks all active connections for shutdown.
	activeStreamers map[int]*JSONLStream
	// listener is the listener that listens for the incoming connections.
	listener net.Listener
	// nextID is used to keep track of the JSONLStreamer.
	nextID int
	// closeWG is the waiting group that is used to wait on shutdown.
	closeWG sync.WaitGroup
}

// NewJSONLServer creates a new JSONL server from the provided arguments.
func NewJSONLServer(config *JSONLServerConfig) (*JSONLServer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.TCPBufferLength < 8192 {
		return nil, fmt.Errorf("tcp buffer length is too small: got %d, minimum 8192", config.TCPBufferLength)
	}
	if config.TCPTimeout <= 0 {
		return nil, fmt.Errorf("tcp timeout must be positive: got %s", config.TCPTimeout)
	}
	if config.Address == "" {
		return nil, fmt.Errorf("jsonl server address cannot be empty")
	}
	if config.AuthHandler == nil || config.StreamHandler == nil {
		return nil, fmt.Errorf("handlers cannot be nil")
	}

	return &JSONLServer{
		config:          config,
		activeStreamers: make(map[int]*JSONLStream),
	}, nil
}

// ListenAndServe starts the server. It spawns a new goroutine for each new
// connection similar to like the http.Server implementation.
//
// If the Shutdown method is called it returns the ErrServerClosed.
func (s *JSONLServer) ListenAndServe() error {
	if s.closed.Load() {
		return ErrServerClosed
	}

	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.listener = listener
	s.mutex.Unlock()

	if s.closed.Load() {
		return ErrServerClosed
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return ErrServerClosed
			}
			return err
		}

		s.mutex.Lock()
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			s.mutex.Unlock()
			return fmt.Errorf("expected TCP connection, got %T", conn)
		}
		streamer := newJSONLStreamer(s.nextID, tcpConn, s)
		s.activeStreamers[s.nextID] = streamer
		s.nextID++
		s.closeWG.Add(1)
		s.mutex.Unlock()

		go s.handleConnection(streamer)
	}
}

// Shutdown closes the listener and all open connections. Calling this method
// will cancel the context of all the JSONLStreamers.
//
// Multiple calls would not change the status and return a nil error.
func (s *JSONLServer) Shutdown(timeout time.Duration) error {
	if s.closed.Swap(true) {
		return nil
	}

	exitCtx, exitCancel := context.WithTimeout(context.Background(), timeout)
	defer exitCancel()

	s.mutex.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	for _, streamer := range s.activeStreamers {
		s.removeStreamer(streamer)
	}
	s.mutex.Unlock()

	// Wait for active goroutines to finish, but respect the deadline.
	done := make(chan struct{})
	go func() {
		s.closeWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-exitCtx.Done():
		return exitCtx.Err()
	}
}

// handleConnection invokes the handler with the newly created JSONLStreamer,
// then it closes it.
func (s *JSONLServer) handleConnection(stream *JSONLStream) {
	defer func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		s.removeStreamer(stream)
	}()

	// Initial handshake.
	agentAuthRequest, err := receive[api.AuthRequest](stream.conn, stream.decoder, s.config.TCPTimeout, s.config.TCPBufferLength)
	if err != nil {
		log.Printf("Initial handshake failed: cannot receive api.AuthRequest: %v.\n", err)
		return
	}
	agentAuthResponse := s.config.AuthHandler(*agentAuthRequest)
	err = send(stream.conn, stream.encoder, s.config.TCPTimeout, s.config.TCPBufferLength, &agentAuthResponse)
	if err != nil {
		log.Printf("Initial handshake failed: cannot send api.AuthResponse: %v.\n", err)
		return
	}
	if !agentAuthResponse.Authenticated {
		log.Printf("Initial handshake failed: agent is not authenticated.\n")
		return
	}

	// Agent is authenticated, invoke stream handler.
	status := &JSONLAuthStatus{
		AgentID:       agentAuthRequest.AgentID,
		LocalAddress:  stream.conn.LocalAddr(),
		RemoteAddress: stream.conn.RemoteAddr(),
	}
	log.Printf("Agent authentification succeeded from %v.\n", stream.conn.RemoteAddr())
	s.config.StreamHandler(status, stream)
}

// removeStreamer cancels the streamer's context, closes its connection, removes
// it from the active streamers map, and releases the WaitGroup slot.
// Must be called with s.mutex held. Must only be called once per streamer.
func (s *JSONLServer) removeStreamer(streamer *JSONLStream) {
	if _, ok := s.activeStreamers[streamer.id]; !ok {
		return
	}
	streamer.cancel()
	_ = streamer.conn.Close()
	delete(s.activeStreamers, streamer.id)
	s.closeWG.Done()
}

// JSONLStream represents an active JSONL connection to an agent.
type JSONLStream struct {
	// id is the identifier in the server.
	id int
	// ctx is the context of the JSONLStreamer.
	ctx context.Context
	// cancel is the cancellation function of the JSONLStreamer.
	cancel context.CancelFunc
	// conn is the underlying tcp connection.
	conn *net.TCPConn
	// encoder is the JSON encoder that writes to the tcp socket.
	encoder *json.Encoder
	// decoder is the JSON decoder that reads from the tcp socket.
	decoder *json.Decoder
	// server is the JSONLServer.
	server *JSONLServer
}

// newJSONLStreamer creates a new JSONLStream from a connection.
func newJSONLStreamer(id int, conn *net.TCPConn, server *JSONLServer) *JSONLStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &JSONLStream{
		id:      id,
		conn:    conn,
		ctx:     ctx,
		cancel:  cancel,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
		server:  server,
	}
}

// Context returns the context of the JSONLStreamer.
func (s *JSONLStream) Context() context.Context {
	return s.ctx
}

// ID returns the connection id of the streamer.
func (s *JSONLStream) ID() int {
	return s.id
}

// Send sends the given element to the tcp connection encoded as a JSON line.
func (s *JSONLStream) Send(e *api.ProbingDirective) error {
	return send(s.conn, s.encoder, s.server.config.TCPTimeout, s.server.config.TCPBufferLength, e)
}

// Receive reads the next element from the tcp connection decoded from a JSON line.
func (s *JSONLStream) Receive() (*api.ForwardingInfoElement, error) {
	return receive[api.ForwardingInfoElement](s.conn, s.decoder, s.server.config.TCPTimeout, s.server.config.TCPBufferLength)
}

// send tries to send the specified type of object from the tcp connection while
// setting up the write deadline and write buffer.
//
// It returns the wrapped error if any of the operations fail.
func send[E any](conn *net.TCPConn, encoder *json.Encoder, timeout time.Duration, buffer int, e *E) error {
	if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("send failed: cannot set write deadline: %w", err)
	}
	if err := conn.SetWriteBuffer(buffer); err != nil {
		return fmt.Errorf("send failed: cannot set write buffer: %w", err)
	}
	if err := encoder.Encode(e); err != nil {
		return fmt.Errorf("send failed: cannot encode: %w", err)
	}
	return nil
}

// receive tries to receive the specified type of object from the tcp
// connection while setting up the read deadline and read buffer.
//
// It returns the wrapped error if any of the operations fail.
func receive[E any](conn *net.TCPConn, decoder *json.Decoder, timeout time.Duration, buffer int) (*E, error) {
	var e E
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return nil, fmt.Errorf("receive failed: cannot set read deadline: %w", err)
	}
	if err := conn.SetReadBuffer(buffer); err != nil {
		return nil, fmt.Errorf("receive failed: cannot set read buffer: %w", err)
	}
	if err := decoder.Decode(&e); err != nil {
		return nil, fmt.Errorf("receive failed: cannot decode: %w", err)
	}
	return &e, nil
}

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

// StreamHandleFunc is the handling function for the stream. It is called in a
// separate goroutine for each new connection.
type StreamHandleFunc func(s *JSONLStream)

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

// newJSONLServer creates a new JSONL server from the provided arguments.
func NewJSONLServer(config *JSONLServerConfig) (*JSONLServer, error) {
	// Check config values.
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.TCPBufferLength <= 8192 {
		return nil, fmt.Errorf("tcp buffer length is too small: got %d, minimum 8192", config.TCPBufferLength)
	}
	if config.TCPTimeout.Seconds() == 0.0 {
		return nil, fmt.Errorf("tcp timeout is not valid: got %s", config.TCPTimeout.String())
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

	// This listener is closed by the Close method.
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}
	s.listener = listener

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
		tcpConn, _ := conn.(*net.TCPConn)
		streamer := newJSONLStreamer(s.nextID, tcpConn, s)
		s.activeStreamers[s.nextID] = streamer
		s.nextID++
		s.mutex.Unlock()

		go s.handleConnection(streamer)
	}
}

// Shutdown closes the listener and all open connections. Calling this method
// will cancel the context of all the JSONLStreamers.
func (s *JSONLServer) Shutdown(ctx context.Context) error {
	if s.closed.Load() {
		return nil
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.listener != nil {
		_ = s.listener.Close()
	}

	for _, streamer := range s.activeStreamers {
		s.removeStreamer(streamer)
	}

	s.closed.Store(true)

	// Waits until all of the streamers are closed.
	s.closeWG.Wait()
	return nil
}

// handleConnection invokes the handler with the newly created JSONLStreamer,
// then it closes it.
func (s *JSONLServer) handleConnection(stream *JSONLStream) {
	s.closeWG.Add(1)

	defer func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		s.removeStreamer(stream)
	}()

	// The first line agent sends should be the AuthRequest.
	var agentAuthRequest api.AuthRequest
	if err := stream.conn.SetReadDeadline(time.Now().Add(s.config.TCPTimeout)); err != nil {
		log.Printf("Initial handshake failed on set connection deadline: %v.\n", err)
		return
	}
	if err := stream.conn.SetReadBuffer(s.config.TCPBufferLength); err != nil {
		log.Printf("Initial handshake failed on set connection buffer: %v.\n", err)
		return
	}
	if err := stream.decoder.Decode(&agentAuthRequest); err != nil {
		log.Printf("Initial handshake failed on decode AuthRequest: %v.\n", err)
		return
	}

	// Get the authentification response from the auth handler.
	var agentAuthResponse api.AuthResponse
	agentAuthResponse = s.config.AuthHandler(agentAuthRequest)

	// Send the api.AuthResponse to the agent.
	if err := stream.encoder.Encode(agentAuthResponse); err != nil {
		log.Printf("Initial handshake failed on decode AuthResponse: %v.\n", err)
	}

	// If the auth handler fails then close the connection.
	if !agentAuthResponse.Authenticated {
		log.Printf("Agent authentification failed on request from: %v.\n", stream.conn.RemoteAddr())
		return
	}

	// Populate the agentID from the authentification request.
	stream.agentID = agentAuthRequest.AgentID

	log.Printf("Agent authentification succeeded from %v.\n", stream.conn.RemoteAddr())

	// Then call the handler if it is set.
	s.config.StreamHandler(stream)
}

// removeStreamer closes the streamer's connection, cancels it context, removes
// it from the active streamers set, and releases the close waiting group.
// This method is not protected by the mutex.
func (s *JSONLServer) removeStreamer(streamer *JSONLStream) {
	_ = streamer.conn.Close()
	// Only call Done if streamer is still in the map (not already removed)
	if _, exists := s.activeStreamers[streamer.id]; exists {
		delete(s.activeStreamers, streamer.id)
		s.closeWG.Done()
	}
}

// JSONLStream is the streamer struct that is returned on the handle function.
type JSONLStream struct {
	// agentID is the id of the connected agent.
	agentID string
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
	// decoder is the JSON decoder that reads from the tcp scoket.
	decoder *json.Decoder
	// server is the JSONLServer.
	server *JSONLServer
}

// newJSONLStreamer creates a new JSONLStreamer from a connection.
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
		agentID: "", // This will be populated later after authentification.
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

// AgentID returns the connected agent's id.
func (s *JSONLStream) AgentID() string {
	return s.agentID
}

// Send sends the given element to the tcp encoded as a JSON. If the streamer is
// closed the Send method unblocks.
func (s *JSONLStream) Send(e *api.ProbingDirective) error {
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.server.config.TCPTimeout)); err != nil {
		return err
	}
	if err := s.conn.SetWriteBuffer(s.server.config.TCPBufferLength); err != nil {
		return err
	}
	if err := s.encoder.Encode(e); err != nil {
		return err
	}
	return nil
}

// Receive sends the given element to the tcp encoded as a JSON. If the streamer
// is closed the Receive method unblocks.
func (s *JSONLStream) Receive() (*api.ForwardingInfoElement, error) {
	var e *api.ForwardingInfoElement
	if err := s.conn.SetReadDeadline(time.Now().Add(s.server.config.TCPTimeout)); err != nil {
		return nil, err
	}
	if err := s.conn.SetReadBuffer(s.server.config.TCPBufferLength); err != nil {
		return nil, err
	}
	if err := s.decoder.Decode(e); err != nil {
		return nil, err
	}
	return e, nil
}

package retina

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

var (
	// ErrJSONLServerClosed is used to denote the server is closed.
	ErrJSONLServerClosed = errors.New("jsonl server closed")
)

type AgentInfo struct {
	agentID string
}

// HandleFunc is the handling function for the stream. If it exists then the
// stream is closed. It is called in a separate goroutine for each new
// connection.
type HandleFunc[S any, R any] func(info *AgentInfo, s *JSONLStreamer[S, R])

// AuthHandleFunc is the handling function for the stream. It gets the
// api.AuthRequest from the agent and returns the api.AuthResponse. If the
// Authenticated field is false then the connection closed. The api.AuthResponse
// object is send to the agent all the times.
type AuthHandleFunc func(req api.AuthRequest) api.AuthResponse

// JSONLServer is the implementation of the tcp socket where the type S is sent,
// and R is received as json lines.
type JSONLServer[S any, R any] struct {
	// TCPBufferLength is the size if the tcp read and write buffer.
	TCPBufferLength int
	// TCPDeadline is the timeout duration.
	TCPDeadline time.Duration
	// Address is the listening address of the tcp socket.
	Address string

	// streamHandler is the function called for each new connection.
	streamHandler HandleFunc[S, R]
	// authHandler is the function called for each new connection's
	// authentification handling.
	authHandler AuthHandleFunc
	// closed indicates if the server has been shut down.
	closed bool
	// mutex protects the server state.
	mutex sync.Mutex
	// activeStreamers tracks all active connections for shutdown.
	activeStreamers map[int]*JSONLStreamer[S, R]
	// listener is the listener that listens for the incoming connections.
	listener net.Listener
	// nextID is used to keep track of the JSONLStreamer.
	nextID int
	// closeWG is the waiting group that is used to wait on shutdown.
	closeWG sync.WaitGroup
}

// StreamHandleFunc sets the handler to the given function.
func (s *JSONLServer[S, R]) StreamHandleFunc(handler HandleFunc[S, R]) {
	s.streamHandler = handler
}

// AuthHandleFunc sets the authentification handler to the given function.
func (s *JSONLServer[S, R]) AuthHandleFunc(handler AuthHandleFunc) {
	s.authHandler = handler
}

// ListenAndServe starts the server. It spawns a new goroutine for each new
// connection similar to like the http.Server implementation.
//
// If the Shutdown method is called it returns the ErrServerClosed.
func (s *JSONLServer[S, R]) ListenAndServe() error {
	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return ErrJSONLServerClosed
	}

	// If the activeStreamers is not initialized initialize here.
	if s.activeStreamers == nil {
		s.activeStreamers = make(map[int]*JSONLStreamer[S, R])
	}
	s.mutex.Unlock()

	// This listener is closed by the Close method.
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	s.listener = listener

	s.mutex.Lock()
	if s.closed {
		s.mutex.Unlock()
		return ErrJSONLServerClosed
	}
	s.mutex.Unlock()

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mutex.Lock()
			if s.closed {
				s.mutex.Unlock()
				return ErrJSONLServerClosed
			}
			s.mutex.Unlock()

			return err
		}

		s.mutex.Lock()
		// We assume this is always TCPConn because we always use TCP.
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
//
// TODO: We need to get a context to give it a timeout for closing the streams.
func (s *JSONLServer[S, R]) Shutdown(ctx context.Context) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.closed {
		return nil
	}

	if s.listener != nil {
		_ = s.listener.Close()
	}

	for _, streamer := range s.activeStreamers {
		s.removeStreamer(streamer)
	}

	s.closed = true

	// Waits until all of the streamers are closed.
	s.closeWG.Wait()
	return nil
}

// handleConnection invokes the handler with the newly created JSONLStreamer,
// then it closes it.
func (s *JSONLServer[S, R]) handleConnection(streamer *JSONLStreamer[S, R]) {
	s.closeWG.Add(1)
	defer func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()

		s.removeStreamer(streamer)
	}()

	// The first line agent sends should be the AuthRequest.
	var agentAuthRequest api.AuthRequest

	if err := streamer.conn.SetReadDeadline(time.Now().Add(s.TCPDeadline)); err != nil {
		log.Printf("Initial handshake failed on set connection deadline: %v.\n", err)
		return
	}
	if err := streamer.conn.SetReadBuffer(s.TCPBufferLength); err != nil {
		log.Printf("Initial handshake failed on set connection buffer: %v.\n", err)
		return
	}
	if err := streamer.decoder.Decode(&agentAuthRequest); err != nil {
		log.Printf("Initial handshake failed on decode AuthRequest: %v.\n", err)
		return
	}

	// Get the authentification response from the auth handler.
	var agentInfo *AgentInfo
	var agentAuthResponse api.AuthResponse
	if s.authHandler != nil {
		agentAuthResponse = s.authHandler(agentAuthRequest)
	}

	// Send the api.AuthResponse to the agent.
	if err := streamer.encoder.Encode(agentAuthResponse); err != nil {
		log.Printf("Initial handshake failed on decode AuthResponse: %v.\n", err)
	}

	// If the auth handler fails then close the connection.
	if !agentAuthResponse.Authenticated {
		log.Printf("Agent authentification failed on request from: %v.\n", streamer.conn.RemoteAddr())
		return
	}

	log.Printf("Agent authentification succeeded from %v.\n", streamer.conn.RemoteAddr())

	// Then call the handler if it is set.
	if s.streamHandler != nil {
		s.streamHandler(agentInfo, streamer)
	}
}

// removeStreamer closes the streamer's connection, cancels it context, removes
// it from the active streamers set, and releases the close waiting group.
// This method is not protected by the mutex.
func (s *JSONLServer[S, R]) removeStreamer(streamer *JSONLStreamer[S, R]) {
	_ = streamer.conn.Close()
	// Only call Done if streamer is still in the map (not already removed)
	if _, exists := s.activeStreamers[streamer.id]; exists {
		delete(s.activeStreamers, streamer.id)
		s.closeWG.Done()
	}
}

// JSONLStreamer is the streamer struct that is returned on the handle function.
type JSONLStreamer[S, R any] struct {
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
	server *JSONLServer[S, R]
}

// newJSONLStreamer creates a new JSONLStreamer from a connection.
func newJSONLStreamer[S, R any](id int, conn *net.TCPConn, server *JSONLServer[S, R]) *JSONLStreamer[S, R] {
	ctx, cancel := context.WithCancel(context.Background())
	return &JSONLStreamer[S, R]{
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
func (s *JSONLStreamer[S, R]) Context() context.Context {
	return s.ctx
}

// ID returns the connection id of the streamer.
func (s *JSONLStreamer[S, R]) ID() int {
	return s.id
}

// Send sends the given element to the tcp encoded as a JSON. If the streamer is
// closed the Send method unblocks.
func (s *JSONLStreamer[S, R]) Send(e *S) error {
	if err := s.conn.SetWriteDeadline(time.Now().Add(s.server.TCPDeadline)); err != nil {
		return err
	}
	if err := s.conn.SetWriteBuffer(s.server.TCPBufferLength); err != nil {
		return err
	}
	if err := s.encoder.Encode(e); err != nil {
		return err
	}
	return nil
}

// Receive sends the given element to the tcp encoded as a JSON. If the streamer
// is closed the Receive method unblocks.
func (s *JSONLStreamer[S, R]) Receive() (*R, error) {
	var e *R

	if err := s.conn.SetReadDeadline(time.Now().Add(s.server.TCPDeadline)); err != nil {
		return nil, err
	}
	if err := s.conn.SetReadBuffer(s.server.TCPBufferLength); err != nil {
		return nil, err
	}
	if err := s.decoder.Decode(e); err != nil {
		return nil, err
	}
	return e, nil
}

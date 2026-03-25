// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

// Package servers provides the AgentServer for bidirectional PD/FIE
// communication with agents, and the APIServer for streaming
// ForwardingInfoElements to HTTP clients.
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

// agentKeepalivePeriod is the interval between TCP keepalive probes
// for agent connections.
const agentKeepalivePeriod = 30 * time.Second

// AgentAuthStatus contains the agent's identity and connection info after
// a successful authentication handshake.
type AgentAuthStatus struct {
	AgentID       string
	RemoteAddress net.Addr
}

// AgentHandleFunc is called in a separate goroutine for each authenticated
// agent connection.
type AgentHandleFunc func(status *AgentAuthStatus, s *AgentStream)

// AuthHandleFunc handles authentication for incoming agent connections.
// It receives the agent's AuthRequest and returns an AuthResponse.
// If Authenticated is false, the connection is closed.
type AuthHandleFunc func(req api.AuthRequest) api.AuthResponse

// AgentServerConfig configures the TCP listener and handlers for the AgentServer.
type AgentServerConfig struct {
	// Address is the TCP listening address in the form "host:port".
	Address string
	// HandshakeTimeout is the deadline for the initial authentication exchange.
	HandshakeTimeout time.Duration
	BufferLength     int
	AgentHandler     AgentHandleFunc
	AuthHandler      AuthHandleFunc
}

// AgentServer is the TCP server that handles bidirectional PD/FIE communication
// with connected agents using newline-delimited JSON.
type AgentServer struct {
	config   *AgentServerConfig
	shutdown atomic.Bool
	mutex    sync.Mutex
	// connections tracks all active agent connections for shutdown.
	connections  map[int]*AgentStream
	listener     net.Listener
	nextStreamID int
	wg           sync.WaitGroup
}

// NewAgentServer creates a new AgentServer from the provided config.
// Address and BufferLength are validated by Config.Validate() in orchestrator.go.
// Handler validation remains here as handlers are not part of Config.
func NewAgentServer(config *AgentServerConfig) (*AgentServer, error) {
	if config.AuthHandler == nil || config.AgentHandler == nil {
		return nil, fmt.Errorf("handlers cannot be nil")
	}

	return &AgentServer{
		config:      config,
		connections: make(map[int]*AgentStream),
	}, nil
}

// ListenAndServe binds to the configured address and accepts incoming agent
// connections. Returns ErrServerShutdown if Shutdown has been called.
func (s *AgentServer) ListenAndServe() error {
	if s.shutdown.Load() {
		return ErrServerShutdown
	}

	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.listener = listener
	s.mutex.Unlock()

	if s.shutdown.Load() {
		return ErrServerShutdown
	}

	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.shutdown.Load() {
				return ErrServerShutdown
			}
			return err
		}

		s.mutex.Lock()
		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			s.mutex.Unlock()
			return fmt.Errorf("expected TCP connection, got %T", conn)
		}
		stream, err := newAgentStream(s.nextStreamID, tcpConn, s)
		if err != nil {
			s.mutex.Unlock()
			log.Printf("Failed to configure agent connection: %v", err)
			_ = tcpConn.Close()
			continue
		}
		s.connections[s.nextStreamID] = stream
		s.nextStreamID++
		s.wg.Add(1)
		s.mutex.Unlock()

		go s.handleAgent(stream)
	}
}

// Shutdown closes the listener and all open connections, cancelling the context
// of all active AgentStreams. Multiple calls are a no-op and return nil.
func (s *AgentServer) Shutdown(timeout time.Duration) error {
	if s.shutdown.Swap(true) {
		return nil
	}

	exitCtx, exitCancel := context.WithTimeout(context.Background(), timeout)
	defer exitCancel()

	s.mutex.Lock()
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	for _, stream := range s.connections {
		s.removeConnection(stream)
	}
	s.mutex.Unlock()

	// Wait for active goroutines to finish, but respect the deadline.
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-exitCtx.Done():
		return exitCtx.Err()
	}
}

// handleAgent performs the authentication handshake and invokes the
// AgentHandler for authenticated agents.
func (s *AgentServer) handleAgent(stream *AgentStream) {
	defer func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		s.removeConnection(stream)
	}()

	status, err := s.handshake(stream)
	if err != nil {
		log.Printf("Handshake failed: %v", err)
		return
	}

	log.Printf("Agent authenticated from %v", status.RemoteAddress)
	s.config.AgentHandler(status, stream)
}

func (s *AgentServer) handshake(stream *AgentStream) (*AgentAuthStatus, error) {
	authReq, err := receive[api.AuthRequest](stream.conn, stream.decoder, s.config.HandshakeTimeout)
	if err != nil {
		return nil, fmt.Errorf("could not receive auth request: %w", err)
	}

	authResp := s.config.AuthHandler(*authReq)
	if err := send(stream.conn, stream.encoder, s.config.HandshakeTimeout, &authResp); err != nil {
		return nil, fmt.Errorf("could not send auth response: %w", err)
	}

	if !authResp.Authenticated {
		return nil, fmt.Errorf("agent not authenticated: %s", authResp.Message)
	}

	return &AgentAuthStatus{
		AgentID:       authReq.AgentID,
		RemoteAddress: stream.conn.RemoteAddr(),
	}, nil
}

// removeConnection cleans up a connection when the agent disconnects or the
// server shuts down.
// Must be called with s.mutex held to avoid races with concurrent connections.
// Must only be called once per connection to avoid double-counting the WaitGroup.
func (s *AgentServer) removeConnection(stream *AgentStream) {
	if _, ok := s.connections[stream.id]; !ok {
		return
	}
	stream.cancel()
	_ = stream.conn.Close()
	delete(s.connections, stream.id)
	s.wg.Done()
}

// AgentStream is the bidirectional JSON communication channel with a single
// connected agent. Use SendPD to issue ProbingDirectives and ReceiveFIE to
// collect ForwardingInfoElements.
type AgentStream struct {
	id      int
	ctx     context.Context
	cancel  context.CancelFunc
	conn    *net.TCPConn
	encoder *json.Encoder
	decoder *json.Decoder
	server  *AgentServer
}

func newAgentStream(id int, conn *net.TCPConn, server *AgentServer) (*AgentStream, error) {
	if err := conn.SetKeepAlive(true); err != nil {
		return nil, fmt.Errorf("failed to enable keepalive: %w", err)
	}
	if err := conn.SetKeepAlivePeriod(agentKeepalivePeriod); err != nil {
		return nil, fmt.Errorf("failed to set keepalive period: %w", err)
	}
	if err := conn.SetReadBuffer(server.config.BufferLength); err != nil {
		return nil, fmt.Errorf("failed to set read buffer: %w", err)
	}
	if err := conn.SetWriteBuffer(server.config.BufferLength); err != nil {
		return nil, fmt.Errorf("failed to set write buffer: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &AgentStream{
		id:      id,
		conn:    conn,
		ctx:     ctx,
		cancel:  cancel,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
		server:  server,
	}, nil
}

func (s *AgentStream) Context() context.Context {
	return s.ctx
}

func (s *AgentStream) SendPD(e *api.ProbingDirective) error {
	return send(s.conn, s.encoder, 0, e)
}

func (s *AgentStream) ReceiveFIE() (*api.ForwardingInfoElement, error) {
	return receive[api.ForwardingInfoElement](s.conn, s.decoder, 0)
}

func send[E any](conn *net.TCPConn, encoder *json.Encoder, timeout time.Duration, e *E) error {
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return fmt.Errorf("send failed: cannot set write deadline: %w", err)
		}
	}
	if err := encoder.Encode(e); err != nil {
		return fmt.Errorf("send failed: cannot encode: %w", err)
	}
	return nil
}

func receive[E any](conn *net.TCPConn, decoder *json.Decoder, timeout time.Duration) (*E, error) {
	var e E
	if timeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return nil, fmt.Errorf("receive failed: cannot set read deadline: %w", err)
		}
	}
	if err := decoder.Decode(&e); err != nil {
		return nil, fmt.Errorf("receive failed: cannot decode: %w", err)
	}
	return &e, nil
}

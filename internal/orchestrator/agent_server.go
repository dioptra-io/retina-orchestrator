// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// agentKeepalivePeriod is the interval between TCP keepalive probes
// for agent connections.
const agentKeepalivePeriod = 30 * time.Second

type agentAuthStatus struct {
	agentID       string
	remoteAddress net.Addr
}

// agentHandleFunc is called in a separate goroutine for each authenticated
// agent connection.
type agentHandleFunc func(status *agentAuthStatus, s *agentStream)

// authHandleFunc handles agent authentication. If Authenticated is false, the connection is closed.
type authHandleFunc func(req api.AuthRequest) api.AuthResponse

type agentServerConfig struct {
	// address is the TCP listening address in the form "host:port".
	address string
	// handshakeTimeout is the deadline for the initial authentication exchange.
	handshakeTimeout time.Duration
	bufferLength     int
	agentHandler     agentHandleFunc
	authHandler      authHandleFunc
	logger           *slog.Logger
}

// agentServer handles bidirectional PD/FIE communication with agents over newline-delimited JSON.
type agentServer struct {
	config   *agentServerConfig
	logger   *slog.Logger
	shutdown atomic.Bool
	mutex    sync.Mutex
	// connections tracks all active agent connections for shutdown.
	connections  map[int]*agentStream
	listener     net.Listener
	nextStreamID int
	wg           sync.WaitGroup
}

func newAgentServer(config *agentServerConfig) (*agentServer, error) {
	if config.authHandler == nil || config.agentHandler == nil {
		return nil, fmt.Errorf("handlers cannot be nil")
	}
	if config.logger == nil {
		config.logger = slog.Default()
	}

	return &agentServer{
		config:      config,
		logger:      config.logger,
		connections: make(map[int]*agentStream),
	}, nil
}

// listenAndServe accepts incoming agent connections. Returns ErrServerShutdown if close has been called.
func (s *agentServer) listenAndServe() error {
	if s.shutdown.Load() {
		return ErrServerShutdown
	}

	listener, err := net.Listen("tcp", s.config.address)
	if err != nil {
		return err
	}
	s.mutex.Lock()
	s.listener = listener
	s.mutex.Unlock()

	s.logger.Info("Agent server listening", slog.String("addr", s.config.address))

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
			s.logger.Error("Failed to configure agent connection",
				slog.String("remote_addr", conn.RemoteAddr().String()),
				slog.Any("err", err))
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

// close closes the listener and all open connections. Multiple calls are a no-op.
func (s *agentServer) close(timeout time.Duration) error {
	if s.shutdown.Swap(true) {
		return nil
	}

	s.logger.Info("Shutting down agent server")

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
		s.logger.Warn("Agent server shutdown timed out", slog.Duration("timeout", timeout))
		return exitCtx.Err()
	}
}

func (s *agentServer) handleAgent(stream *agentStream) {
	defer s.wg.Done()
	defer func() {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		s.removeConnection(stream)
	}()

	status, err := s.handshake(stream)
	if err != nil {
		s.logger.Warn("Handshake failed",
			slog.String("remote_addr", stream.conn.RemoteAddr().String()),
			slog.Any("err", err))
		return
	}

	s.logger.Info("Agent authenticated",
		slog.String("agent_id", status.agentID),
		slog.String("remote_addr", status.remoteAddress.String()))
	s.config.agentHandler(status, stream)
}

func (s *agentServer) handshake(stream *agentStream) (*agentAuthStatus, error) {
	authReq, err := receive[api.AuthRequest](stream.conn, stream.decoder, s.config.handshakeTimeout)
	if err != nil {
		return nil, fmt.Errorf("could not receive auth request: %w", err)
	}

	authResp := s.config.authHandler(*authReq)
	if err := send(stream.conn, stream.encoder, s.config.handshakeTimeout, &authResp); err != nil {
		return nil, fmt.Errorf("could not send auth response: %w", err)
	}

	if !authResp.Authenticated {
		return nil, fmt.Errorf("agent not authenticated: %s", authResp.Message)
	}

	// Clear the handshake deadline so subsequent reads/writes have no timeout.
	if err := stream.conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("could not clear deadline: %w", err)
	}

	return &agentAuthStatus{
		agentID:       authReq.AgentID,
		remoteAddress: stream.conn.RemoteAddr(),
	}, nil
}

// removeConnection must be called with s.mutex held.
func (s *agentServer) removeConnection(stream *agentStream) {
	if _, ok := s.connections[stream.id]; !ok {
		return
	}
	stream.cancel()
	_ = stream.conn.Close()
	delete(s.connections, stream.id)
}

type agentStream struct {
	id      int
	ctx     context.Context
	cancel  context.CancelFunc
	conn    *net.TCPConn
	encoder *json.Encoder
	decoder *json.Decoder
	server  *agentServer
}

func newAgentStream(id int, conn *net.TCPConn, server *agentServer) (*agentStream, error) {
	if err := conn.SetKeepAlive(true); err != nil {
		return nil, fmt.Errorf("failed to enable keepalive: %w", err)
	}
	if err := conn.SetKeepAlivePeriod(agentKeepalivePeriod); err != nil {
		return nil, fmt.Errorf("failed to set keepalive period: %w", err)
	}
	if err := conn.SetReadBuffer(server.config.bufferLength); err != nil {
		return nil, fmt.Errorf("failed to set read buffer: %w", err)
	}
	if err := conn.SetWriteBuffer(server.config.bufferLength); err != nil {
		return nil, fmt.Errorf("failed to set write buffer: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118
	return &agentStream{
		id:      id,
		conn:    conn,
		ctx:     ctx,
		cancel:  cancel,
		encoder: json.NewEncoder(conn),
		decoder: json.NewDecoder(conn),
		server:  server,
	}, nil
}

func (s *agentStream) context() context.Context {
	return s.ctx
}

func (s *agentStream) sendPD(e *api.ProbingDirective) error {
	return send(s.conn, s.encoder, 0, e)
}

func (s *agentStream) receiveFIE() (*api.ForwardingInfoElement, error) {
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

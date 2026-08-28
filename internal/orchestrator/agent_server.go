// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dioptra-io/retina-commons/framing"
	"github.com/dioptra-io/retina-commons/model"
	wire "github.com/dioptra-io/retina-commons/wire/v2"
)

// agentKeepalivePeriod is the interval between TCP keepalive probes
// for agent connections.
const agentKeepalivePeriod = 10 * time.Second

// agentSendTimeout is the deadline for sending a probing directive to an agent.
// Without a deadline, a dead agent will block the sender goroutine indefinitely
// once the TCP send buffer fills up
const agentSendTimeout = 5 * time.Second

type agentAuthStatus struct {
	agentID       string
	remoteAddress net.Addr
}

// agentHandleFunc is called in a separate goroutine for each authenticated
// agent connection.
type agentHandleFunc func(status *agentAuthStatus, s *agentStream)

// authHandleFunc handles agent authentication. If Authenticated is false, the connection is closed.
// wire.AuthRequest/AuthResponse are used directly (no model wrapper) since
// this type is only ever touched once per connection, at the handshake —
// not threaded deeply the way ProbingDirective/ForwardingInfoElement are.
// Pointers, not values: these generated proto messages embed a sync.Mutex
// (via protoimpl.MessageState) that copylocks flags if copied by value.
type authHandleFunc func(req *wire.AuthRequest) *wire.AuthResponse

type agentServerConfig struct {
	// address is the TCP listening address in the form "host:port".
	address string
	// handshakeTimeout is the deadline for the initial authentication exchange.
	handshakeTimeout time.Duration
	bufferLength     int
	agentHandler     agentHandleFunc
	authHandler      authHandleFunc
}

// agentServer handles bidirectional PD/FIE communication with agents over
// length-prefixed protobuf (see retina-commons/framing).
type agentServer struct {
	config   *agentServerConfig
	logger   *slog.Logger
	metrics  *Metrics
	shutdown atomic.Bool
	mutex    sync.Mutex
	// connections tracks all active agent connections for shutdown.
	connections  map[int]*agentStream
	listener     net.Listener
	nextStreamID int
	wg           sync.WaitGroup
}

func newAgentServer(config *agentServerConfig, logger *slog.Logger, metrics *Metrics) (*agentServer, error) {
	if config.authHandler == nil || config.agentHandler == nil {
		return nil, fmt.Errorf("handlers cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &agentServer{
		config:      config,
		logger:      logger,
		metrics:     metrics,
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
	s.metrics.AgentsConnected.Inc()
	defer func() {
		s.metrics.AgentDisconnectionsTotal.WithLabelValues(status.agentID).Inc()
		s.metrics.AgentsConnected.Dec()
	}()

	s.logger.Info("Agent authenticated",
		slog.String("agent_id", status.agentID),
		slog.String("remote_addr", status.remoteAddress.String()))
	s.config.agentHandler(status, stream)
}

func (s *agentServer) handshake(stream *agentStream) (*agentAuthStatus, error) {
	var authReq wire.AuthRequest
	if err := framing.Receive(stream.conn, s.config.handshakeTimeout, &authReq); err != nil {
		return nil, fmt.Errorf("could not receive auth request: %w", err)
	}

	authResp := s.config.authHandler(&authReq)
	if err := framing.Send(stream.conn, s.config.handshakeTimeout, authResp); err != nil {
		return nil, fmt.Errorf("could not send auth response: %w", err)
	}

	if !authResp.Authenticated {
		s.metrics.AuthFailuresTotal.Inc()
		return nil, fmt.Errorf("agent not authenticated: %s", authResp.Message)
	}

	// Clear the handshake deadline so subsequent reads/writes have no timeout.
	if err := stream.conn.SetDeadline(time.Time{}); err != nil {
		return nil, fmt.Errorf("could not clear deadline: %w", err)
	}

	return &agentAuthStatus{
		agentID:       authReq.AgentId, // wire.AuthRequest field — see authHandleFunc doc
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
	id     int
	ctx    context.Context
	cancel context.CancelFunc
	conn   *net.TCPConn
	server *agentServer
}

func newAgentStream(id int, conn *net.TCPConn, server *agentServer) (*agentStream, error) {
	if err := conn.SetKeepAlive(true); err != nil {
		return nil, fmt.Errorf("failed to enable keepalive: %w", err)
	}
	if err := conn.SetKeepAlivePeriod(agentKeepalivePeriod); err != nil {
		return nil, fmt.Errorf("failed to set keepalive period: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118
	return &agentStream{
		id:     id,
		conn:   conn,
		ctx:    ctx,
		cancel: cancel,
		server: server,
	}, nil
}

func (s *agentStream) context() context.Context {
	return s.ctx
}

// sendPD converts pd to its wire representation and sends it, prefixed
// with its length (see retina-commons/framing).
func (s *agentStream) sendPD(pd *model.ProbingDirective) error {
	wirePD, err := pd.ToProto()
	if err != nil {
		return fmt.Errorf("send failed: cannot convert PD to wire format: %w", err)
	}
	if err := framing.Send(s.conn, agentSendTimeout, wirePD); err != nil {
		return fmt.Errorf("send failed: %w", err)
	}
	return nil
}

// receiveFIE reads a length-prefixed frame and converts it from its wire
// representation (see retina-commons/framing). No read deadline is set
// here — the FIE stream from an already-authenticated agent is expected
// to be long-lived and idle between probes, unlike the bounded handshake.
func (s *agentStream) receiveFIE() (*model.ForwardingInfoElement, error) {
	var wireFIE wire.ForwardingInfoElement
	if err := framing.Receive(s.conn, 0, &wireFIE); err != nil {
		return nil, fmt.Errorf("receive failed: %w", err)
	}
	fie, err := model.ForwardingInfoElementFromProto(&wireFIE)
	if err != nil {
		return nil, fmt.Errorf("receive failed: cannot convert FIE from wire format: %w", err)
	}
	return &fie, nil
}

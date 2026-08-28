// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"net"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/framing"
	"github.com/dioptra-io/retina-commons/model"
	wire "github.com/dioptra-io/retina-commons/wire/v2"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Remaining coverage gaps are unreachable without refactoring *net.TCPConn to
// an interface, as syscall-level errors cannot be injected on a real connection:
//   - newAgentStream: SetKeepAlive, SetKeepAlivePeriod, SetReadBuffer, SetWriteBuffer
//   - handshake: send error after auth recv — kernel buffers the write on loopback
//     even when the client has already closed. SetDeadline error after successful
//     auth — both require syscall-level injection unavailable on loopback.
//   - listenAndServe: non-TCP type assertion, newAgentStream error continue,
//     second shutdown race after listener setup — all require syscall-level
//     error injection on *net.TCPConn.
//
// send[E]/receive[E]'s own round-trip, timeout, and decode-error behavior is
// covered directly in retina-commons/framing's test suite, not duplicated
// here — this file only exercises them through the orchestrator's actual
// call sites (handshake, sendPD, receiveFIE).

// -- helpers ------------------------------------------------------------------

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot get free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// newTCPPair is a shared helper (also used by orchestrator_test.go, same
// package) for tests that need a real connected TCP pair rather than going
// through the full agentServer accept/handshake flow.
func newTCPPair(t *testing.T) (client, server *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot create listener: %v", err)
	}
	defer func() { _ = ln.Close() }()

	accepted := make(chan *net.TCPConn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- conn.(*net.TCPConn)
	}()

	dial, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	return dial.(*net.TCPConn), <-accepted
}

func newTestAgentServer(t *testing.T, auth authHandleFunc, agent agentHandleFunc) (*agentServer, string) {
	t.Helper()
	addr := freeAddr(t)
	s, err := newAgentServer(&agentServerConfig{
		address:          addr,
		handshakeTimeout: time.Second,
		bufferLength:     4096,
		authHandler:      auth,
		agentHandler:     agent,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return s, addr
}

var allowAll authHandleFunc = func(_ *wire.AuthRequest) *wire.AuthResponse {
	return &wire.AuthResponse{Authenticated: true}
}

var denyAll authHandleFunc = func(_ *wire.AuthRequest) *wire.AuthResponse {
	return &wire.AuthResponse{Authenticated: false, Message: "denied"}
}

var nopAgentHandler agentHandleFunc = func(_ *agentAuthStatus, _ *agentStream) {}

func startAgentServer(t *testing.T, s *agentServer) {
	t.Helper()
	go func() { _ = s.listenAndServe() }()
	time.Sleep(20 * time.Millisecond)
}

// doHandshake sends an AuthRequest and returns the AuthResponse. Unlike the
// old JSON-based version, there's no decoder to thread through to callers —
// framing.Receive reads directly off conn each call, with no buffering state
// to preserve across calls. Takes/returns pointers, not values — these
// generated proto messages embed a sync.Mutex that copylocks flags if
// copied by value.
func doHandshake(t *testing.T, conn net.Conn, req *wire.AuthRequest) *wire.AuthResponse {
	t.Helper()
	if err := framing.Send(conn, 0, req); err != nil {
		t.Fatalf("cannot send auth request: %v", err)
	}
	var resp wire.AuthResponse
	if err := framing.Receive(conn, 0, &resp); err != nil {
		t.Fatalf("cannot decode auth response: %v", err)
	}
	return &resp
}

// validPD returns a ProbingDirective with the fields model.ProbingDirective.
// ToProto() requires (DestinationAddress) filled in — the old api.ProbingDirective
// had no such validation, so pre-migration tests could get away with a bare
// literal; this can't.
func validPD(id uint64) *model.ProbingDirective {
	return &model.ProbingDirective{
		ProbingDirectiveID: id,
		DestinationAddress: net.ParseIP("192.0.2.1"),
	}
}

// validWireFIE returns a wire.ForwardingInfoElement with the fields
// model.ForwardingInfoElementFromProto requires (Agent, SourceAddress,
// DestinationAddress, ProductionTimestamp) — same reasoning as validPD.
func validWireFIE(pdID uint64) *wire.ForwardingInfoElement {
	return &wire.ForwardingInfoElement{
		Agent:               &wire.Agent{AgentId: "a1"},
		ProbingDirectiveId:  pdID,
		SourceAddress:       "192.0.2.2",
		DestinationAddress:  "192.0.2.1",
		ProductionTimestamp: timestamppb.New(time.Now()),
	}
}

// -- newAgentServer -----------------------------------------------------------

func TestNewAgentServer_NilAuthHandler(t *testing.T) {
	t.Parallel()
	_, err := newAgentServer(&agentServerConfig{agentHandler: nopAgentHandler}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for nil authHandler, got nil")
	}
}

func TestNewAgentServer_NilAgentHandler(t *testing.T) {
	t.Parallel()
	_, err := newAgentServer(&agentServerConfig{authHandler: allowAll}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for nil agentHandler, got nil")
	}
}

func TestNewAgentServer_Valid(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)
	if s == nil {
		t.Fatal("expected non-nil server")
	}
}

func TestNewAgentServer_NilLogger(t *testing.T) {
	t.Parallel()
	s, err := newAgentServer(&agentServerConfig{
		address:          "127.0.0.1:0",
		handshakeTimeout: time.Second,
		bufferLength:     4096,
		authHandler:      allowAll,
		agentHandler:     nopAgentHandler,
	}, nil, testMetrics())
	if err != nil {
		t.Fatalf("unexpected error with nil logger: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil server")
	}
}

// -- listenAndServe -----------------------------------------------------------

func TestListenAndServe_BindError(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot bind: %v", err)
	}
	defer func() { _ = ln.Close() }()

	s, err := newAgentServer(&agentServerConfig{
		address:      ln.Addr().String(),
		bufferLength: 4096,
		authHandler:  allowAll,
		agentHandler: nopAgentHandler,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.listenAndServe(); err == nil || err == ErrServerShutdown {
		t.Fatalf("expected bind error, got %v", err)
	}
}

func TestListenAndServe_ShutdownBeforeListen(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)
	_ = s.close(time.Second)
	if err := s.listenAndServe(); err != ErrServerShutdown {
		t.Fatalf("expected ErrServerShutdown, got %v", err)
	}
}

func TestListenAndServe_ReturnsErrServerShutdownAfterClose(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)

	done := make(chan error, 1)
	go func() { done <- s.listenAndServe() }()
	time.Sleep(20 * time.Millisecond)
	_ = s.close(time.Second)

	select {
	case err := <-done:
		if err != ErrServerShutdown {
			t.Fatalf("expected ErrServerShutdown, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listenAndServe did not return after close")
	}
}

func TestListenAndServe_AcceptError(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)

	done := make(chan error, 1)
	go func() { done <- s.listenAndServe() }()
	time.Sleep(20 * time.Millisecond)

	s.mutex.Lock()
	_ = s.listener.Close()
	s.mutex.Unlock()

	select {
	case err := <-done:
		if err == nil || err == ErrServerShutdown {
			t.Fatalf("expected raw accept error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listenAndServe did not return after listener was closed")
	}
}

// -- close --------------------------------------------------------------------

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	s, _ := newTestAgentServer(t, allowAll, nopAgentHandler)
	startAgentServer(t, s)

	if err := s.close(time.Second); err != nil {
		t.Fatalf("first close: unexpected error: %v", err)
	}
	if err := s.close(time.Second); err != nil {
		t.Fatalf("second close: unexpected error: %v", err)
	}
}

func TestClose_Timeout(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	started := make(chan struct{})
	s, addr := newTestAgentServer(t, allowAll, func(_ *agentAuthStatus, _ *agentStream) {
		close(started)
		<-block
	})
	startAgentServer(t, s)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	doHandshake(t, conn, &wire.AuthRequest{AgentId: "a1"})
	<-started

	err = s.close(time.Millisecond)
	close(block)
	if err == nil {
		t.Fatal("expected timeout error from close, got nil")
	}
}

// -- handshake ----------------------------------------------------------------

func TestHandshake_Success(t *testing.T) {
	t.Parallel()
	statusCh := make(chan *agentAuthStatus, 1)
	s, addr := newTestAgentServer(t, allowAll, func(status *agentAuthStatus, _ *agentStream) {
		statusCh <- status
	})
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	resp := doHandshake(t, conn, &wire.AuthRequest{AgentId: "agent-1", Secret: "s"})
	if !resp.Authenticated {
		t.Fatalf("expected authenticated, got: %s", resp.Message)
	}
	select {
	case status := <-statusCh:
		if status.agentID != "agent-1" {
			t.Errorf("expected agentID agent-1, got %s", status.agentID)
		}
	case <-time.After(time.Second):
		t.Fatal("agent handler not called")
	}
}

func TestHandshake_Failure(t *testing.T) {
	t.Parallel()
	s, addr := newTestAgentServer(t, denyAll, nopAgentHandler)
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	resp := doHandshake(t, conn, &wire.AuthRequest{AgentId: "bad", Secret: "wrong"})
	if resp.Authenticated {
		t.Fatal("expected not authenticated")
	}
	if resp.Message != "denied" {
		t.Errorf("expected message 'denied', got %q", resp.Message)
	}
}

func TestHandshake_ConnectionClosedBeforeAuth(t *testing.T) {
	t.Parallel()
	s, addr := newTestAgentServer(t, allowAll, nopAgentHandler)
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	_ = conn.Close()
	time.Sleep(50 * time.Millisecond)
}

func TestHandshake_DeadlineClearedAfterAuth(t *testing.T) {
	t.Parallel()

	handshakeTimeout := 100 * time.Millisecond
	addr := freeAddr(t)
	fieCh := make(chan *model.ForwardingInfoElement, 1)

	s, err := newAgentServer(&agentServerConfig{
		address:          addr,
		handshakeTimeout: handshakeTimeout,
		bufferLength:     4096,
		authHandler:      allowAll,
		agentHandler: func(_ *agentAuthStatus, stream *agentStream) {
			if err := stream.sendPD(validPD(1)); err != nil {
				return
			}
			fie, err := stream.receiveFIE()
			if err != nil {
				return
			}
			fieCh <- fie
		},
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	doHandshake(t, conn, &wire.AuthRequest{AgentId: "a1"})

	// Read the PD immediately — the server sends it right after auth.
	var wirePD wire.ProbingDirective
	if err := framing.Receive(conn, 0, &wirePD); err != nil {
		t.Fatalf("cannot decode PD: %v", err)
	}

	// Wait longer than handshakeTimeout before sending the FIE — if the
	// deadline is not cleared, the server-side connection will have timed
	// out by now and the send below will fail.
	time.Sleep(handshakeTimeout * 3)

	if err := framing.Send(conn, 0, validWireFIE(1)); err != nil {
		t.Fatalf("connection timed out after handshake — deadline not cleared: %v", err)
	}

	select {
	case got := <-fieCh:
		if got.ProbingDirectiveID != 1 {
			t.Errorf("expected FIE ID 1, got %d", got.ProbingDirectiveID)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive FIE — connection may have timed out")
	}
}

// -- agentStream --------------------------------------------------------------

func TestAgentStream_Context(t *testing.T) {
	t.Parallel()
	ctxCh := make(chan bool, 1)
	s, addr := newTestAgentServer(t, allowAll, func(_ *agentAuthStatus, stream *agentStream) {
		ctxCh <- stream.context() != nil
	})
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	doHandshake(t, conn, &wire.AuthRequest{AgentId: "a1"})

	select {
	case ok := <-ctxCh:
		if !ok {
			t.Error("expected non-nil context")
		}
	case <-time.After(time.Second):
		t.Fatal("agent handler not called")
	}
}

func TestAgentStream_SendPDReceiveFIE(t *testing.T) {
	t.Parallel()
	fieCh := make(chan *model.ForwardingInfoElement, 1)
	s, addr := newTestAgentServer(t, allowAll, func(_ *agentAuthStatus, stream *agentStream) {
		if err := stream.sendPD(validPD(42)); err != nil {
			return
		}
		fie, err := stream.receiveFIE()
		if err != nil {
			return
		}
		fieCh <- fie
	})
	startAgentServer(t, s)
	defer func() { _ = s.close(time.Second) }()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("cannot dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	doHandshake(t, conn, &wire.AuthRequest{AgentId: "a1"})

	var wirePD wire.ProbingDirective
	if err := framing.Receive(conn, 0, &wirePD); err != nil {
		t.Fatalf("cannot decode PD: %v", err)
	}
	if wirePD.ProbingDirectiveId != 42 {
		t.Errorf("expected PD ID 42, got %d", wirePD.ProbingDirectiveId)
	}

	if err := framing.Send(conn, 0, validWireFIE(42)); err != nil {
		t.Fatalf("cannot encode FIE: %v", err)
	}

	select {
	case got := <-fieCh:
		if got.ProbingDirectiveID != 42 {
			t.Errorf("expected FIE ID 42, got %d", got.ProbingDirectiveID)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive FIE in time")
	}
}

// -- sendPD / receiveFIE conversion errors -------------------------------------

// TestAgentStream_SendPD_ToProtoError covers sendPD's own conversion-error
// branch (pd.ToProto() failing) as distinct from a network-level send
// failure — TestAgentHandler_SendPDError already covers the latter.
func TestAgentStream_SendPD_ToProtoError(t *testing.T) {
	t.Parallel()
	client, server := newTCPPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	stream := &agentStream{conn: server}

	// Missing DestinationAddress — pd.ToProto() rejects this before any
	// network I/O happens.
	if err := stream.sendPD(&model.ProbingDirective{ProbingDirectiveID: 1}); err == nil {
		t.Fatal("expected error for PD with missing DestinationAddress, got nil")
	}
}

// TestAgentStream_ReceiveFIE_FromProtoError covers receiveFIE's own
// conversion-error branch (ForwardingInfoElementFromProto failing on a
// well-framed but semantically invalid message) as distinct from a
// framing-level receive failure.
func TestAgentStream_ReceiveFIE_FromProtoError(t *testing.T) {
	t.Parallel()
	client, server := newTCPPair(t)
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	stream := &agentStream{conn: server}

	// Well-framed but missing required fields (Agent, SourceAddress,
	// DestinationAddress, ProductionTimestamp) — framing.Receive succeeds,
	// but the model conversion rejects it.
	go func() {
		_ = framing.Send(client, 0, &wire.ForwardingInfoElement{ProbingDirectiveId: 1})
	}()

	if _, err := stream.receiveFIE(); err == nil {
		t.Fatal("expected error for malformed FIE, got nil")
	}
}

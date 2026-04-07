// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// 100% coverage: every branch in newAPIServer, listenAndServe, close,
// handleStream, addClient, removeClient, sendFIE, and context is exercised.

// -- helpers ------------------------------------------------------------------

func newTestAPIServer(t *testing.T, handler fieHandleFunc) (*apiServer, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot get free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	s, err := newAPIServer(&apiServerConfig{
		address:           addr,
		readHeaderTimeout: time.Second,
		fieHandler:        handler,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s.server.Addr = addr
	return s, addr
}

// startHandler starts only the HTTP mux via httptest.NewServer for handler-level
// tests. Does NOT use s.listenAndServe so there is no conflict with s.close.
func startHandler(t *testing.T, s *apiServer) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(s.server.Handler)
	t.Cleanup(func() { ts.Close() })
	return ts
}

// startListening starts s.listenAndServe in a goroutine and waits briefly for
// the listener to be ready.
func startListening(t *testing.T, s *apiServer) {
	t.Helper()
	go func() { _ = s.listenAndServe() }()
	time.Sleep(20 * time.Millisecond)
}

type failWriter struct{}

func (f *failWriter) Write([]byte) (int, error) { return 0, fmt.Errorf("write error") }

type nopFlusher struct{}

func (n nopFlusher) Flush() {}

// nonFlushResponseWriter implements http.ResponseWriter without http.Flusher,
// triggering the "streaming unsupported" branch in handleStream.
type nonFlushResponseWriter struct {
	header http.Header
	body   bytes.Buffer
	code   int
}

func newNonFlushResponseWriter() *nonFlushResponseWriter {
	return &nonFlushResponseWriter{header: make(http.Header)}
}

func (w *nonFlushResponseWriter) Header() http.Header         { return w.header }
func (w *nonFlushResponseWriter) WriteHeader(code int)        { w.code = code }
func (w *nonFlushResponseWriter) Write(b []byte) (int, error) { return w.body.Write(b) }

// -- newAPIServer -------------------------------------------------------------

func TestNewAPIServer_NilHandler(t *testing.T) {
	t.Parallel()
	_, err := newAPIServer(&apiServerConfig{address: "127.0.0.1:0"})
	if err == nil {
		t.Fatal("expected error for nil fieHandler, got nil")
	}
}

func TestNewAPIServer_Valid(t *testing.T) {
	t.Parallel()
	s, _ := newTestAPIServer(t, func(_ *fieClient) {})
	if s == nil {
		t.Fatal("expected non-nil apiServer")
	}
}

// -- listenAndServe -----------------------------------------------------------

func TestAPIServer_ListenAndServe_Close(t *testing.T) {
	t.Parallel()
	s, _ := newTestAPIServer(t, func(_ *fieClient) {})

	done := make(chan error, 1)
	go func() { done <- s.listenAndServe() }()
	time.Sleep(20 * time.Millisecond)

	if err := s.close(time.Second); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("listenAndServe returned unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("listenAndServe did not return after close")
	}
}

func TestAPIServer_ListenAndServe_BindError(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot bind: %v", err)
	}
	defer func() { _ = ln.Close() }()

	s, err := newAPIServer(&apiServerConfig{
		address:           ln.Addr().String(),
		readHeaderTimeout: time.Second,
		fieHandler:        func(_ *fieClient) {},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.listenAndServe(); err == nil {
		t.Fatal("expected bind error, got nil")
	}
}

// -- close --------------------------------------------------------------------

func TestAPIServer_Close_Timeout(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
	})

	s, addr := newTestAPIServer(t, func(_ *fieClient) { <-block })
	startListening(t, s)

	go func() {
		resp, err := http.Get("http://" + addr + "/stream")
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
	time.Sleep(20 * time.Millisecond)

	if err := s.close(time.Millisecond); err == nil {
		t.Fatal("expected timeout error from close, got nil")
	}
}

// -- handleStream -------------------------------------------------------------

func TestHandleStream_SendFIE(t *testing.T) {
	t.Parallel()
	done := make(chan struct{})
	s, _ := newTestAPIServer(t, func(client *fieClient) {
		_ = client.sendFIE(&SequencedFIE{SequenceNumber: 42})
		<-done
	})
	ts := startHandler(t, s)

	resp, err := http.Get(ts.URL + "/stream")
	if err != nil {
		t.Fatalf("cannot connect to /stream: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	close(done)

	if ct := resp.Header.Get("Content-Type"); ct != "application/x-ndjson" {
		t.Errorf("expected Content-Type application/x-ndjson, got %q", ct)
	}

	var fie SequencedFIE
	if err := json.NewDecoder(bufio.NewReader(resp.Body)).Decode(&fie); err != nil {
		t.Fatalf("cannot decode SequencedFIE: %v", err)
	}
	if fie.SequenceNumber != 42 {
		t.Errorf("expected SequenceNumber 42, got %d", fie.SequenceNumber)
	}
}

func TestHandleStream_StreamingUnsupported(t *testing.T) {
	t.Parallel()
	s, _ := newTestAPIServer(t, func(_ *fieClient) {})

	w := newNonFlushResponseWriter()
	r := httptest.NewRequest(http.MethodGet, "/stream", nil)
	s.handleStream(w, r)

	if w.code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", w.code)
	}
}

func TestHandleStream_DeferRemovesClient(t *testing.T) {
	t.Parallel()
	returned := make(chan struct{})
	s, _ := newTestAPIServer(t, func(_ *fieClient) {})
	ts := startHandler(t, s)

	go func() {
		resp, err := http.Get(ts.URL + "/stream")
		if err == nil {
			_ = resp.Body.Close()
		}
		close(returned)
	}()

	<-returned
	time.Sleep(20 * time.Millisecond)

	s.mutex.Lock()
	n := len(s.clients)
	s.mutex.Unlock()
	if n != 0 {
		t.Errorf("expected 0 clients after handler returned, got %d", n)
	}
}

// -- addClient / removeClient -------------------------------------------------

func TestAPIServer_AddRemoveClient(t *testing.T) {
	t.Parallel()
	s, _ := newTestAPIServer(t, func(_ *fieClient) {})
	client := &fieClient{
		ctx:     context.Background(),
		flusher: nopFlusher{},
		encoder: json.NewEncoder(&failWriter{}),
	}

	s.addClient(client)
	if len(s.clients) != 1 {
		t.Errorf("expected 1 client after add, got %d", len(s.clients))
	}
	s.removeClient(client)
	if len(s.clients) != 0 {
		t.Errorf("expected 0 clients after remove, got %d", len(s.clients))
	}
}

// -- fieClient ----------------------------------------------------------------

func TestFIEClient_Context(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := &fieClient{ctx: ctx, flusher: nopFlusher{}, encoder: json.NewEncoder(&failWriter{})}
	if client.context() != ctx {
		t.Error("expected context() to return the original context")
	}
}

func TestFIEClient_SendFIE(t *testing.T) {
	t.Parallel()
	_, server := newTCPPair(t)
	defer func() { _ = server.Close() }()

	client := &fieClient{
		ctx:     context.Background(),
		flusher: nopFlusher{},
		encoder: json.NewEncoder(server),
	}
	if err := client.sendFIE(&SequencedFIE{SequenceNumber: 1}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestFIEClient_SendFIE_EncodeError(t *testing.T) {
	t.Parallel()
	client := &fieClient{
		ctx:     context.Background(),
		flusher: nopFlusher{},
		encoder: json.NewEncoder(&failWriter{}),
	}
	if err := client.sendFIE(&SequencedFIE{}); err == nil {
		t.Fatal("expected error from sendFIE with failing writer, got nil")
	}
}

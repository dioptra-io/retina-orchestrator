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

// 100% coverage: every branch in NewAPIServer, ListenAndServe, Shutdown,
// handleStream, addClient, removeClient, SendFIE, and Context is exercised.

// -- helpers ------------------------------------------------------------------

func newTestAPIServer(t *testing.T, handler FIEHandleFunc) (*APIServer, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot get free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	s, err := NewAPIServer(&APIServerConfig{
		Address:           addr,
		ReadHeaderTimeout: time.Second,
		FIEHandler:        handler,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	s.server.Addr = addr
	return s, addr
}

// startHandler starts only the HTTP mux via httptest.NewServer for handler-level
// tests. Does NOT use s.ListenAndServe so there is no conflict with s.Shutdown.
func startHandler(t *testing.T, s *APIServer) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(s.server.Handler)
	t.Cleanup(func() { ts.Close() })
	return ts
}

// startListening starts s.ListenAndServe in a goroutine and waits briefly for
// the listener to be ready.
func startListening(t *testing.T, s *APIServer) {
	t.Helper()
	go func() { _ = s.ListenAndServe() }()
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

// -- NewAPIServer -------------------------------------------------------------

func TestNewAPIServer_NilHandler(t *testing.T) {
	t.Parallel()
	_, err := NewAPIServer(&APIServerConfig{Address: "127.0.0.1:0"})
	if err == nil {
		t.Fatal("expected error for nil FIEHandler, got nil")
	}
}

func TestNewAPIServer_Valid(t *testing.T) {
	t.Parallel()
	s, _ := newTestAPIServer(t, func(_ *FIEClient) {})
	if s == nil {
		t.Fatal("expected non-nil APIServer")
	}
}

// -- ListenAndServe -----------------------------------------------------------

func TestAPIServer_ListenAndServe_Shutdown(t *testing.T) {
	t.Parallel()
	s, _ := newTestAPIServer(t, func(_ *FIEClient) {})

	done := make(chan error, 1)
	go func() { done <- s.ListenAndServe() }()
	time.Sleep(20 * time.Millisecond)

	if err := s.Shutdown(time.Second); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ListenAndServe returned unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ListenAndServe did not return after Shutdown")
	}
}

func TestAPIServer_ListenAndServe_BindError(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot bind: %v", err)
	}
	defer ln.Close()

	s, err := NewAPIServer(&APIServerConfig{
		Address:           ln.Addr().String(),
		ReadHeaderTimeout: time.Second,
		FIEHandler:        func(_ *FIEClient) {},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.ListenAndServe(); err == nil {
		t.Fatal("expected bind error, got nil")
	}
}

// -- Shutdown -----------------------------------------------------------------

func TestAPIServer_Shutdown_Timeout(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-block:
		default:
			close(block)
		}
	})

	s, addr := newTestAPIServer(t, func(_ *FIEClient) { <-block })
	startListening(t, s)

	go func() {
		resp, err := http.Get("http://" + addr + "/stream")
		if err == nil {
			resp.Body.Close()
		}
	}()
	time.Sleep(20 * time.Millisecond)

	if err := s.Shutdown(time.Millisecond); err == nil {
		t.Fatal("expected timeout error from Shutdown, got nil")
	}
}

// -- handleStream -------------------------------------------------------------

func TestHandleStream_SendFIE(t *testing.T) {
	t.Parallel()
	done := make(chan struct{})
	s, _ := newTestAPIServer(t, func(client *FIEClient) {
		_ = client.SendFIE(&SequencedFIE{SequenceNumber: 42})
		<-done
	})
	ts := startHandler(t, s)

	resp, err := http.Get(ts.URL + "/stream")
	if err != nil {
		t.Fatalf("cannot connect to /stream: %v", err)
	}
	defer resp.Body.Close()
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
	s, _ := newTestAPIServer(t, func(_ *FIEClient) {})

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
	s, _ := newTestAPIServer(t, func(_ *FIEClient) {})
	ts := startHandler(t, s)

	go func() {
		resp, err := http.Get(ts.URL + "/stream")
		if err == nil {
			resp.Body.Close()
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
	s, _ := newTestAPIServer(t, func(_ *FIEClient) {})
	client := &FIEClient{
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

// -- FIEClient ----------------------------------------------------------------

func TestFIEClient_Context(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	client := &FIEClient{ctx: ctx, flusher: nopFlusher{}, encoder: json.NewEncoder(&failWriter{})}
	if client.Context() != ctx {
		t.Error("expected Context() to return the original context")
	}
}

func TestFIEClient_SendFIE(t *testing.T) {
	t.Parallel()
	_, server := newTCPPair(t)
	defer server.Close()

	client := &FIEClient{
		ctx:     context.Background(),
		flusher: nopFlusher{},
		encoder: json.NewEncoder(server),
	}
	if err := client.SendFIE(&SequencedFIE{SequenceNumber: 1}); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestFIEClient_SendFIE_EncodeError(t *testing.T) {
	t.Parallel()
	client := &FIEClient{
		ctx:     context.Background(),
		flusher: nopFlusher{},
		encoder: json.NewEncoder(&failWriter{}),
	}
	if err := client.SendFIE(&SequencedFIE{}); err == nil {
		t.Fatal("expected error from SendFIE with failing writer, got nil")
	}
}

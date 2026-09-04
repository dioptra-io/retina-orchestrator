// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Intentionally uncovered in streamOnce (api_client.go):
//
//   - The NewRequestWithContext error branch is unreachable in practice —
//     Config.Validate() already guarantees a valid URL upstream.
//   - resp.Body.Close() and "connection closed unexpectedly" only run on a
//     clean server response/close while the client's write side is still
//     open — reproducing that deterministically risks the same flakiness
//     this suite spent a while debugging, so it's left untested.
//   - TestStreamOnce_MidStreamDisconnectIsDetected hits either the
//     encode-error or respCh-error branch, whichever goroutine notices
//     first — which one shows covered can vary between runs.

func newTestMetrics() *Metrics {
	return &Metrics{
		APIClientFIEsDroppedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "test_api_client_fies_dropped_total",
		}),
		APIClientConnectionUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "test_api_client_connection_up",
		}),
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func waitForGauge(t *testing.T, g prometheus.Gauge, want float64) {
	t.Helper()
	const timeout = time.Second
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if testutil.ToFloat64(g) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("gauge did not reach %v within %s (last value %v)", want, timeout, testutil.ToFloat64(g))
}

// fakeIngestServer mimics retina-api's handleIngest: no response until the
// stream ends, plus a mid-stream disconnect for failure-detection tests.
type fakeIngestServer struct {
	server *httptest.Server

	mu       sync.Mutex
	received []api.ForwardingInfoElement

	closeAfterN int // hijack + hard-reset the connection after N decoded FIEs; 0 = never

	receivedCh chan struct{}
	attempts   atomic.Int64
}

func newFakeIngestServer(t *testing.T) *fakeIngestServer {
	t.Helper()
	f := &fakeIngestServer{receivedCh: make(chan struct{}, 100_000)}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *fakeIngestServer) handle(w http.ResponseWriter, r *http.Request) {
	f.attempts.Add(1)

	dec := json.NewDecoder(r.Body)
	count := 0
	for {
		var fie api.ForwardingInfoElement
		if err := dec.Decode(&fie); err != nil {
			break
		}
		f.mu.Lock()
		f.received = append(f.received, fie)
		f.mu.Unlock()
		select {
		case f.receivedCh <- struct{}{}:
		default:
		}
		count++

		if f.closeAfterN > 0 && count >= f.closeAfterN {
			if hj, ok := w.(http.Hijacker); ok {
				if conn, _, err := hj.Hijack(); err == nil {
					// SetLinger(0) forces a hard RST instead of a graceful
					// FIN: an ordinary close can leave the client's next
					// write indefinitely unacknowledged rather than
					// failing, which is what caused this test to hang.
					if tcpConn, ok := conn.(*net.TCPConn); ok {
						_ = tcpConn.SetLinger(0)
					}
					conn.Close()
				}
			}
			return
		}
	}
	fmt.Fprintf(w, `{"received": %d}`, count)
}

func (f *fakeIngestServer) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.received)
}

// waitForCount blocks until the server has received at least n FIEs or the
// timeout elapses.
func (f *fakeIngestServer) waitForCount(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for f.count() < n {
		select {
		case <-f.receivedCh:
		case <-deadline:
			t.Fatalf("timed out waiting for server to receive %d FIEs, got %d", n, f.count())
		}
	}
}

// ---- newAPIClient / config validation ----

// TestNewAPIClient_Validation covers only what newAPIClient itself checks —
// url/bufferSize/reconnectDelay validation lives in Config.Validate() now
// (see orchestrator_test.go's TestConfig_Validate_* tests).
func TestNewAPIClient_Validation(t *testing.T) {
	t.Parallel()
	baseConfig := func() *apiClientConfig {
		return &apiClientConfig{url: "http://example.invalid", bufferSize: 100, metrics: newTestMetrics()}
	}

	tests := []struct {
		name    string
		mutate  func(c *apiClientConfig)
		wantErr bool
	}{
		{"nil metrics", func(c *apiClientConfig) { c.metrics = nil }, true},
		{"valid minimal config", func(*apiClientConfig) {}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := baseConfig()
			tt.mutate(cfg)
			_, err := newAPIClient(cfg)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// TestNewAPIClient_Defaults covers only what newAPIClient itself defaults
// (httpClient, logger) — bufferSize/reconnectDelay default in
// Config.applyDefaults() now (orchestrator_test.go).
func TestNewAPIClient_Defaults(t *testing.T) {
	t.Parallel()
	cfg := &apiClientConfig{url: "http://example.invalid", bufferSize: 100, metrics: newTestMetrics()}
	c, err := newAPIClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.httpClient == nil {
		t.Error("expected default httpClient to be set")
	}
	if cfg.logger == nil {
		t.Error("expected default logger to be set")
	}
	if cap(c.fieChan) != 100 {
		t.Errorf("expected fieChan capacity to match the given bufferSize, got %d", cap(c.fieChan))
	}
}

// TestNewAPIClient_UnbufferedIfBufferSizeUnset: newAPIClient no longer
// defaults a zero bufferSize, so calling it directly with none set silently
// produces an unbuffered channel.
func TestNewAPIClient_UnbufferedIfBufferSizeUnset(t *testing.T) {
	t.Parallel()
	c, err := newAPIClient(&apiClientConfig{url: "http://example.invalid", metrics: newTestMetrics()})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cap(c.fieChan) != 0 {
		t.Errorf("expected an unbuffered channel when bufferSize is left unset, got capacity %d", cap(c.fieChan))
	}
}

func TestNewAPIClient_PreservesExplicitValues(t *testing.T) {
	t.Parallel()
	httpClient := &http.Client{Timeout: time.Minute}
	logger := discardLogger()
	metrics := newTestMetrics()

	cfg := &apiClientConfig{
		url:            "http://example.invalid",
		bufferSize:     42,
		reconnectDelay: 3 * time.Second,
		httpClient:     httpClient,
		logger:         logger,
		metrics:        metrics,
	}
	c, err := newAPIClient(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.bufferSize != 42 || cap(c.fieChan) != 42 {
		t.Errorf("expected explicit bufferSize 42 preserved, got %d (chan cap %d)", cfg.bufferSize, cap(c.fieChan))
	}
	if cfg.reconnectDelay != 3*time.Second {
		t.Errorf("expected explicit reconnectDelay preserved, got %s", cfg.reconnectDelay)
	}
	if cfg.httpClient != httpClient {
		t.Error("expected explicit httpClient preserved")
	}
}

// ---- push ----

func TestPush_NilFIEIsDroppedAndCounted(t *testing.T) {
	t.Parallel()
	metrics := newTestMetrics()
	c, err := newAPIClient(&apiClientConfig{url: "http://example.invalid", bufferSize: 1, metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}

	c.push(nil)

	if got := testutil.ToFloat64(metrics.APIClientFIEsDroppedTotal); got != 1 {
		t.Errorf("expected dropped counter 1, got %v", got)
	}
	if len(c.fieChan) != 0 {
		t.Error("expected channel to remain empty after a nil push")
	}
}

func TestPush_SucceedsWhenSpaceAvailable(t *testing.T) {
	t.Parallel()
	metrics := newTestMetrics()
	c, err := newAPIClient(&apiClientConfig{url: "http://example.invalid", bufferSize: 2, metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}

	fie := &api.ForwardingInfoElement{ProbingDirectiveID: 42}
	c.push(fie)

	if got := testutil.ToFloat64(metrics.APIClientFIEsDroppedTotal); got != 0 {
		t.Errorf("did not expect any drops, got %v", got)
	}
	select {
	case got := <-c.fieChan:
		if got.ProbingDirectiveID != 42 {
			t.Errorf("unexpected FIE dequeued: %+v", got)
		}
	default:
		t.Error("expected FIE to be queued")
	}
}

func TestPush_FullBufferDropsAndCounts(t *testing.T) {
	t.Parallel()
	metrics := newTestMetrics()
	c, err := newAPIClient(&apiClientConfig{url: "http://example.invalid", bufferSize: 1, metrics: metrics})
	if err != nil {
		t.Fatal(err)
	}

	fie1 := &api.ForwardingInfoElement{ProbingDirectiveID: 1}
	fie2 := &api.ForwardingInfoElement{ProbingDirectiveID: 2}

	c.push(fie1) // fills the buffer (size 1)
	c.push(fie2) // must be dropped

	if got := testutil.ToFloat64(metrics.APIClientFIEsDroppedTotal); got != 1 {
		t.Errorf("expected dropped counter 1, got %v", got)
	}
	select {
	case got := <-c.fieChan:
		if got != fie1 {
			t.Error("expected the first FIE to remain queued, second to be dropped")
		}
	default:
		t.Error("expected one FIE to remain queued")
	}
}

// ---- streamOnce ----

func TestStreamOnce_SuccessfulStreamingAndCleanShutdown(t *testing.T) {
	t.Parallel()
	fake := newFakeIngestServer(t)
	metrics := newTestMetrics()

	c, err := newAPIClient(&apiClientConfig{
		url:        fake.server.URL,
		bufferSize: 100,
		metrics:    metrics,
		logger:     discardLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.streamOnce(ctx) }()

	waitForGauge(t, metrics.APIClientConnectionUp, 1)

	// Push continuously rather than a fixed handful: a small number of
	// small writes can sit buffered indefinitely (see api_client.go's
	// streamOnce doc comment) — sustained traffic, as in real production
	// load, reliably forces a flush.
	stopPushing := make(chan struct{})
	go func() {
		var i uint64
		for {
			select {
			case <-stopPushing:
				return
			default:
				c.push(&api.ForwardingInfoElement{ProbingDirectiveID: i})
				i++
				time.Sleep(time.Millisecond)
			}
		}
	}()
	fake.waitForCount(t, 5, 5*time.Second)
	close(stopPushing)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected nil error on clean shutdown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("streamOnce did not return after context cancellation")
	}

	waitForGauge(t, metrics.APIClientConnectionUp, 0)
}

// TestStreamOnce_CanceledBeforeAnyFIE covers cancellation when nothing has
// ever been pushed — distinct from the clean-shutdown path in
// TestStreamOnce_SuccessfulStreamingAndCleanShutdown, which cancels after
// data has already flowed.
func TestStreamOnce_CanceledBeforeAnyFIE(t *testing.T) {
	t.Parallel()
	fake := newFakeIngestServer(t)

	metrics := newTestMetrics()
	c, err := newAPIClient(&apiClientConfig{url: fake.server.URL, bufferSize: 100, metrics: metrics, logger: discardLogger()})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := c.streamOnce(ctx); err != nil {
		t.Errorf("expected nil error on cancellation before any FIE was pushed, got %v", err)
	}
}

func TestStreamOnce_MidStreamDisconnectIsDetected(t *testing.T) {
	t.Parallel()
	fake := newFakeIngestServer(t)
	fake.closeAfterN = 2

	metrics := newTestMetrics()
	c, err := newAPIClient(&apiClientConfig{url: fake.server.URL, bufferSize: 100, metrics: metrics, logger: discardLogger()})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.streamOnce(ctx) }()

	waitForGauge(t, metrics.APIClientConnectionUp, 1)

	// Push continuously rather than a one-off burst: a handful of small
	// writes can sit buffered in the connection's write buffer
	// indefinitely (see api_client.go's streamOnce doc comment) — only
	// sustained traffic reliably forces a flush, same as real production
	// load would.
	stopPushing := make(chan struct{})
	defer close(stopPushing)
	go func() {
		var i uint64
		for {
			select {
			case <-stopPushing:
				return
			default:
				c.push(&api.ForwardingInfoElement{ProbingDirectiveID: i})
				i++
				time.Sleep(time.Millisecond)
			}
		}
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error after the server closed the connection mid-stream")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("streamOnce did not detect the mid-stream disconnect in time")
	}

	waitForGauge(t, metrics.APIClientConnectionUp, 0)
}

// ---- run ----

// TestRun_ReturnsImmediatelyIfAlreadyCanceled covers run()'s top-of-loop
// ctx.Err() check specifically — every other run() test exits via the
// select's <-ctx.Done() case instead, never looping back to hit this one.
func TestRun_ReturnsImmediatelyIfAlreadyCanceled(t *testing.T) {
	t.Parallel()
	metrics := newTestMetrics()
	c, err := newAPIClient(&apiClientConfig{url: "http://example.invalid", bufferSize: 100, metrics: metrics, logger: discardLogger()})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled before run() is ever called

	done := make(chan struct{})
	go func() {
		c.run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not return immediately for an already-canceled context")
	}
}

// TestRun_RetriesOnFailureAndStopsOnCancel points at an address nothing is
// listening on — a real, common failure mode (retina-api down, wrong port)
// — rather than simulating server-side rejection, since retina-api's
// ingest handler doesn't reject early in the current design (see
// api_client.go's streamOnce doc comment on why an early response isn't
// used at all).
func TestRun_RetriesOnFailureAndStopsOnCancel(t *testing.T) {
	t.Parallel()

	metrics := newTestMetrics()
	c, err := newAPIClient(&apiClientConfig{
		url:            "http://127.0.0.1:1/api/v1/ingest", // port 1: nothing listens here
		bufferSize:     100,
		metrics:        metrics,
		reconnectDelay: 10 * time.Millisecond,
		logger:         discardLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.run(ctx)
		close(done)
	}()

	// Let it retry a handful of times before stopping it.
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("run did not return after context cancellation")
	}
}

func TestRun_SuccessfulSessionDoesNotRetry(t *testing.T) {
	t.Parallel()
	fake := newFakeIngestServer(t)

	metrics := newTestMetrics()
	c, err := newAPIClient(&apiClientConfig{
		url:            fake.server.URL,
		bufferSize:     100,
		metrics:        metrics,
		reconnectDelay: 10 * time.Millisecond,
		logger:         discardLogger(),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.run(ctx)
		close(done)
	}()

	waitForGauge(t, metrics.APIClientConnectionUp, 1)

	// Push continuously rather than once: a single small write can sit
	// buffered indefinitely (see api_client.go's streamOnce doc comment) —
	// sustained traffic, as in real production load, reliably forces a flush.
	stopPushing := make(chan struct{})
	go func() {
		var i uint64
		for {
			select {
			case <-stopPushing:
				return
			default:
				c.push(&api.ForwardingInfoElement{ProbingDirectiveID: i})
				i++
				time.Sleep(time.Millisecond)
			}
		}
	}()
	fake.waitForCount(t, 1, 5*time.Second)
	close(stopPushing)

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not return after cancel")
	}

	if attempts := fake.attempts.Load(); attempts != 1 {
		t.Errorf("expected exactly one connection attempt for a healthy session, got %d", attempts)
	}
}

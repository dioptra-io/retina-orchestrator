// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/framing"
	"github.com/dioptra-io/retina-commons/model"
	wire "github.com/dioptra-io/retina-commons/wire/v2"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Intentionally uncovered:
//
//   - NewOrch: newQueue/newRingBuffer errors are unreachable (hardcoded safe
//     values); newAPIServer only fails on nil fieHandler, never nil here.
//   - runScheduler: PD drop branch requires a consumer to vanish between
//     TryPush and send — not reproducible in unit tests.
//   - runAPIServer/runAgentServer: non-context error branches require server
//     failure unrelated to shutdown — not injectable without refactoring.
//   - fieStreamHandler: internal_error on Pop requires a non-context error
//     from RingBuffer, which has no injectable trigger.
//   - agentHandler: the o.scheduler.UpdateFromFIE error-log branch is
//     unreachable with the real *Scheduler — UpdateFromFIE's only two
//     return points both return nil, so the error path exists on the
//     method signature but nothing in its body ever populates it. Testing
//     this would need agentHandler to depend on a Scheduler interface
//     rather than the concrete type, purely to inject a fake that errors —
//     not worth that structural change for one log line.

// -- helpers ------------------------------------------------------------------

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testMetrics() *Metrics {
	return NewMetrics(prometheus.NewRegistry())
}

// writePDFile writes a single, valid PD as a protojson-encoded
// wire.ProbingDirective — readPDs() parses PD files with protojson now
// (see scheduler.go), and model.ProbingDirectiveFromProto rejects an
// empty destination_address, so this can't be a bare zero-value literal
// the way the old api.ProbingDirective version could.
func writePDFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	pd := &wire.ProbingDirective{
		ProbingDirectiveId: 1,
		IpVersion:          wire.IPVersion_IP_VERSION_IPV4,
		DestinationAddress: "192.0.2.1",
	}
	b, err := protojson.Marshal(pd)
	if err != nil {
		t.Fatalf("cannot marshal PD: %v", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		t.Fatalf("cannot write PD file: %v", err)
	}
	_ = f.Close()
	return f.Name()
}

func validConfig(t *testing.T) *Config {
	t.Helper()
	return &Config{
		AgentAddress:               "127.0.0.1:0",
		AgentBufferLength:          8192,
		PDQueueSize:                100,
		RingBufferSize:             100,
		APIAddress:                 "127.0.0.1:0",
		PDPathV4:                   writePDFile(t),
		Seed:                       0,
		IssuanceRate:               1.0,
		ImpactThreshold:            1.0,
		Secret:                     "secret",
		ActiveSetSize:              1,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               9,
	}
}

// validWireInfo returns an Info with the fields InfoFromProto requires
// (timestamps are always required in this schema — see retina-commons/model).
func validWireInfo() *wire.Info {
	return &wire.Info{
		ProbeTtl:          1,
		SentTimestamp:     timestamppb.New(time.Now()),
		ReceivedTimestamp: timestamppb.New(time.Now()),
	}
}

// validWireFIE is defined in agent_server_test.go (same package) — reused
// here rather than redefined.

// sendFIEs sends a sequence of FIEs over conn to exercise agentHandler FIE
// receive paths: one with an unknown PD ID (UpdateFromFIE error log), one
// incomplete (continue branch), and one complete (ring buffer push). Each
// FIE is otherwise fully valid — see validWireFIE's doc comment for why a
// bare/empty literal no longer works here the way api.ForwardingInfoElement
// allowed.
func sendFIEs(t *testing.T, conn net.Conn) {
	t.Helper()

	unknown := validWireFIE(999)
	if err := framing.Send(conn, 0, unknown); err != nil {
		t.Fatalf("cannot send unknown FIE: %v", err)
	}

	incomplete := validWireFIE(1)
	incomplete.NearInfo = validWireInfo()
	if err := framing.Send(conn, 0, incomplete); err != nil {
		t.Fatalf("cannot send incomplete FIE: %v", err)
	}

	complete := validWireFIE(1)
	complete.NearInfo = validWireInfo()
	complete.FarInfo = validWireInfo()
	if err := framing.Send(conn, 0, complete); err != nil {
		t.Fatalf("cannot send complete FIE: %v", err)
	}
}

// -- Config.Validate ----------------------------------------------------------

func TestConfig_Validate_Valid(t *testing.T) {
	t.Parallel()
	if err := validConfig(t).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfig_Validate_DefaultsAPIReadHeaderTimeout(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.APIReadHeaderTimeout = 0
	_ = c.Validate()
	if c.APIReadHeaderTimeout != 5*time.Second {
		t.Errorf("expected default 5s, got %v", c.APIReadHeaderTimeout)
	}
}

func TestConfig_Validate_DefaultsFIEFilterPolicy(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.FIEFilterPolicy = ""
	_ = c.Validate()
	if c.FIEFilterPolicy != "both" {
		t.Errorf("expected default 'both', got %q", c.FIEFilterPolicy)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	t.Parallel()
	base := validConfig(t)
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty AgentAddress", func(c *Config) { c.AgentAddress = "" }},
		{"small AgentBufferLength", func(c *Config) { c.AgentBufferLength = 100 }},
		{"zero PDQueueSize", func(c *Config) { c.PDQueueSize = 0 }},
		{"zero RingBufferSize", func(c *Config) { c.RingBufferSize = 0 }},
		{"empty APIAddress", func(c *Config) { c.APIAddress = "" }},
		{"both PD paths empty", func(c *Config) { c.PDPathV4 = ""; c.PDPathV6 = "" }},
		{"zero IssuanceRate", func(c *Config) { c.IssuanceRate = 0 }},
		{"negative IssuanceRate", func(c *Config) { c.IssuanceRate = -1 }},
		{"zero ImpactThreshold", func(c *Config) { c.ImpactThreshold = 0 }},
		{"negative ImpactThreshold", func(c *Config) { c.ImpactThreshold = -1 }},
		{"invalid FIEFilterPolicy", func(c *Config) { c.FIEFilterPolicy = "invalid" }},
		{"zero ActiveSetSize", func(c *Config) { c.ActiveSetSize = 0 }},
		{"zero ConsecutiveMissesThreshold", func(c *Config) { c.ConsecutiveMissesThreshold = 0 }},
		{"zero MaxEvictions", func(c *Config) { c.MaxEvictions = 0 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := *base
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("expected error for %q, got nil", tc.name)
			}
		})
	}
}

// -- NewOrch ------------------------------------------------------------------

func TestNewOrch_Valid(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o == nil {
		t.Fatal("expected non-nil orchestrator")
	}
}

func TestNewOrch_InvalidConfig(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.AgentAddress = ""
	if _, err := NewOrch(c, testLogger(), testMetrics()); err == nil {
		t.Fatal("expected error for invalid config, got nil")
	}
}

func TestNewOrch_SchedulerError(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.PDPathV4 = "/nonexistent/path.jsonl"
	if _, err := NewOrch(c, testLogger(), testMetrics()); err == nil {
		t.Fatal("expected error for bad PDPath, got nil")
	}
}

func TestNewOrch_NilLogger(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), nil, testMetrics())
	if err != nil {
		t.Fatalf("unexpected error with nil logger: %v", err)
	}
	if o == nil {
		t.Fatal("expected non-nil orchestrator")
	}
}

func TestNewOrch_NilMetrics(t *testing.T) {
	t.Parallel()
	if _, err := NewOrch(validConfig(t), testLogger(), nil); err == nil {
		t.Fatal("expected error for nil metrics, got nil")
	}
}

// -- Run ----------------------------------------------------------------------

func TestRun_StartsAndStopsCleanly(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = o.Run(ctx)
	if err != nil && err != context.DeadlineExceeded {
		t.Fatalf("unexpected Run error: %v", err)
	}
}

// -- runScheduler -------------------------------------------------------------

func TestRunScheduler_ContextCancelled(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = o.runScheduler(ctx)
	if err != nil {
		t.Fatalf("expected nil (clean shutdown), got %v", err)
	}
}

func TestRunScheduler_SkipsNilPD(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := o.runScheduler(ctx); err != nil {
		t.Fatalf("expected nil (clean shutdown), got %v", err)
	}
}

func TestRunScheduler_DropsWhenNoQueue(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := o.runScheduler(ctx); err != nil {
		t.Fatalf("expected nil (clean shutdown), got %v", err)
	}
}

func TestRunScheduler_PushesToExistingQueue(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Register a consumer for the agent ID used in the PD file (empty string
	// since writePDFile sets no AgentId).
	consumer, err := o.pdQueue.NewConsumer("")
	if err != nil {
		t.Fatalf("unexpected error creating consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := o.runScheduler(ctx); err != nil {
		t.Fatalf("expected nil (clean shutdown), got %v", err)
	}
}

// -- runAPIServer -------------------------------------------------------------

func TestRunAPIServer_StartsAndStops(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = o.runAPIServer(ctx)
	if err != nil {
		t.Fatalf("unexpected runAPIServer error: %v", err)
	}
}

// -- runAgentServer -----------------------------------------------------------

func TestRunAgentServer_StartsAndStops(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = o.runAgentServer(ctx)
	if err != nil {
		t.Fatalf("unexpected runAgentServer error: %v", err)
	}
}

// -- fieStreamHandler ---------------------------------------------------------

// TestFieStreamHandler_SendsAndStops pushes directly onto the ring buffer,
// bypassing wire serialization entirely (Push takes a plain
// *model.ForwardingInfoElement, no ToProto/FromProto involved) — so unlike
// sendFIEs above, a minimal literal is fine here; nothing validates it on
// this path. modelFIEToAPIv1 (called inside fieStreamHandler) is a plain
// field copy with no validation either.
func TestFieStreamHandler_SendsAndStops(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	var buf bytes.Buffer
	client := &fieClient{
		ctx:     ctx,
		flusher: nopFlusher{},
		encoder: json.NewEncoder(&buf),
	}

	fie := &model.ForwardingInfoElement{
		ProbingDirectiveID: 1,
		NearInfo:           &model.Info{},
		FarInfo:            &model.Info{},
	}

	done := make(chan struct{})
	go func() {
		o.fieStreamHandler(client)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	_ = o.ringBuffer.Push(fie)

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fieStreamHandler did not return after context cancel")
	}

	if buf.Len() == 0 {
		t.Error("expected FIE to be written to buffer")
	}
}

func TestFieStreamHandler_SendFIEError(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &fieClient{
		ctx:     ctx,
		flusher: nopFlusher{},
		encoder: json.NewEncoder(&failWriter{}),
	}

	fie := &model.ForwardingInfoElement{
		ProbingDirectiveID: 1,
		NearInfo:           &model.Info{},
		FarInfo:            &model.Info{},
	}

	done := make(chan struct{})
	go func() {
		o.fieStreamHandler(client)
		close(done)
	}()

	// Wait for consumer to be created before pushing.
	time.Sleep(20 * time.Millisecond)
	_ = o.ringBuffer.Push(fie)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fieStreamHandler did not return on sendFIE error")
	}
}

// -- agentHandler -------------------------------------------------------------

func TestAgentHandler_DuplicateConnection(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	status := &agentAuthStatus{agentID: "agent-1"}

	// Pre-register a consumer to simulate an already-connected agent.
	_, err = o.pdQueue.NewConsumer("agent-1")
	if err != nil {
		t.Fatalf("unexpected error creating consumer: %v", err)
	}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return for duplicate connection")
	}
}

func TestAgentHandler_ReceivesAndForwardsPD(t *testing.T) {
	// Not parallel — uses real TCP connections.
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	clientConn, serverConn := newTCPPair(t)
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream := &agentStream{
		conn:   serverConn,
		ctx:    ctx,
		cancel: cancel,
	}

	status := &agentAuthStatus{agentID: "agent-1"}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, stream)
		close(done)
	}()

	// Wait for agentHandler to register its consumer before pushing.
	time.Sleep(20 * time.Millisecond)

	pd := &model.ProbingDirective{
		ProbingDirectiveID: 1,
		AgentID:            "agent-1",
		DestinationAddress: net.ParseIP("192.0.2.1"),
	}
	if err := o.pdQueue.TryPush("agent-1", pd); err != nil {
		t.Fatalf("unexpected push error: %v", err)
	}

	var received wire.ProbingDirective
	if err := framing.Receive(clientConn, 500*time.Millisecond, &received); err != nil {
		t.Fatalf("cannot decode PD: %v", err)
	}
	if received.ProbingDirectiveId != 1 {
		t.Errorf("expected PD ID 1, got %d", received.ProbingDirectiveId)
	}

	cancel()
	_ = serverConn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return after context cancel")
	}
}

func TestAgentHandler_ReceivesFIE(t *testing.T) {
	// Not parallel — uses real TCP connections.
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	clientConn, serverConn := newTCPPair(t)
	defer func() { _ = clientConn.Close() }()
	defer func() { _ = serverConn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream := &agentStream{
		conn:   serverConn,
		ctx:    ctx,
		cancel: cancel,
	}

	status := &agentAuthStatus{agentID: "agent-2"}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, stream)
		close(done)
	}()

	// Give agentHandler time to start its goroutines.
	time.Sleep(20 * time.Millisecond)
	sendFIEs(t, clientConn)
	time.Sleep(50 * time.Millisecond)

	cancel()
	_ = serverConn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return after context cancel")
	}
}

func TestAgentHandler_SendPDError(t *testing.T) {
	// Not parallel — uses real TCP connections.
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	clientConn, serverConn := newTCPPair(t)
	defer func() { _ = clientConn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	stream := &agentStream{
		conn:   serverConn,
		ctx:    ctx,
		cancel: cancel,
	}

	status := &agentAuthStatus{agentID: "agent-3"}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, stream)
		close(done)
	}()

	// Wait for agentHandler to register its consumer, then close the
	// server connection so the next sendPD fails.
	time.Sleep(20 * time.Millisecond)
	_ = serverConn.Close()

	// Push a PD with a valid DestinationAddress — so sendPD reaches the
	// actual network write (and fails there, on the closed connection)
	// rather than failing earlier at ToProto()'s required-field check,
	// which would test a different code path than intended here.
	pd := &model.ProbingDirective{
		ProbingDirectiveID: 1,
		AgentID:            "agent-3",
		DestinationAddress: net.ParseIP("192.0.2.1"),
	}
	_ = o.pdQueue.TryPush("agent-3", pd)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return after sendPD error")
	}
}

// -- filterFIE ----------------------------------------------------------------
//
// filterFIE no longer returns an error (see orchestrator.go) — the policy
// is validated once in Config.Validate() against an immutable config copy,
// so there's no longer a reachable invalid-policy case to test. The old
// TestFilterFIE_InvalidPolicy, which forced FIEFilterPolicy to "invalid"
// after construction to trigger that error path, has no equivalent anymore
// and is removed rather than repurposed.

func TestFilterFIE_PolicyAny(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	o.config.FIEFilterPolicy = "any"

	if !o.filterFIE(&model.ForwardingInfoElement{}) {
		t.Error("expected policy 'any' to allow all FIEs")
	}
}

func TestFilterFIE_PolicyOne(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	o.config.FIEFilterPolicy = "one"

	tests := []struct {
		name string
		fie  *model.ForwardingInfoElement
		want bool
	}{
		{"both nil", &model.ForwardingInfoElement{}, false},
		{"near only", &model.ForwardingInfoElement{NearInfo: &model.Info{}}, true},
		{"far only", &model.ForwardingInfoElement{FarInfo: &model.Info{}}, true},
		{"both set", &model.ForwardingInfoElement{NearInfo: &model.Info{}, FarInfo: &model.Info{}}, true},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := o.filterFIE(tt.fie); got != tt.want {
				t.Errorf("filterFIE(%s) = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

// -- agentAuthHandler ---------------------------------------------------------

func TestAgentAuthHandler_ValidSecret(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.Secret = "mysecret"
	o, err := NewOrch(c, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp := o.agentAuthHandler(&wire.AuthRequest{Secret: "mysecret"})
	if !resp.Authenticated {
		t.Errorf("expected authenticated, got: %s", resp.Message)
	}
}

func TestAgentAuthHandler_InvalidSecret(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.Secret = "mysecret"
	o, err := NewOrch(c, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp := o.agentAuthHandler(&wire.AuthRequest{Secret: "wrong"})
	if resp.Authenticated {
		t.Fatal("expected not authenticated")
	}
}

// -- modelFIEToAPIv1 -----------------------------------------------------------

func TestModelFIEToAPIv1_IPVersionOutOfRange(t *testing.T) {
	t.Parallel()
	fie := &model.ForwardingInfoElement{IPVersion: wire.IPVersion(256)}
	if _, err := modelFIEToAPIv1(fie); err == nil {
		t.Fatal("expected error for out-of-range IPVersion, got nil")
	}
}

func TestModelFIEToAPIv1_ProtocolOutOfRange(t *testing.T) {
	t.Parallel()
	fie := &model.ForwardingInfoElement{Protocol: wire.Protocol(256)}
	if _, err := modelFIEToAPIv1(fie); err == nil {
		t.Fatal("expected error for out-of-range Protocol, got nil")
	}
}

// TestFieStreamHandler_ConvertFIEError covers the new error branch added
// to fieStreamHandler alongside modelFIEToAPIv1's fallibility (see G115
// fix) — nothing previously pushed a FIE with an out-of-range enum value
// through the full ring-buffer-to-HTTP-client flow.
func TestFieStreamHandler_ConvertFIEError(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var buf bytes.Buffer
	client := &fieClient{
		ctx:     ctx,
		flusher: nopFlusher{},
		encoder: json.NewEncoder(&buf),
	}

	fie := &model.ForwardingInfoElement{
		ProbingDirectiveID: 1,
		IPVersion:          wire.IPVersion(256),
	}

	done := make(chan struct{})
	go func() {
		o.fieStreamHandler(client)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	_ = o.ringBuffer.Push(fie)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fieStreamHandler did not return on FIE conversion error")
	}
}

// TestRunScheduler_ContinuesOnNilPDWithValidContext exercises runScheduler's
// `if pd == nil { continue }` line specifically — distinct from the
// already-documented TryPush race, and from every other runScheduler test,
// which only produces a nil pd via context cancellation (caught earlier by
// runScheduler's own ctx.Err() check, so continue is never reached there).
//
// Emptying pdMap directly makes NextPD's selection nil while ctx stays
// valid: a fresh scheduler's zero-value lastIssuance means the first
// wait fires almost instantly (nextTime is already in the past, not
// because of cancellation), so pd==nil and ctx is still valid when
// runScheduler checks it. The second loop iteration then faces a real
// ~1s wait (lastIssuance was just updated), which the short test timeout
// cancels normally — but by then, continue already ran once.
func TestRunScheduler_ContinuesOnNilPDWithValidContext(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for id := range o.scheduler.pdMap {
		delete(o.scheduler.pdMap, id)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if err := o.runScheduler(ctx); err != nil {
		t.Fatalf("expected nil (clean shutdown), got %v", err)
	}
}

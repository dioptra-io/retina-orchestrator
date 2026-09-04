// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"context"
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
//   - NewOrch: newQueue's error is unreachable (hardcoded safe value).
//   - runScheduler: PD drop branch requires a consumer to vanish between
//     TryPush and send — not reproducible in unit tests.
//   - runAgentServer: non-context error branches require server failure
//     unrelated to shutdown — not injectable without refactoring. apiClient's
//     own connect/retry/failure branches are covered directly in
//     api_client_test.go, not re-tested here.
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

// validConfig returns a Config that passes Validate() as-is. APIURL points
// at a closed local port — fast, guaranteed connection refusal, since
// nothing here needs a real retina-api.
func validConfig(t *testing.T) *Config {
	t.Helper()
	return &Config{
		AgentAddress:               "127.0.0.1:0",
		AgentBufferLength:          8192,
		PDQueueSize:                100,
		APIURL:                     "http://127.0.0.1:1/api/v1/ingest",
		APIBufferSize:              10_000,
		APIReconnectDelay:          5 * time.Second,
		PDPathV4:                   writePDFile(t),
		Seed:                       0,
		IssuanceRate:               1.0,
		ImpactThreshold:            1.0,
		FIEFilterPolicy:            "both",
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
// incomplete (continue branch), and one complete (pushed to apiClient).
// Each FIE is otherwise fully valid — see validWireFIE's doc comment for
// why a bare/empty literal no longer works here the way
// api.ForwardingInfoElement allowed.
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

func TestConfig_ApplyDefaults_FIEFilterPolicy(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.FIEFilterPolicy = ""
	c.applyDefaults()
	if c.FIEFilterPolicy != "both" {
		t.Errorf("expected default 'both', got %q", c.FIEFilterPolicy)
	}
}

func TestConfig_ApplyDefaults_APIBufferSize(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.APIBufferSize = 0
	c.applyDefaults()
	if c.APIBufferSize != 10_000 {
		t.Errorf("expected default 10000, got %d", c.APIBufferSize)
	}
}

func TestConfig_ApplyDefaults_APIReconnectDelay(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.APIReconnectDelay = 0
	c.applyDefaults()
	if c.APIReconnectDelay != 5*time.Second {
		t.Errorf("expected default 5s, got %v", c.APIReconnectDelay)
	}
}

func TestConfig_ApplyDefaults_DoesNotOverrideExplicitValues(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.APIBufferSize = 500
	c.APIReconnectDelay = 3 * time.Second
	c.FIEFilterPolicy = "any"
	c.applyDefaults()
	if c.APIBufferSize != 500 {
		t.Errorf("expected explicit APIBufferSize 500 preserved, got %d", c.APIBufferSize)
	}
	if c.APIReconnectDelay != 3*time.Second {
		t.Errorf("expected explicit APIReconnectDelay preserved, got %v", c.APIReconnectDelay)
	}
	if c.FIEFilterPolicy != "any" {
		t.Errorf("expected explicit FIEFilterPolicy preserved, got %q", c.FIEFilterPolicy)
	}
}

// TestConfig_Validate_DoesNotMutate documents the contract directly:
// Validate no longer applies defaults (that's applyDefaults' job) — calling
// it on a config with zero-valued optional fields must leave them zero.
func TestConfig_Validate_DoesNotMutate(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.APIBufferSize = 0
	c.APIReconnectDelay = 0

	// APIBufferSize/APIReconnectDelay at zero don't fail Validate on their
	// own (see TestConfig_Validate_AcceptsZeroOptionalNumericFields) — this
	// test only cares that Validate leaves them exactly as given either way.
	_ = c.Validate()

	if c.APIBufferSize != 0 {
		t.Errorf("expected Validate to leave APIBufferSize untouched, got %d", c.APIBufferSize)
	}
	if c.APIReconnectDelay != 0 {
		t.Errorf("expected Validate to leave APIReconnectDelay untouched, got %v", c.APIReconnectDelay)
	}
}

// TestConfig_Validate_AcceptsZeroOptionalNumericFields: unlike
// FIEFilterPolicy, zero isn't out-of-bounds for these fields, so Validate
// alone can't tell "unset" from "meant zero" — applyDefaults is what
// actually resolves it, and NewOrch always calls that first.
func TestConfig_Validate_AcceptsZeroOptionalNumericFields(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.APIBufferSize = 0
	c.APIReconnectDelay = 0
	if err := c.Validate(); err != nil {
		t.Fatalf("expected zero-valued optional numeric fields to pass Validate, got: %v", err)
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
		{"empty APIURL", func(c *Config) { c.APIURL = "" }},
		{"APIURL unsupported scheme", func(c *Config) { c.APIURL = "ftp://retina0.lip6.fr:8090" }},
		{"APIURL missing host", func(c *Config) { c.APIURL = "https://" }},
		{"APIURL malformed host", func(c *Config) { c.APIURL = "http://[::1:8090" }},
		{"APIURL root path", func(c *Config) { c.APIURL = "https://retina0.lip6.fr:8090/" }},
		{"APIURL fragment", func(c *Config) { c.APIURL = "https://retina0.lip6.fr:8090/api/v1/ingest#frag" }},
		{"negative APIBufferSize", func(c *Config) { c.APIBufferSize = -1 }},
		{"APIBufferSize implausibly large", func(c *Config) { c.APIBufferSize = 6_000_000 }},
		{"negative APIReconnectDelay", func(c *Config) { c.APIReconnectDelay = -time.Second }},
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

// TestRunScheduler_ContinuesOnNilPDWithValidContext covers the `if pd ==
// nil { continue }` branch specifically — every other test here only gets
// pd==nil via ctx cancellation, which returns before continue is reached.
// Emptying pdMap makes NextPD return nil while ctx is still valid instead.
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

// TestAgentHandler_ConvertFIEError covers the inline modelFIEToAPIv1-then-continue
// branch in agentHandler's FIE loop (replaces the deleted fieStreamHandler
// coverage). Proto3 enums accept any int32 on the wire, so an out-of-range
// IPVersion round-trips fine and only fails at our own range check.
func TestAgentHandler_ConvertFIEError(t *testing.T) {
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

	status := &agentAuthStatus{agentID: "agent-bad-fie"}

	done := make(chan struct{})
	go func() {
		o.agentHandler(status, stream)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)

	badFIE := validWireFIE(1)
	badFIE.IpVersion = wire.IPVersion(256) // out of uint8 range
	badFIE.NearInfo = validWireInfo()
	badFIE.FarInfo = validWireInfo()
	if err := framing.Send(clientConn, 0, badFIE); err != nil {
		t.Fatalf("cannot send bad FIE: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	cancel()
	_ = serverConn.Close()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("agentHandler did not return after a FIE conversion error (expected log-and-continue, not a hang)")
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
// TestFilterFIE_InvalidPolicy is gone — filterFIE no longer returns an
// error, since Config.Validate() rejects an invalid policy before construction.

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

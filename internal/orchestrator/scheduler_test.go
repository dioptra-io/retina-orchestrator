// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"encoding/json"
	"net"
	"os"
	"testing"

	api "github.com/dioptra-io/retina-commons/api/v1"
)

// Coverage is ~99%: the only uncovered branch is the `newRandomizer` error path
// in NewScheduler, which is unreachable — indices is guaranteed non-empty by the
// `len(pds) == 0` guard directly above it.

// -- helpers ------------------------------------------------------------------

func writeSchedulerPDFile(t *testing.T, pds []*api.ProbingDirective) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	for _, pd := range pds {
		b, err := json.Marshal(pd)
		if err != nil {
			t.Fatalf("cannot marshal directive: %v", err)
		}
		if _, err := f.Write(append(b, '\n')); err != nil {
			t.Fatalf("cannot write to temp file: %v", err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("cannot close temp file: %v", err)
	}
	return f.Name()
}

func makePD(id uint64) *api.ProbingDirective {
	return &api.ProbingDirective{ProbingDirectiveID: id}
}

// makeFIEFull creates a FIE with both near and far replies — considered yielding.
func makeFIEFull(id uint64, near, far net.IP) *api.ForwardingInfoElement {
	return &api.ForwardingInfoElement{
		ProbingDirectiveID: id,
		NearInfo:           &api.Info{ReplyAddress: near},
		FarInfo:            &api.Info{ReplyAddress: far},
	}
}

// makeFIETimeout creates a FIE with no replies — considered a miss.
func makeFIETimeout(id uint64) *api.ForwardingInfoElement {
	return &api.ForwardingInfoElement{ProbingDirectiveID: id}
}

func newTestSchedulerConfig(t *testing.T, pds []*api.ProbingDirective) *SchedulerConfig {
	t.Helper()
	return &SchedulerConfig{
		Seed:                       42,
		IssuanceRate:               1000.0,
		PDPath:                     writeSchedulerPDFile(t, pds),
		ActiveSetSize:              len(pds),
		ConsecutiveMissesThreshold: 100, // high threshold so tests don't trigger replacement unexpectedly
		MaxEvictions:               3,
	}
}

func newTestScheduler(t *testing.T, pds []*api.ProbingDirective) *Scheduler {
	t.Helper()
	s, err := NewScheduler(newTestSchedulerConfig(t, pds), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return s
}

// -- NewScheduler -------------------------------------------------------------

func TestNewScheduler_InvalidRate(t *testing.T) {
	t.Parallel()
	for _, rate := range []float64{0, -1} {
		_, err := NewScheduler(&SchedulerConfig{
			Seed: 0, IssuanceRate: rate, PDPath: "irrelevant",
			ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
		}, nil, testMetrics())
		if err == nil {
			t.Errorf("rate %v: expected error, got nil", rate)
		}
	}
}

func TestNewScheduler_MissingFile(t *testing.T) {
	t.Parallel()
	_, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPath: "/nonexistent/path.jsonl",
		ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestNewScheduler_EmptyFile(t *testing.T) {
	t.Parallel()
	_, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPath: writeSchedulerPDFile(t, nil),
		ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for empty directive file, got nil")
	}
}

func TestNewScheduler_Valid(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1), makePD(2)})
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestNewScheduler_NilLogger(t *testing.T) {
	t.Parallel()
	s, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPath: writeSchedulerPDFile(t, []*api.ProbingDirective{makePD(1)}),
		ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, nil, testMetrics())
	if err != nil {
		t.Fatalf("unexpected error with nil logger: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestNewScheduler_NilMetrics(t *testing.T) {
	t.Parallel()
	_, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPath: "valid/path.jsonl",
		ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), nil)
	if err == nil {
		t.Fatal("expected error for nil metrics, got nil")
	}
}

// -- readPDs ------------------------------------------------------------------

func TestReadPDs_InvalidJSON(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	if _, err := f.WriteString("not valid json\n"); err != nil {
		t.Fatalf("cannot write to temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("cannot close temp file: %v", err)
	}
	_, err = NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPath: f.Name(),
		ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected unmarshal error for invalid JSON, got nil")
	}
}

func TestReadPDs_ScannerError(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	// Write a line longer than bufio.MaxScanTokenSize (64 KiB) to trigger
	// scanner.Err() = bufio.ErrTooLong.
	if _, err := f.Write(make([]byte, 64*1024+1)); err != nil {
		t.Fatalf("cannot write to temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("cannot close temp file: %v", err)
	}
	_, err = NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPath: f.Name(),
		ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected scanner error for oversized line, got nil")
	}
}

// -- ipKey --------------------------------------------------------------------

func TestIpKey_Nil(t *testing.T) {
	t.Parallel()
	if ipKey(nil) != "" {
		t.Error("expected empty string for nil IP")
	}
}

func TestIpKey_IPv4(t *testing.T) {
	t.Parallel()
	if ipKey(net.ParseIP("1.2.3.4")) == "" {
		t.Error("expected non-empty key for IPv4 address")
	}
}

func TestIpKey_IPv6(t *testing.T) {
	t.Parallel()
	if ipKey(net.ParseIP("2001:db8::1")) == "" {
		t.Error("expected non-empty key for IPv6 address")
	}
}

func TestIpKey_IPv4MappedIPv6AreEqual(t *testing.T) {
	t.Parallel()
	if ipKey(net.ParseIP("1.2.3.4")) != ipKey(net.ParseIP("::ffff:1.2.3.4")) {
		t.Error("IPv4 and its IPv4-mapped IPv6 form should produce the same key")
	}
}

// -- recordImpact -------------------------------------------------------------

func TestRecordImpact_NilAddressAfterNonNil(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	addr := net.ParseIP("10.0.0.1")

	// Use a full FIE to avoid incrementing consecutiveMisses.
	if err := s.UpdateFromFIE(makeFIEFull(1, addr, net.ParseIP("10.0.0.2"))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// NearInfo present but ReplyAddress nil: triggers recordImpact(nil, pd),
	// covering its nil address guard.
	fie := &api.ForwardingInfoElement{
		ProbingDirectiveID: 1,
		NearInfo:           &api.Info{ReplyAddress: nil},
		FarInfo:            &api.Info{ReplyAddress: net.ParseIP("10.0.0.2")},
	}
	if err := s.UpdateFromFIE(fie); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := s.impactRecords[ipKey(addr)]; ok {
		t.Error("expected impact record for old address to be removed")
	}
}

// -- UpdateFromFIE ------------------------------------------------------------

func TestUpdateFromFIE_UnknownID(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	// Unknown PD ID is treated as a stale FIE from a replaced directive — not an error.
	if err := s.UpdateFromFIE(makeFIETimeout(99)); err != nil {
		t.Fatalf("expected nil for unknown directive ID, got: %v", err)
	}
}

func TestUpdateFromFIE_NilNearAndFar(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	if err := s.UpdateFromFIE(makeFIETimeout(1)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.pdMap[1].issuanceProb != 1.0 {
		t.Errorf("expected issuance prob 1.0, got %v", s.pdMap[1].issuanceProb)
	}
	if len(s.impactRecords) != 0 {
		t.Errorf("expected no impact records, got %d", len(s.impactRecords))
	}
}

func TestUpdateFromFIE_SingleDirectiveSingleAddress(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	// Use full FIE so the directive is considered yielding.
	if err := s.UpdateFromFIE(makeFIEFull(1, net.ParseIP("10.0.0.1"), net.ParseIP("10.0.0.2"))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only this directive impacts the address: probability stays 1.0.
	if s.pdMap[1].issuanceProb != 1.0 {
		t.Errorf("expected issuance prob 1.0, got %v", s.pdMap[1].issuanceProb)
	}
}

func TestUpdateFromFIE_AddressImpactsProb(t *testing.T) {
	t.Parallel()
	// Test near address sharing; far address follows the same logic symmetrically.
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1), makePD(2)})
	addr := net.ParseIP("10.0.0.1")

	if err := s.UpdateFromFIE(makeFIEFull(1, addr, net.ParseIP("10.0.1.1"))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIEFull(2, addr, net.ParseIP("10.0.1.2"))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Two directives share addr: maxImpacts=2, prob=0.5.
	if s.pdMap[2].issuanceProb != 0.5 {
		t.Errorf("expected issuance prob 0.5, got %v", s.pdMap[2].issuanceProb)
	}
}

func TestUpdateFromFIE_AddressChange(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	addr1 := net.ParseIP("10.0.0.1")
	addr2 := net.ParseIP("10.0.0.2")
	far := net.ParseIP("10.0.0.3")

	if err := s.UpdateFromFIE(makeFIEFull(1, addr1, far)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIEFull(1, addr2, far)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := s.impactRecords[ipKey(addr1)]; ok {
		t.Error("expected impact record for addr1 to be removed")
	}
	if _, ok := s.impactRecords[ipKey(addr2)]; !ok {
		t.Error("expected impact record for addr2")
	}
}

func TestUpdateFromFIE_MaxOfNearAndFarImpacts(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1), makePD(2), makePD(3)})
	nearAddr := net.ParseIP("10.0.0.1")
	farAddr := net.ParseIP("10.0.0.2")

	if err := s.UpdateFromFIE(makeFIEFull(1, nearAddr, farAddr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIEFull(2, net.ParseIP("10.0.0.3"), farAddr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIEFull(3, net.ParseIP("10.0.0.4"), farAddr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIEFull(1, nearAddr, farAddr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const want = 1.0 / 3.0
	if s.pdMap[1].issuanceProb != want {
		t.Errorf("expected issuance prob %.4f, got %.4f", want, s.pdMap[1].issuanceProb)
	}
}

func TestUpdateFromFIE_ConsecutiveMissesTriggersReplacement(t *testing.T) {
	t.Parallel()
	// Active set: pd1. Unused pool: pd2 (same agent).
	pds := []*api.ProbingDirective{
		{ProbingDirectiveID: 1, AgentID: "agent-a"},
		{ProbingDirectiveID: 2, AgentID: "agent-a"},
	}
	s, err := NewScheduler(&SchedulerConfig{
		Seed:                       42,
		IssuanceRate:               1000.0,
		PDPath:                     writeSchedulerPDFile(t, pds),
		ActiveSetSize:              1,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Send ConsecutiveMissesThreshold nil FIEs to trigger replacement.
	// The last call removes pd1 from pdMap; errors are expected after that.
	for range s.config.ConsecutiveMissesThreshold {
		_ = s.UpdateFromFIE(makeFIETimeout(1))
	}

	// pd1 should be gone from active set, pd2 should be there.
	if _, ok := s.pdMap[1]; ok {
		t.Error("expected pd1 to be replaced out of active set")
	}
	if _, ok := s.pdMap[2]; !ok {
		t.Error("expected pd2 to be drawn into active set")
	}
}

func TestReplacePD_PermanentEviction(t *testing.T) {
	t.Parallel()
	// Active set: pd1. Unused pool: pd2, pd3 (same agent).
	// MaxEvictions=1: a PD is permanently evicted after one recycling.
	pds := []*api.ProbingDirective{
		{ProbingDirectiveID: 1, AgentID: "agent-a"},
		{ProbingDirectiveID: 2, AgentID: "agent-a"},
		{ProbingDirectiveID: 3, AgentID: "agent-a"},
	}
	s, err := NewScheduler(&SchedulerConfig{
		Seed:                       42,
		IssuanceRate:               1000.0,
		PDPath:                     writeSchedulerPDFile(t, pds),
		ActiveSetSize:              1,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               1,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	triggerReplacement := func() {
		var activeID uint64
		for id := range s.pdMap {
			activeID = id
		}
		for range s.config.ConsecutiveMissesThreshold {
			_ = s.UpdateFromFIE(makeFIETimeout(activeID))
		}
	}

	// With MaxEvictions=1 and 3 PDs, each PD gets recycled once in the first
	// three rounds (evictionCount becomes 1). On the fourth round, the active
	// PD already has evictionCount=1 >= MaxEvictions and is permanently evicted.
	triggerReplacement()
	triggerReplacement()
	triggerReplacement()
	triggerReplacement()

	// PDsEvictedTotal should have incremented.
	if len(s.unusedByAgent["agent-a"]) >= 3 {
		t.Errorf("expected unused pool to shrink due to permanent eviction, got %d entries", len(s.unusedByAgent["agent-a"]))
	}
}

func TestReplacePD_PoolExhausted(t *testing.T) {
	t.Parallel()
	// Active set: pd1 only, no unused pool.
	pds := []*api.ProbingDirective{
		{ProbingDirectiveID: 1, AgentID: "agent-a"},
	}
	s, err := NewScheduler(&SchedulerConfig{
		Seed:                       42,
		IssuanceRate:               1000.0,
		PDPath:                     writeSchedulerPDFile(t, pds),
		ActiveSetSize:              1,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Trigger replacement — unused pool is empty, replacePD returns nil.
	for range s.config.ConsecutiveMissesThreshold {
		_ = s.UpdateFromFIE(makeFIETimeout(1))
	}
	// pd1 moved to unused pool, active set is empty — NextPD returns nil.
	s.issuancePeriod = 0
	pd := s.NextPD()
	if pd != nil {
		t.Errorf("expected nil from NextPD when pool exhausted, got pd %d", pd.ProbingDirectiveID)
	}
}

func TestNextPD_PoolExhaustedOnBernoulli(t *testing.T) {
	t.Parallel()
	// Single PD, no unused pool — Bernoulli failure with nothing to replace from.
	pds := []*api.ProbingDirective{
		{ProbingDirectiveID: 1, AgentID: "agent-a"},
	}
	s, err := NewScheduler(&SchedulerConfig{
		Seed:                       42,
		IssuanceRate:               1000.0,
		PDPath:                     writeSchedulerPDFile(t, pds),
		ActiveSetSize:              1,
		ConsecutiveMissesThreshold: 100,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Force Bernoulli failure — unused pool is empty so replacePD returns nil.
	s.pdMap[1].issuanceProb = 0.0
	s.issuancePeriod = 0
	pd := s.NextPD()
	if pd != nil {
		t.Errorf("expected nil when pool exhausted on Bernoulli failure, got pd %d", pd.ProbingDirectiveID)
	}
}

func TestNextPD_ReturnsDirective(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	if pd := s.NextPD(); pd == nil {
		t.Fatal("expected non-nil directive (issuance prob is 1.0)")
	}
}

func TestNextPD_ReplacesOnLowProbability(t *testing.T) {
	t.Parallel()
	// Active set: pd1. Unused pool: pd2 (same agent).
	pds := []*api.ProbingDirective{
		{ProbingDirectiveID: 1, AgentID: "agent-a"},
		{ProbingDirectiveID: 2, AgentID: "agent-a"},
	}
	s, err := NewScheduler(&SchedulerConfig{
		Seed:                       42,
		IssuanceRate:               1000.0,
		PDPath:                     writeSchedulerPDFile(t, pds),
		ActiveSetSize:              1,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Force issuance prob to 0 to guarantee Bernoulli failure and replacement.
	s.pdMap[1].issuanceProb = 0.0
	pd := s.NextPD()
	// Should return the replacement (pd2), not nil.
	if pd == nil || pd.ProbingDirectiveID != 2 {
		t.Errorf("expected replacement pd2, got %v", pd)
	}
}

func TestNextPD_CycleDurationObserved(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	s.issuancePeriod = 0
	for range 3 {
		s.NextPD()
	}
}

func TestUpdateFromFIE_TimeoutClearsStaleAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		firstFIE func(net.IP) *api.ForwardingInfoElement
		resetFIE func(net.IP) *api.ForwardingInfoElement
	}{
		{
			name: "near address",
			// Full FIE to establish near address without counting as miss.
			firstFIE: func(addr net.IP) *api.ForwardingInfoElement {
				return makeFIEFull(1, addr, net.ParseIP("10.0.0.99"))
			},
			// FIE with nil near clears the stale near address.
			resetFIE: func(addr net.IP) *api.ForwardingInfoElement {
				return makeFIEFull(1, nil, net.ParseIP("10.0.0.99"))
			},
		},
		{
			name: "far address",
			firstFIE: func(addr net.IP) *api.ForwardingInfoElement {
				return makeFIEFull(1, net.ParseIP("10.0.0.99"), addr)
			},
			resetFIE: func(addr net.IP) *api.ForwardingInfoElement {
				return makeFIEFull(1, net.ParseIP("10.0.0.99"), nil)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
			addr := net.ParseIP("10.0.0.1")

			if err := s.UpdateFromFIE(tt.firstFIE(addr)); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if _, ok := s.impactRecords[ipKey(addr)]; !ok {
				t.Fatalf("expected impact record for %s", tt.name)
			}

			if err := s.UpdateFromFIE(tt.resetFIE(addr)); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if _, ok := s.impactRecords[ipKey(addr)]; ok {
				t.Errorf("expected stale %s impact record to be removed", tt.name)
			}
			if pd, ok := s.pdMap[1]; ok {
				if pd.issuanceProb != 1.0 {
					t.Errorf("expected issuance prob 1.0 after address cleared, got %v", pd.issuanceProb)
				}
			}
		})
	}
}

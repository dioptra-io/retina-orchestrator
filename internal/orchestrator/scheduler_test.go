// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/model"
	wire "github.com/dioptra-io/retina-commons/wire/v2"
	"google.golang.org/protobuf/encoding/protojson"
)

// Coverage is ~99%: the only uncovered branches are:
//   - NewScheduler's `newRandomizer` error path, unreachable — indices is
//     guaranteed non-empty by the `len(v4pds) == 0 && len(v6pds) == 0`
//     guard directly above it.
//   - NextPD's busy-wait loop's runtime.Gosched() branch (the sub-100µs
//     tail of the Sleep-vs-Gosched split). Deterministically landing
//     `remaining` inside that narrow window isn't reliably testable —
//     time.Sleep only guarantees sleeping at least the requested
//     duration, not precisely it — and the branch itself is a pure
//     scheduling hint with no behavioral effect, unlike the rest of
//     NextPD's wait/cancellation/stale-pd logic, which is covered.

// -- helpers ------------------------------------------------------------------

func writeSchedulerPDFile(t *testing.T, pds []*wire.ProbingDirective) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	for _, pd := range pds {
		b, err := protojson.Marshal(pd)
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

// makePD/makePDV4/makePDV6 return *wire.ProbingDirective (not *model) since
// their only use is being written to a file and read back through
// readPDs() — no need to round-trip through model's net.IP/uint8 typing
// for that. DestinationAddress is required now: model.ProbingDirectiveFromProto
// (called inside readPDs) rejects an empty one, unlike the old api.ProbingDirective.
func makePD(id uint64) *wire.ProbingDirective {
	return &wire.ProbingDirective{ProbingDirectiveId: id, IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.1"}
}

//nolint:unparam // id is always 1 in current tests but is a meaningful parameter
func makePDV4(id uint64) *wire.ProbingDirective {
	return &wire.ProbingDirective{ProbingDirectiveId: id, IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.1"}
}

//nolint:unparam // id is always 1 in current tests but is a meaningful parameter
func makePDV6(id uint64) *wire.ProbingDirective {
	return &wire.ProbingDirective{ProbingDirectiveId: id, IpVersion: wire.IPVersion_IP_VERSION_IPV6, DestinationAddress: "2001:db8::1"}
}

// makeFIEFull/makeFIETimeout return *model.ForwardingInfoElement, since
// they're passed directly to UpdateFromFIE — a plain in-memory function
// call that reads struct fields directly, with no ToProto/FromProto
// conversion involved. Unlike the PD helpers above, no required-field
// validation applies here, so these stay minimal, matching the originals.

// makeFIEFull creates a FIE with both near and far replies — considered yielding.
func makeFIEFull(id uint64, near, far net.IP) *model.ForwardingInfoElement {
	return &model.ForwardingInfoElement{
		ProbingDirectiveID: id,
		NearInfo:           &model.Info{ReplyAddress: near},
		FarInfo:            &model.Info{ReplyAddress: far},
	}
}

// makeFIETimeout creates a FIE with no replies — considered a miss.
func makeFIETimeout(id uint64) *model.ForwardingInfoElement {
	return &model.ForwardingInfoElement{ProbingDirectiveID: id}
}

func newTestSchedulerConfig(t *testing.T, pds []*wire.ProbingDirective) *SchedulerConfig {
	t.Helper()
	// Split pds by IP version for the two-file approach.
	var v4pds, v6pds []*wire.ProbingDirective
	for _, pd := range pds {
		if pd.IpVersion == wire.IPVersion_IP_VERSION_IPV6 {
			v6pds = append(v6pds, pd)
		} else {
			v4pds = append(v4pds, pd)
		}
	}
	// ActiveSetSize * 2 ensures all PDs go into the active set regardless of
	// protocol split — halfActive = len(pds), so all PDs from each file are active.
	return &SchedulerConfig{
		Seed:                       42,
		IssuanceRate:               1000.0,
		PDPathV4:                   writeSchedulerPDFile(t, v4pds),
		PDPathV6:                   writeSchedulerPDFile(t, v6pds),
		ImpactThreshold:            1.0,
		ActiveSetSize:              len(pds) * 2,
		ConsecutiveMissesThreshold: 100, // high threshold so tests don't trigger replacement unexpectedly
		MaxEvictions:               3,
	}
}

func newTestScheduler(t *testing.T, pds []*wire.ProbingDirective) *Scheduler {
	t.Helper()
	s, err := NewScheduler(newTestSchedulerConfig(t, pds), testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return s
}

// newTestSchedulerWithConfig creates a scheduler with explicit V4 file and config.
// V6 pool is empty — use newTestScheduler for mixed protocol tests.
//
//nolint:unparam // activeSetSize is always 1 in current tests but is a meaningful parameter
func newTestSchedulerWithConfig(t *testing.T, v4pds []*wire.ProbingDirective, activeSetSize, missingThreshold, maxEvictions int) *Scheduler {
	t.Helper()
	s, err := NewScheduler(&SchedulerConfig{
		Seed:                       42,
		IssuanceRate:               1000.0,
		PDPathV4:                   writeSchedulerPDFile(t, v4pds),
		ImpactThreshold:            1.0,
		ActiveSetSize:              activeSetSize,
		ConsecutiveMissesThreshold: missingThreshold,
		MaxEvictions:               maxEvictions,
	}, testLogger(), testMetrics())
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
			Seed: 0, IssuanceRate: rate, PDPathV4: "irrelevant",
			ImpactThreshold: 1.0, ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
		}, nil, testMetrics())
		if err == nil {
			t.Errorf("rate %v: expected error, got nil", rate)
		}
	}
}

func TestNewScheduler_BothFilesMissing(t *testing.T) {
	t.Parallel()
	_, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPathV4: "/nonexistent/v4.jsonl", PDPathV6: "/nonexistent/v6.jsonl",
		ImpactThreshold: 1.0, ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for missing files, got nil")
	}
}

func TestNewScheduler_EmptyFiles(t *testing.T) {
	t.Parallel()
	_, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0,
		PDPathV4:                   writeSchedulerPDFile(t, nil),
		PDPathV6:                   writeSchedulerPDFile(t, nil),
		ActiveSetSize:              1,
		ImpactThreshold:            1.0,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for empty files, got nil")
	}
}

func TestNewScheduler_DuplicatePDID(t *testing.T) {
	t.Parallel()
	// Same PD ID in both V4 and V6 files should be rejected.
	_, err := NewScheduler(&SchedulerConfig{
		Seed:                       0,
		IssuanceRate:               1.0,
		PDPathV4:                   writeSchedulerPDFile(t, []*wire.ProbingDirective{makePDV4(1)}),
		PDPathV6:                   writeSchedulerPDFile(t, []*wire.ProbingDirective{makePDV6(1)}),
		ActiveSetSize:              2,
		ImpactThreshold:            1.0,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for duplicate PD ID across V4 and V6 files, got nil")
	}
}

func TestNewScheduler_DuplicatePDIDWithinFile(t *testing.T) {
	t.Parallel()
	// Same PD ID appearing twice in the same file should be rejected.
	_, err := NewScheduler(&SchedulerConfig{
		Seed:                       0,
		IssuanceRate:               1.0,
		PDPathV4:                   writeSchedulerPDFile(t, []*wire.ProbingDirective{makePDV4(1), makePDV4(1)}),
		ActiveSetSize:              2,
		ImpactThreshold:            1.0,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for duplicate PD ID within V4 file, got nil")
	}
}

func TestNewScheduler_BadV6File(t *testing.T) {
	t.Parallel()
	_, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0,
		PDPathV6:                   "/nonexistent/v6.jsonl",
		ActiveSetSize:              1,
		ImpactThreshold:            1.0,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for missing V6 file, got nil")
	}
}

func TestNewScheduler_V6UnusedPool(t *testing.T) {
	t.Parallel()
	// Three V6 PDs with ActiveSetSize=2: two active, one in unused pool.
	// This exercises ipVersionLabel("6") in the PDsUnusedTotal metric initialization.
	s, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0,
		PDPathV6: writeSchedulerPDFile(t, []*wire.ProbingDirective{
			makePDV6(1),
			makePDV6(2),
			makePDV6(3),
		}),
		ActiveSetSize:              2,
		ImpactThreshold:            1.0,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestNewScheduler_OnlyV4(t *testing.T) {
	t.Parallel()
	s, err := NewScheduler(&SchedulerConfig{
		Seed:                       0,
		IssuanceRate:               1.0,
		PDPathV4:                   writeSchedulerPDFile(t, []*wire.ProbingDirective{makePDV4(1)}),
		PDPathV6:                   "",
		ActiveSetSize:              1,
		ImpactThreshold:            1.0,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error with V4 only: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestNewScheduler_OnlyV6(t *testing.T) {
	t.Parallel()
	s, err := NewScheduler(&SchedulerConfig{
		Seed:                       0,
		IssuanceRate:               1.0,
		PDPathV4:                   "",
		PDPathV6:                   writeSchedulerPDFile(t, []*wire.ProbingDirective{makePDV6(1)}),
		ActiveSetSize:              1,
		ImpactThreshold:            1.0,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error with V6 only: %v", err)
	}
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestNewScheduler_Valid(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1), makePD(2)})
	if s == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestNewScheduler_NilLogger(t *testing.T) {
	t.Parallel()
	s, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0,
		PDPathV4:                   writeSchedulerPDFile(t, []*wire.ProbingDirective{makePDV4(1)}),
		ActiveSetSize:              1,
		ImpactThreshold:            1.0,
		ConsecutiveMissesThreshold: 3,
		MaxEvictions:               3,
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
		Seed: 0, IssuanceRate: 1.0, PDPathV4: "valid/path.jsonl",
		ImpactThreshold: 1.0, ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
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
		Seed: 0, IssuanceRate: 1.0, PDPathV4: f.Name(),
		ImpactThreshold: 1.0, ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
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
	// Write a line longer than the scanner's configured max (4 MiB, set in
	// scheduler.go's readPDs to accommodate legitimately large directives —
	// raised from bufio.Scanner's 64 KiB default) to trigger scanner.Err().
	if _, err := f.Write(make([]byte, 4*1024*1024+1)); err != nil {
		t.Fatalf("cannot write to temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("cannot close temp file: %v", err)
	}
	_, err = NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPathV4: f.Name(),
		ImpactThreshold: 1.0, ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected scanner error for oversized line, got nil")
	}
}

func TestReadPDs_SkipsBlankLines(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	pd := makePDV4(1)
	b, _ := protojson.Marshal(pd)
	// Write blank line before and after a valid PD.
	if _, err := f.WriteString("\n"); err != nil {
		t.Fatalf("cannot write to temp file: %v", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		t.Fatalf("cannot write to temp file: %v", err)
	}
	if _, err := f.WriteString("\n"); err != nil {
		t.Fatalf("cannot write to temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("cannot close temp file: %v", err)
	}
	s, err := NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPathV4: f.Name(),
		ImpactThreshold: 1.0, ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), testMetrics())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(s.pdMap) != 1 {
		t.Errorf("expected 1 PD loaded, got %d", len(s.pdMap))
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

// TestIpKey_MalformedLength covers the ipKey fix where a non-nil net.IP
// of an invalid length (neither 4 nor 16 bytes, so To16() returns nil)
// is treated as absent rather than stringifying To16()'s nil result.
// Nothing in the original test set exercised this case.
func TestIpKey_MalformedLength(t *testing.T) {
	t.Parallel()
	garbage := net.IP{1, 2, 3}
	if ipKey(garbage) != "" {
		t.Error("expected empty string for malformed-length IP")
	}
}

// -- recordImpact -------------------------------------------------------------

func TestRecordImpact_NilAddressAfterNonNil(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1)})
	addr := net.ParseIP("10.0.0.1")

	// Use a full FIE to avoid incrementing consecutiveMisses.
	if err := s.UpdateFromFIE(makeFIEFull(1, addr, net.ParseIP("10.0.0.2"))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// NearInfo present but ReplyAddress nil: triggers recordImpact(nil, pd),
	// covering its nil address guard.
	fie := &model.ForwardingInfoElement{
		ProbingDirectiveID: 1,
		NearInfo:           &model.Info{ReplyAddress: nil},
		FarInfo:            &model.Info{ReplyAddress: net.ParseIP("10.0.0.2")},
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
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1)})
	// Unknown PD ID is treated as a stale FIE from a replaced directive — not an error.
	if err := s.UpdateFromFIE(makeFIETimeout(99)); err != nil {
		t.Fatalf("expected nil for unknown directive ID, got: %v", err)
	}
}

func TestUpdateFromFIE_NilNearAndFar(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1)})
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
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1)})
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
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1), makePD(2)})
	addr := net.ParseIP("10.0.0.1")

	if err := s.UpdateFromFIE(makeFIEFull(1, addr, net.ParseIP("10.0.1.1"))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIEFull(2, addr, net.ParseIP("10.0.1.2"))); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Two directives share addr: maxImpacts=2, prob = min(1, impactThreshold * cycleDuration / 2).
	// cycleDuration uses the actual active-set size (len(s.pdMap)), not the
	// configured target (s.config.ActiveSetSize) — see UpdateFromFIE. Here
	// they differ: ActiveSetSize is len(pds)*2, but both PDs are the same
	// protocol, so the half-split logic gives all slots to that protocol and
	// both PDs load — len(s.pdMap) is 2, not the configured 4.
	cycleDuration := float64(len(s.pdMap)) / s.config.IssuanceRate
	wantProb := min(1.0, s.config.ImpactThreshold*cycleDuration/2.0)
	if s.pdMap[2].issuanceProb != wantProb {
		t.Errorf("expected issuance prob %.6f, got %v", wantProb, s.pdMap[2].issuanceProb)
	}
}

func TestUpdateFromFIE_AddressChange(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1)})
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
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1), makePD(2), makePD(3)})
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
	// cycleDuration uses the actual active-set size (len(s.pdMap)), not the
	// configured target — same reasoning as TestUpdateFromFIE_AddressImpactsProb.
	cycleDuration := float64(len(s.pdMap)) / s.config.IssuanceRate
	want := min(1.0, s.config.ImpactThreshold*cycleDuration/3.0)
	if s.pdMap[1].issuanceProb != want {
		t.Errorf("expected issuance prob %.6f, got %.6f", want, s.pdMap[1].issuanceProb)
	}
}

func TestUpdateFromFIE_ConsecutiveMissesTriggersReplacement(t *testing.T) {
	t.Parallel()
	// Active set: pd1 (V4). Unused pool: pd2 (V4, same agent).
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{
			{ProbingDirectiveId: 1, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.1"},
			{ProbingDirectiveId: 2, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.2"},
		},
		1, 3, 3)

	// Send ConsecutiveMissesThreshold nil FIEs to trigger replacement.
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

// -- replacePD ----------------------------------------------------------------

func TestReplacePD_PermanentEviction(t *testing.T) {
	t.Parallel()
	// Active set: pd1 (V4). Unused pool: pd2, pd3 (V4, same agent).
	// MaxEvictions=1: a PD is permanently evicted after one recycling.
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{
			{ProbingDirectiveId: 1, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.1"},
			{ProbingDirectiveId: 2, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.2"},
			{ProbingDirectiveId: 3, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.3"},
		},
		1, 3, 1)

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

	// Unused pool should have shrunk due to permanent eviction.
	if len(s.unusedByAgent["agent-a"][0]) >= 3 {
		t.Errorf("expected unused pool to shrink due to permanent eviction, got %d entries", len(s.unusedByAgent["agent-a"][0]))
	}
}

func TestReplacePD_PoolExhausted(t *testing.T) {
	t.Parallel()
	// Active set: pd1 only, no unused pool.
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{
			{ProbingDirectiveId: 1, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.1"},
		},
		1, 3, 3)

	// Trigger replacement — unused pool is empty, replacePD returns nil.
	for range s.config.ConsecutiveMissesThreshold {
		_ = s.UpdateFromFIE(makeFIETimeout(1))
	}
	// pd1 moved to unused pool, active set is empty — NextPD returns nil.
	s.issuancePeriod = 0
	pd := s.NextPD(context.Background())
	if pd != nil {
		t.Errorf("expected nil from NextPD when pool exhausted, got pd %d", pd.ProbingDirectiveID)
	}
}

// -- NextPD -------------------------------------------------------------------

func TestNextPD_PoolExhaustedOnBernoulli(t *testing.T) {
	t.Parallel()
	// Single PD, no unused pool — Bernoulli failure with nothing to replace from.
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{
			{ProbingDirectiveId: 1, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.1"},
		},
		1, 100, 3)

	// Force Bernoulli failure — unused pool is empty so replacePD returns nil.
	s.pdMap[1].issuanceProb = 0.0
	s.issuancePeriod = 0
	pd := s.NextPD(context.Background())
	if pd != nil {
		t.Errorf("expected nil when pool exhausted on Bernoulli failure, got pd %d", pd.ProbingDirectiveID)
	}
}

func TestNextPD_ReturnsDirective(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1)})
	if pd := s.NextPD(context.Background()); pd == nil {
		t.Fatal("expected non-nil directive (issuance prob is 1.0)")
	}
}

func TestNextPD_ReplacesOnLowProbability(t *testing.T) {
	t.Parallel()
	// Active set: pd1 (V4). Unused pool: pd2 (V4, same agent).
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{
			{ProbingDirectiveId: 1, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.1"},
			{ProbingDirectiveId: 2, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.2"},
		},
		1, 3, 3)

	// Force issuance prob to 0 to guarantee Bernoulli failure and replacement.
	s.pdMap[1].issuanceProb = 0.0
	pd := s.NextPD(context.Background())
	// Should return the replacement (pd2), not nil.
	if pd == nil || pd.ProbingDirectiveID != 2 {
		t.Errorf("expected replacement pd2, got %v", pd)
	}
}

func TestNextPD_CycleDurationObserved(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1)})
	s.issuancePeriod = 0
	for range 3 {
		s.NextPD(context.Background())
	}
}

func TestUpdateFromFIE_TimeoutClearsStaleAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		firstFIE func(net.IP) *model.ForwardingInfoElement
		resetFIE func(net.IP) *model.ForwardingInfoElement
	}{
		{
			name: "near address",
			firstFIE: func(addr net.IP) *model.ForwardingInfoElement {
				return makeFIEFull(1, addr, net.ParseIP("10.0.0.99"))
			},
			resetFIE: func(addr net.IP) *model.ForwardingInfoElement {
				return makeFIEFull(1, nil, net.ParseIP("10.0.0.99"))
			},
		},
		{
			name: "far address",
			firstFIE: func(addr net.IP) *model.ForwardingInfoElement {
				return makeFIEFull(1, net.ParseIP("10.0.0.99"), addr)
			},
			resetFIE: func(addr net.IP) *model.ForwardingInfoElement {
				return makeFIEFull(1, net.ParseIP("10.0.0.99"), nil)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := newTestScheduler(t, []*wire.ProbingDirective{makePD(1)})
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

// -- Additional coverage -------------------------------------------------------

func TestNewScheduler_NilConfig(t *testing.T) {
	t.Parallel()
	if _, err := NewScheduler(nil, testLogger(), testMetrics()); err == nil {
		t.Fatal("expected error for nil config, got nil")
	}
}

// TestReadPDs_InvalidDirective covers a line that's syntactically valid
// protojson but semantically rejected by model.ProbingDirectiveFromProto
// (missing destination_address) — distinct from TestReadPDs_InvalidJSON,
// which fails earlier, at the protojson syntax level.
func TestReadPDs_InvalidDirective(t *testing.T) {
	t.Parallel()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	pd := &wire.ProbingDirective{ProbingDirectiveId: 1} // no destination_address
	b, err := protojson.Marshal(pd)
	if err != nil {
		t.Fatalf("cannot marshal PD: %v", err)
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		t.Fatalf("cannot write to temp file: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("cannot close temp file: %v", err)
	}
	_, err = NewScheduler(&SchedulerConfig{
		Seed: 0, IssuanceRate: 1.0, PDPathV4: f.Name(),
		ImpactThreshold: 1.0, ActiveSetSize: 1, ConsecutiveMissesThreshold: 3, MaxEvictions: 3,
	}, testLogger(), testMetrics())
	if err == nil {
		t.Fatal("expected error for PD missing destination_address, got nil")
	}
}

// TestNextPD_TimerBasedWait exercises NextPD's timer/select branch
// (issuancePeriod >= 10ms) — every other NextPD test uses IssuanceRate
// high enough (or issuancePeriod set directly to 0) to take the busy-wait
// branch instead, leaving this one entirely uncovered otherwise. A fresh
// scheduler's zero-value lastIssuance puts nextTime in the past, so the
// timer fires immediately — this exercises the timer.C success case
// without an actual multi-millisecond test.
func TestNextPD_TimerBasedWait(t *testing.T) {
	t.Parallel()
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{makePD(1)},
		1, 3, 3)
	s.issuancePeriod = 20 * time.Millisecond

	if pd := s.NextPD(context.Background()); pd == nil {
		t.Fatal("expected non-nil directive")
	}
}

// TestNextPD_ContextCanceledDuringTimerWait exercises the ctx.Done() case
// of the same branch. lastIssuance is set to now (not left at its
// zero-value default) so nextTime is genuinely in the future — otherwise
// the timer would fire immediately regardless of issuancePeriod, racing
// with ctx.Done() instead of deterministically testing cancellation.
func TestNextPD_ContextCanceledDuringTimerWait(t *testing.T) {
	t.Parallel()
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{makePD(1)},
		1, 3, 3)
	s.issuancePeriod = time.Second
	s.lastIssuance = time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if pd := s.NextPD(ctx); pd != nil {
		t.Errorf("expected nil after context cancellation, got pd %d", pd.ProbingDirectiveID)
	}
}

// TestNextPD_BusyWaitBranches exercises the inner Sleep-vs-Gosched split
// inside the busy-wait loop. Every other busy-wait test uses
// issuancePeriod=0, where remaining<=0 is true on the first iteration and
// the loop breaks before ever reaching this split — this needs a genuinely
// positive, sub-10ms remaining duration to reach it at all.
func TestNextPD_BusyWaitBranches(t *testing.T) {
	t.Parallel()
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{makePD(1)},
		1, 3, 3)
	s.issuancePeriod = 2 * time.Millisecond // < 10ms: busy-wait branch
	s.lastIssuance = time.Now()             // nextTime genuinely in the future

	if pd := s.NextPD(context.Background()); pd == nil {
		t.Fatal("expected non-nil directive")
	}
}

// TestNextPD_StalePDDuringWait exercises the actual race-condition guard
// the earlier concurrency fix added: pd is selected, then replaced by a
// concurrent call (simulating UpdateFromFIE's consecutive-miss
// replacement) while NextPD is still waiting — NextPD must detect this
// and return nil rather than acting on a stale pdState. This needs real
// goroutine timing, unlike everything else in this file.
func TestNextPD_StalePDDuringWait(t *testing.T) {
	t.Parallel()
	// Active set: pd1. Unused pool: pd2 (same agent/protocol) for replacePD
	// to draw from.
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{
			{ProbingDirectiveId: 1, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.1"},
			{ProbingDirectiveId: 2, AgentId: "agent-a", IpVersion: wire.IPVersion_IP_VERSION_IPV4, DestinationAddress: "192.0.2.2"},
		},
		1, 3, 3)

	// Force Bernoulli failure once the wait completes, so NextPD takes the
	// replace path and reaches the stale-pd check.
	s.pdMap[1].issuanceProb = 0.0
	s.issuancePeriod = 100 * time.Millisecond
	s.lastIssuance = time.Now()

	resultCh := make(chan *model.ProbingDirective, 1)
	go func() {
		resultCh <- s.NextPD(context.Background())
	}()

	// Let NextPD pass selection and enter its wait, then replace pd1 out
	// from under it — the same effect a concurrent UpdateFromFIE call
	// would have.
	time.Sleep(20 * time.Millisecond)
	s.mutex.Lock()
	s.replacePD(s.pdMap[1])
	s.mutex.Unlock()

	if pd := <-resultCh; pd != nil {
		t.Errorf("expected nil (pd already replaced concurrently), got pd %d", pd.ProbingDirectiveID)
	}
}

// TestNextPD_ContextCanceledDuringBusyWait exercises the busy-wait loop's
// ctx.Err() check specifically — distinct from TestNextPD_BusyWaitBranches,
// which lets the loop run to natural completion and never cancels mid-loop.
// A 1ms deadline against a 5ms period gives comfortable margin for the
// loop to observe the expired context before remaining<=0 would anyway.
func TestNextPD_ContextCanceledDuringBusyWait(t *testing.T) {
	t.Parallel()
	s := newTestSchedulerWithConfig(t,
		[]*wire.ProbingDirective{makePD(1)},
		1, 3, 3)
	s.issuancePeriod = 5 * time.Millisecond // < 10ms: busy-wait branch
	s.lastIssuance = time.Now()             // nextTime genuinely in the future

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	if pd := s.NextPD(ctx); pd != nil {
		t.Errorf("expected nil after context cancellation, got pd %d", pd.ProbingDirectiveID)
	}
}

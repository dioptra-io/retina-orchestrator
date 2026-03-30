// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package issuance

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

func writePDFile(t *testing.T, pds []*api.ProbingDirective) string {
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

func makeFIE(id uint64, near, far net.IP) *api.ForwardingInfoElement {
	fie := &api.ForwardingInfoElement{ProbingDirectiveID: id}
	if near != nil {
		fie.NearInfo = &api.Info{ReplyAddress: near}
	}
	if far != nil {
		fie.FarInfo = &api.Info{ReplyAddress: far}
	}
	return fie
}

func newTestScheduler(t *testing.T, pds []*api.ProbingDirective) *Scheduler {
	t.Helper()
	s, err := NewScheduler(42, 1000.0, writePDFile(t, pds), nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return s
}

// -- NewScheduler -------------------------------------------------------------

func TestNewScheduler_InvalidRate(t *testing.T) {
	t.Parallel()
	for _, rate := range []float64{0, -1} {
		_, err := NewScheduler(0, rate, "irrelevant", nil)
		if err == nil {
			t.Errorf("rate %v: expected error, got nil", rate)
		}
	}
}

func TestNewScheduler_MissingFile(t *testing.T) {
	t.Parallel()
	_, err := NewScheduler(0, 1.0, "/nonexistent/path.jsonl", nil)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestNewScheduler_EmptyFile(t *testing.T) {
	t.Parallel()
	_, err := NewScheduler(0, 1.0, writePDFile(t, nil), nil)
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
	_, err = NewScheduler(0, 1.0, f.Name(), nil)
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
	_, err = NewScheduler(0, 1.0, f.Name(), nil)
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

	if err := s.UpdateFromFIE(makeFIE(1, addr, nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// NearInfo present but ReplyAddress nil: triggers recordImpact(nil, pd),
	// covering its nil address guard.
	fie := &api.ForwardingInfoElement{
		ProbingDirectiveID: 1,
		NearInfo:           &api.Info{ReplyAddress: nil},
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
	if err := s.UpdateFromFIE(makeFIE(99, nil, nil)); err == nil {
		t.Fatal("expected error for unknown directive ID, got nil")
	}
}

func TestUpdateFromFIE_NilNearAndFar(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	if err := s.UpdateFromFIE(makeFIE(1, nil, nil)); err != nil {
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
	if err := s.UpdateFromFIE(makeFIE(1, net.ParseIP("10.0.0.1"), nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only this directive impacts the address: probability stays 1.0.
	if s.pdMap[1].issuanceProb != 1.0 {
		t.Errorf("expected issuance prob 1.0, got %v", s.pdMap[1].issuanceProb)
	}
}

func TestUpdateFromFIE_TwoDirectivesSameNearAddress(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1), makePD(2)})
	addr := net.ParseIP("10.0.0.1")

	if err := s.UpdateFromFIE(makeFIE(1, addr, nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIE(2, addr, nil)); err != nil {
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

	if err := s.UpdateFromFIE(makeFIE(1, addr1, nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIE(1, addr2, nil)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := s.impactRecords[ipKey(addr1)]; ok {
		t.Error("expected impact record for addr1 to be removed")
	}
	if _, ok := s.impactRecords[ipKey(addr2)]; !ok {
		t.Error("expected impact record for addr2")
	}
}

func TestUpdateFromFIE_FarAddressImpactsProb(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1), makePD(2)})
	addr := net.ParseIP("10.0.0.1")

	if err := s.UpdateFromFIE(makeFIE(1, nil, addr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIE(2, nil, addr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.pdMap[2].issuanceProb != 0.5 {
		t.Errorf("expected issuance prob 0.5 via far address, got %v", s.pdMap[2].issuanceProb)
	}
}

func TestUpdateFromFIE_MaxOfNearAndFarImpacts(t *testing.T) {
	t.Parallel()
	// pd1 hits nearAddr (1 impact) and farAddr (3 impacts via pd1..pd3):
	// maxImpacts=3, so pd1's prob=1/3.
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1), makePD(2), makePD(3)})
	nearAddr := net.ParseIP("10.0.0.1")
	farAddr := net.ParseIP("10.0.0.2")

	if err := s.UpdateFromFIE(makeFIE(1, nearAddr, farAddr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIE(2, nil, farAddr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := s.UpdateFromFIE(makeFIE(3, nil, farAddr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Re-update pd1 so its probability is recalculated with full impact knowledge.
	if err := s.UpdateFromFIE(makeFIE(1, nearAddr, farAddr)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	const want = 1.0 / 3.0
	if s.pdMap[1].issuanceProb != want {
		t.Errorf("expected issuance prob %.4f, got %.4f", want, s.pdMap[1].issuanceProb)
	}
}

// -- NextPD -------------------------------------------------------------------

func TestNextPD_ReturnsDirective(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	if pd := s.NextPD(); pd == nil {
		t.Fatal("expected non-nil directive (issuance prob is 1.0)")
	}
}

func TestNextPD_SkipsDirective(t *testing.T) {
	t.Parallel()
	s := newTestScheduler(t, []*api.ProbingDirective{makePD(1)})
	s.pdMap[1].issuanceProb = 0.0
	if pd := s.NextPD(); pd != nil {
		t.Fatal("expected nil directive when issuance probability is 0")
	}
}

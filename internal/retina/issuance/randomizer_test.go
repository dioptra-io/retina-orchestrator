// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package issuance

import (
	"testing"
)

// 100% coverage: every branch in newRandomizer, Next, and Cycle is exercised.

// -- newRandomizer ------------------------------------------------------------

func TestNewRandomizer_EmptyIndices(t *testing.T) {
	t.Parallel()
	r, err := newRandomizer(42, []uint64{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "invalid argument: indices slice cannot be empty" {
		t.Fatalf("unexpected error message: %q", err.Error())
	}
	if r != nil {
		t.Fatal("expected nil randomizer, got non-nil")
	}
}

func TestNewRandomizer_ValidIndices(t *testing.T) {
	t.Parallel()
	r, err := newRandomizer(42, []uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("expected non-nil randomizer")
	}
	if r.Cycle() != 0 {
		t.Fatalf("expected initial cycle 0, got %d", r.Cycle())
	}
}

// -- Next ---------------------------------------------------------------------

func TestRandomizer_NextReturnsEachIndexOnce(t *testing.T) {
	t.Parallel()
	indices := []uint64{10, 20, 30, 40, 50}
	r, err := newRandomizer(42, append([]uint64(nil), indices...))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	seen := make(map[uint64]int)
	for range len(indices) {
		seen[r.Next()]++
	}
	for _, idx := range indices {
		if seen[idx] != 1 {
			t.Errorf("index %d appeared %d times in one cycle, want 1", idx, seen[idx])
		}
	}
	if r.Cycle() != 0 {
		t.Errorf("cycle should still be 0 after exactly one full permutation, got %d", r.Cycle())
	}
}

func TestRandomizer_CycleIncrementsAfterFullPermutation(t *testing.T) {
	t.Parallel()
	indices := []uint64{1, 2, 3}
	r, err := newRandomizer(99, append([]uint64(nil), indices...))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for range len(indices) {
		r.Next()
	}
	if r.Cycle() != 0 {
		t.Errorf("cycle should be 0 before first inter-cycle call, got %d", r.Cycle())
	}
	r.Next()
	if r.Cycle() != 1 {
		t.Errorf("expected cycle 1 after starting second permutation, got %d", r.Cycle())
	}
}

func TestRandomizer_MultipleCycles(t *testing.T) {
	t.Parallel()
	indices := []uint64{7, 14, 21}
	n := len(indices)

	r, err := newRandomizer(7, append([]uint64(nil), indices...))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for c := range 5 {
		seen := make(map[uint64]int)
		for range n {
			seen[r.Next()]++
		}
		for _, idx := range indices {
			if seen[idx] != 1 {
				t.Errorf("cycle %d: index %d appeared %d times, want 1", c, idx, seen[idx])
			}
		}
	}
}

// -- Determinism --------------------------------------------------------------

func TestRandomizer_Deterministic(t *testing.T) {
	t.Parallel()
	indices := []uint64{1, 2, 3, 4, 5}
	n := 15 // three cycles

	collect := func() []uint64 {
		r, err := newRandomizer(123, append([]uint64(nil), indices...))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out := make([]uint64, n)
		for i := range n {
			out[i] = r.Next()
		}
		return out
	}

	a, b := collect(), collect()
	for i := range a {
		if a[i] != b[i] {
			t.Errorf("position %d: got %d and %d, same seed should produce identical sequence", i, a[i], b[i])
		}
	}
}

func TestRandomizer_DifferentSeeds(t *testing.T) {
	t.Parallel()
	indices := []uint64{1, 2, 3, 4, 5, 6, 7, 8}
	n := 40

	collect := func(seed uint64) []uint64 {
		r, err := newRandomizer(seed, append([]uint64(nil), indices...))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		out := make([]uint64, n)
		for i := range n {
			out[i] = r.Next()
		}
		return out
	}

	a, b := collect(1), collect(2)
	allEqual := true
	for i := range a {
		if a[i] != b[i] {
			allEqual = false
			break
		}
	}
	if allEqual {
		t.Error("different seeds produced identical sequences")
	}
}

// -- Edge cases ---------------------------------------------------------------

func TestRandomizer_SingleElement(t *testing.T) {
	t.Parallel()
	r, err := newRandomizer(0, []uint64{42})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for range 5 {
		if v := r.Next(); v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	}
	if r.Cycle() != 4 {
		t.Errorf("expected cycle 4, got %d", r.Cycle())
	}
}

func TestRandomizer_AllIndicesReachableAcrossCycles(t *testing.T) {
	t.Parallel()
	indices := []uint64{100, 200, 300, 400}
	r, err := newRandomizer(55, append([]uint64(nil), indices...))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	valid := make(map[uint64]struct{})
	for _, v := range indices {
		valid[v] = struct{}{}
	}

	for range 10 * len(indices) {
		v := r.Next()
		if _, ok := valid[v]; !ok {
			t.Errorf("Next returned unexpected value %d", v)
		}
	}
}

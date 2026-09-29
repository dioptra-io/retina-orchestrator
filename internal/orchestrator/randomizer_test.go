// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"math/rand/v2"
	"testing"
)

// 100% coverage: every branch in newRandomizer, Next, Cycle, Replace, Remove, and Len is exercised.

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

// -- Replace ------------------------------------------------------------------

func TestRandomizer_Replace(t *testing.T) {
	t.Parallel()
	r, err := newRandomizer(42, []uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Replace ID 2 with ID 99
	r.Replace(2, 99)

	// 99 should appear in a full cycle, 2 should not
	seen := make(map[uint64]int)
	for range 3 {
		seen[r.Next()]++
	}
	if seen[2] != 0 {
		t.Error("replaced ID 2 should not appear")
	}
	if seen[99] != 1 {
		t.Errorf("new ID 99 should appear once, got %d", seen[99])
	}
}

func TestRandomizer_ReplaceUnknownID(t *testing.T) {
	t.Parallel()
	r, err := newRandomizer(42, []uint64{1, 2, 3})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be a no-op, not panic
	r.Replace(999, 100)
	seen := make(map[uint64]int)
	for range 3 {
		seen[r.Next()]++
	}
	for _, id := range []uint64{1, 2, 3} {
		if seen[id] != 1 {
			t.Errorf("ID %d should appear once after no-op Replace, got %d", id, seen[id])
		}
	}
}

func TestRandomizer_IndexPDConsistency(t *testing.T) {
	t.Parallel()
	indices := []uint64{10, 20, 30, 40, 50}
	r, err := newRandomizer(42, append([]uint64(nil), indices...))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	checkConsistency := func() {
		for id, pos := range r.indexPD {
			if r.indices[pos] != id {
				t.Errorf("indexPD inconsistency: indexPD[%d]=%d but indices[%d]=%d", id, pos, pos, r.indices[pos])
			}
		}
	}

	// Check after each Next call
	for range len(indices) * 2 {
		r.Next()
		checkConsistency()
	}
}

// -- Remove / Len -------------------------------------------------------------

func removeTestRandomizer(t *testing.T, seed uint64, ids ...uint64) *randomizer {
	t.Helper()
	r, err := newRandomizer(seed, append([]uint64(nil), ids...))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return r
}

// assertRandomizerInvariants checks what Remove must preserve: indices,
// indexPD, and length agree, every ID's recorded position is its real
// position, and the cycle boundary stays in range.
func assertRandomizerInvariants(t *testing.T, r *randomizer) {
	t.Helper()
	if len(r.indices) != r.length || len(r.indexPD) != r.length {
		t.Fatalf("size mismatch: len(indices)=%d length=%d len(indexPD)=%d",
			len(r.indices), r.length, len(r.indexPD))
	}
	for pos, id := range r.indices {
		if got, ok := r.indexPD[id]; !ok || got != pos {
			t.Fatalf("indexPD[%d] = %d (present=%v), want position %d", id, got, ok, pos)
		}
	}
	if r.i < -1 || r.i > r.length-1 {
		t.Fatalf("boundary i=%d out of range for length %d", r.i, r.length)
	}
}

func drawIDs(r *randomizer, n int) []uint64 {
	out := make([]uint64, 0, n)
	for range n {
		out = append(out, r.Next())
	}
	return out
}

// assertSameIDs checks got holds exactly the wanted IDs, each once.
func assertSameIDs(t *testing.T, got []uint64, want ...uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v, want the set %v", got, want)
	}
	seen := make(map[uint64]bool, len(got))
	for _, id := range got {
		if seen[id] {
			t.Fatalf("got %v: %d drawn twice, want the set %v", got, id, want)
		}
		seen[id] = true
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("got %v: missing %d, want the set %v", got, id, want)
		}
	}
}

func TestRandomizer_RemoveUndrawn(t *testing.T) {
	t.Parallel()
	r := removeTestRandomizer(t, 1, 1, 2, 3, 4, 5)

	r.Remove(3)

	assertRandomizerInvariants(t, r)
	if r.Len() != 4 {
		t.Fatalf("expected Len() 4, got %d", r.Len())
	}
	assertSameIDs(t, drawIDs(r, 4), 1, 2, 4, 5)
	if r.Cycle() != 0 {
		t.Errorf("expected the cycle not to advance within the first pass, got %d", r.Cycle())
	}
}

func TestRandomizer_RemoveDrawnMidCycle(t *testing.T) {
	t.Parallel()
	r := removeTestRandomizer(t, 2, 1, 2, 3, 4, 5)
	first, second := r.Next(), r.Next()

	r.Remove(first)

	assertRandomizerInvariants(t, r)
	// The three still-undrawn IDs finish the current cycle, once each.
	var undrawn []uint64
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		if id != first && id != second {
			undrawn = append(undrawn, id)
		}
	}
	assertSameIDs(t, drawIDs(r, 3), undrawn...)
	if r.Cycle() != 0 {
		t.Errorf("expected cycle 0 until the pass completes, got %d", r.Cycle())
	}
	// The next cycle covers every survivor (all but the removed one).
	var survivors []uint64
	for _, id := range []uint64{1, 2, 3, 4, 5} {
		if id != first {
			survivors = append(survivors, id)
		}
	}
	assertSameIDs(t, drawIDs(r, 4), survivors...)
	if r.Cycle() != 1 {
		t.Errorf("expected cycle 1, got %d", r.Cycle())
	}
}

// TestRandomizer_RemoveWhenCycleFullyDrawn covers removal at the moment
// i == -1 (everything drawn, wrap not yet triggered).
func TestRandomizer_RemoveWhenCycleFullyDrawn(t *testing.T) {
	t.Parallel()
	r := removeTestRandomizer(t, 1, 1, 2, 3)
	drawIDs(r, 3)

	r.Remove(2)

	assertRandomizerInvariants(t, r)
	assertSameIDs(t, drawIDs(r, 2), 1, 3)
	if r.Cycle() != 1 {
		t.Errorf("expected the next draws to start cycle 1, got %d", r.Cycle())
	}
}

func TestRandomizer_RemoveUnknownIsNoOp(t *testing.T) {
	t.Parallel()
	r := removeTestRandomizer(t, 1, 1, 2, 3)

	r.Remove(99)

	assertRandomizerInvariants(t, r)
	if r.Len() != 3 {
		t.Errorf("expected Len() 3 after removing an unknown ID, got %d", r.Len())
	}
}

func TestRandomizer_RemoveDownToEmpty(t *testing.T) {
	t.Parallel()
	r := removeTestRandomizer(t, 1, 0, 1, 2) // 0 is a legitimate PD ID
	r.Next()

	for _, id := range []uint64{1, 0, 2} {
		r.Remove(id)
		assertRandomizerInvariants(t, r)
	}
	if r.Len() != 0 {
		t.Fatalf("expected Len() 0, got %d", r.Len())
	}
	r.Remove(1) // removing from an empty randomizer is a no-op
	assertRandomizerInvariants(t, r)
}

// TestRandomizer_RemoveRandomOps interleaves Next and Remove at random and
// checks the properties that matter: only live IDs are drawn, none twice in
// a cycle, and a cycle never wraps until every live ID has been drawn —
// which fails if Remove ever leaves an ID on the wrong side of the boundary.
func TestRandomizer_RemoveRandomOps(t *testing.T) {
	t.Parallel()
	for seed := uint64(0); seed < 300; seed++ {
		ids := make([]uint64, 0, 24)
		alive := make(map[uint64]bool, 24)
		for id := uint64(0); id < 24; id++ {
			ids = append(ids, id)
			alive[id] = true
		}
		r := removeTestRandomizer(t, seed, ids...)
		rng := rand.New(rand.NewPCG(seed, 1)) // #nosec G404
		drawn := make(map[uint64]bool)

		for step := 0; step < 300 && r.Len() > 0; step++ {
			if rng.IntN(3) == 0 {
				var candidates []uint64
				for _, id := range ids {
					if alive[id] {
						candidates = append(candidates, id)
					}
				}
				victim := candidates[rng.IntN(len(candidates))]
				r.Remove(victim)
				delete(alive, victim)
				delete(drawn, victim)
			} else {
				before := r.Cycle()
				id := r.Next()
				if r.Cycle() != before {
					if len(drawn) != len(alive) {
						t.Fatalf("seed %d step %d: cycle advanced with %d of %d live IDs drawn",
							seed, step, len(drawn), len(alive))
					}
					drawn = make(map[uint64]bool)
				}
				if !alive[id] {
					t.Fatalf("seed %d step %d: drew removed ID %d", seed, step, id)
				}
				if drawn[id] {
					t.Fatalf("seed %d step %d: ID %d drawn twice in one cycle", seed, step, id)
				}
				drawn[id] = true
			}
			assertRandomizerInvariants(t, r)
			if r.Len() != len(alive) {
				t.Fatalf("seed %d step %d: Len() = %d, want %d", seed, step, r.Len(), len(alive))
			}
		}
	}
}

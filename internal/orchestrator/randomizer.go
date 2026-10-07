// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"fmt"
	"math/rand/v2"
)

// randomizer provides randomized iteration over a set of uint64 indices using
// the Fisher-Yates shuffle algorithm. Each call to Next returns a unique index
// within the current cycle; once all indices have been returned, a new cycle
// begins and the sequence is reshuffled.
//
// randomizer is not safe for concurrent use. All calls to Next, Replace,
// Remove, Add, Len, and Cycle must be made under the scheduler mutex.
type randomizer struct {
	random  *rand.Rand
	indices []uint64
	// indexPD is a reverse map from PD ID to its current position in indices,
	// kept in sync with every swap performed by Next, Replace, Remove, and Add.
	indexPD map[uint64]int
	i       int
	length  int
	cycle   int
}

func newRandomizer(seed uint64, indices []uint64) (*randomizer, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("invalid argument: indices slice cannot be empty")
	}

	indexPD := make(map[uint64]int, len(indices))
	for pos, id := range indices {
		indexPD[id] = pos
	}

	return &randomizer{
		random:  rand.New(rand.NewPCG(seed, 0)), // #nosec G404
		indices: indices,
		indexPD: indexPD,
		i:       len(indices) - 1,
		length:  len(indices),
		cycle:   0,
	}, nil
}

// Next returns the next randomly selected index using an in-place Fisher-Yates
// shuffle. This is O(1) per call — only one swap is performed rather than
// shuffling the entire slice upfront.
//
// When all indices have been returned, the cycle counter is incremented and a
// new permutation begins. The cycle increment happens before the first element
// of the new cycle is returned, so Cycle() reflects the cycle of the element
// about to be returned, not the one just returned.
//
// Next must not be called when Len() == 0 (every ID has been removed).
func (r *randomizer) Next() uint64 {
	if r.i < 0 {
		r.cycle++
		r.i = r.length - 1
	}
	j := r.random.IntN(r.i + 1)
	// Update reverse map before swapping — values at positions i and j
	// are still the originals at this point.
	r.indexPD[r.indices[j]] = r.i
	r.indexPD[r.indices[r.i]] = j
	// Swap
	r.indices[j], r.indices[r.i] = r.indices[r.i], r.indices[j]
	out := r.indices[r.i]
	r.i--
	return out
}

// Replace substitutes oldID with newID in the active set indices.
//
// Preconditions: oldID must be present in the active set; newID must not
// already be present. Violating these preconditions will corrupt the reverse
// map. The scheduler guarantees them by removing the old PD from pdMap before
// adding the replacement.
//
// Replace may be called mid-cycle. The replacement takes the evicted PD's slot
// and will be drawn when that slot comes up in the current or next cycle,
// which means "each active ID exactly once per cycle" holds approximately but
// not strictly when replacements occur mid-cycle.
func (r *randomizer) Replace(oldID, newID uint64) {
	pos, ok := r.indexPD[oldID]
	if !ok {
		return
	}
	delete(r.indexPD, oldID)
	r.indices[pos] = newID
	r.indexPD[newID] = pos
}

// Remove deletes id in O(1), shrinking the cycle by one; a no-op if id is
// absent. Within a cycle, indices is [undrawn: 0..i][drawn: i+1..end], so an
// undrawn ID's hole is first moved to the boundary (lowering i), and the hole
// is then refilled from the end of the slice, which is always a drawn slot.
// After the last ID is removed, Len() == 0 and Next must not be called.
func (r *randomizer) Remove(id uint64) {
	pos, ok := r.indexPD[id]
	if !ok {
		return
	}
	last := r.length - 1

	if pos <= r.i {
		boundary := r.indices[r.i]
		r.indices[pos] = boundary
		r.indexPD[boundary] = pos
		pos = r.i
		r.i--
	}

	if pos != last {
		end := r.indices[last]
		r.indices[pos] = end
		r.indexPD[end] = pos
	}
	r.indices = r.indices[:last]
	r.length--
	delete(r.indexPD, id)
}

// Add inserts id as undrawn, growing the cycle by one, in O(1). Precondition:
// id must not already be present. The new slot is appended, then swapped
// into position i+1 — the first drawn slot — extending the undrawn region by
// one. If i == -1 (cycle about to wrap), the next Next() returns id
// immediately instead of wrapping; the wrap happens on the call after.
func (r *randomizer) Add(id uint64) {
	pos := r.length
	r.indices = append(r.indices, id)
	r.indexPD[id] = pos
	r.length++

	boundary := r.i + 1
	if boundary != pos {
		moved := r.indices[boundary]
		r.indices[pos] = moved
		r.indexPD[moved] = pos
		r.indices[boundary] = id
		r.indexPD[id] = boundary
	}
	r.i = boundary
}

// Len returns the number of IDs currently in the randomizer.
func (r *randomizer) Len() int {
	return r.length
}

// Cycle returns the current cycle count. The count is incremented at the start
// of each new permutation, before the first element is returned.
func (r *randomizer) Cycle() int {
	return r.cycle
}

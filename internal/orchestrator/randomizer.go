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
// randomizer is not safe for concurrent use. All calls to Next, Replace, and
// Cycle must be made under the scheduler mutex.
type randomizer struct {
	random  *rand.Rand
	indices []uint64
	// indexPD is a reverse map from PD ID to its current position in indices,
	// kept in sync with every swap performed by Next and Replace.
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

// Cycle returns the current cycle count. The count is incremented at the start
// of each new permutation, before the first element is returned.
func (r *randomizer) Cycle() int {
	return r.cycle
}

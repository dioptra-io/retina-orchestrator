// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"fmt"
	"math/rand/v2"
)

// randomizer provides randomized iteration over a set of indices using the
// Fisher-Yates shuffle algorithm. Each call to Next returns a unique index
// within the current cycle; once all indices have been returned, a new cycle
// begins and the sequence is reshuffled.
type randomizer struct {
	random  *rand.Rand
	indices []uint64
	i       int
	length  int
	cycle   int
}

func newRandomizer(seed uint64, indices []uint64) (*randomizer, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("invalid argument: indices slice cannot be empty")
	}
	return &randomizer{
		random:  rand.New(rand.NewPCG(seed, 0)), // #nosec G404
		indices: indices,
		i:       len(indices) - 1,
		length:  len(indices),
		cycle:   0,
	}, nil
}

// Next returns the next randomly selected index. At the end of each cycle,
// the internal state is reset and a new random permutation begins.
// This is O(1) per call — only one swap is performed rather than shuffling
// the entire slice upfront.
func (r *randomizer) Next() uint64 {
	if r.i < 0 {
		r.cycle += 1
		r.i = r.length - 1
	}

	j := r.random.IntN(r.i + 1)
	k := r.indices[j]
	r.indices[r.i], r.indices[j] = r.indices[j], r.indices[r.i]
	r.i -= 1

	return k
}

// Cycle returns the current cycle count, i.e. how many full permutations
// of the indices have been completed.
func (r *randomizer) Cycle() int {
	return r.cycle
}

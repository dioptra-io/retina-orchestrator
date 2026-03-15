package probing

import (
	"fmt"
	"math/rand/v2"
)

// randomizer is the implementation of an index randomizer. It uses the
// Fisher-Yates shuffle.
type randomizer struct {
	random  *rand.Rand
	indices []uint64
	i       int
	length  int
	cycle   int
}

// newRandomizer generates a new randomizer. It takes the indices as a
// parameter.
func newRandomizer(seed uint64, indices []uint64) (*randomizer, error) {
	if len(indices) == 0 {
		return nil, fmt.Errorf("invalid argument: indices array cannot be empty")
	}

	return &randomizer{
		random:  rand.New(rand.NewPCG(seed, 0)), // #nosec G404
		indices: indices,
		i:       len(indices) - 1,
		length:  len(indices),
		cycle:   0,
	}, nil
}

// Next generates a next random index. It doesn't need to shuffle the whole
// array, this this call is O(1).
func (r *randomizer) Next() uint64 {
	// Check if this is the end of the cycle.
	if r.i < 0 {
		r.cycle += 1
		r.i = r.length - 1
	}

	// Generate a random index and swap.
	j := r.random.IntN(r.i + 1)
	k := r.indices[j]
	r.indices[r.i], r.indices[j] = r.indices[j], r.indices[r.i]
	r.i -= 1

	return k
}

// Cycle returns the which cycle we are in at the moment.
func (r *randomizer) Cycle() int {
	return r.cycle
}

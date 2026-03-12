package probing

import (
	"fmt"
	"math/rand/v2"
)

// randomizer is the implementation of an index randomizer. It uses the
// Fisher-Yates shuffle.
type randomizer struct {
	random   *rand.Rand
	indicies []uint64
	i        int
	length   int
	cycle    int
}

// newRandomizer generates a new randomizer. It takes the indicies as a
// parameter.
func newRandomizer(seed uint64, indicies []uint64) (*randomizer, error) {
	if len(indicies) == 0 {
		return nil, fmt.Errorf("invalid argument: indicies array cannot be empty")
	}

	return &randomizer{
		random:   rand.New(rand.NewPCG(seed, 0)), // #nosec G404
		indicies: indicies,
		i:        len(indicies) - 1,
		length:   len(indicies),
		cycle:    0,
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
	k := r.indicies[j]
	r.indicies[r.i], r.indicies[j] = r.indicies[j], r.indicies[r.i]
	r.i -= 1

	return k
}

// Cycle returns the which cycle we are in at the moment.
func (r *randomizer) Cycle() int {
	return r.cycle
}

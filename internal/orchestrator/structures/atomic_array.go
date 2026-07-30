package structures

import (
	"math"
	"sync/atomic"
)

type AtomicFloat64Array struct {
	// Fixed-size backing storage for cells, preallocated once.
	// Only Add (goroutine A) hands out pointers into this; it is
	// never appended to, so these pointers stay valid forever.
	cellPool []atomic.Uint64
	poolNext int // only touched by goroutine A

	// Holds an immutable snapshot of the slice.
	// The pointed-to atomic.Uint64 objects remain stable.
	items atomic.Pointer[[]*atomic.Uint64]
}

// NewAtomicFloat64Array preallocates storage for expectedCapacity
// elements. Add calls up to that count allocate nothing; calls
// beyond it fall back to individual heap allocation.
func NewAtomicFloat64Array(expectedCapacity int) *AtomicFloat64Array {
	l := &AtomicFloat64Array{
		cellPool: make([]atomic.Uint64, expectedCapacity),
	}

	initial := make([]*atomic.Uint64, 0, expectedCapacity)
	l.items.Store(&initial)

	return l
}

// Add appends a new value and returns its index.
//
// Must only be called from a single goroutine (no internal
// synchronization against concurrent Add calls).
func (l *AtomicFloat64Array) Add(x float64) int {
	var cell *atomic.Uint64
	if l.poolNext < len(l.cellPool) {
		cell = &l.cellPool[l.poolNext]
		l.poolNext++
	} else {
		cell = new(atomic.Uint64) // beyond expected capacity
	}
	cell.Store(math.Float64bits(x))

	old := l.items.Load()
	index := len(*old)

	next := append(*old, cell) // in-place if capacity allows
	l.items.Store(&next)

	return index
}

// Set atomically replaces element i. Wait-free: a bounded pointer
// load, bounds check, and atomic store — no locks, no retries.
func (l *AtomicFloat64Array) Set(i int, x float64) bool {
	items := l.items.Load()

	if i < 0 || i >= len(*items) {
		return false
	}

	(*items)[i].Store(math.Float64bits(x))
	return true
}

// Dump returns a weakly consistent snapshot.
func (l *AtomicFloat64Array) Dump() []float64 {
	items := l.items.Load()

	result := make([]float64, len(*items))
	for i, cell := range *items {
		result[i] = math.Float64frombits(cell.Load())
	}

	return result
}

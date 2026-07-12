package structures

import (
	"math"
	"sync/atomic"
)

type AtomicFloat64 struct {
	bits atomic.Uint64
}

func (a *AtomicFloat64) Store(v float64) {
	a.bits.Store(math.Float64bits(v))
}

func (a *AtomicFloat64) Load() float64 {
	return math.Float64frombits(a.bits.Load())
}

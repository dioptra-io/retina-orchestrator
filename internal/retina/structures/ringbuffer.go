package structures

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// ringBufferConsumer is a consumer of a RingBuffer. Each consumer maintains its
// own tail pointer, allowing multiple consumers to independently read from the
// same buffer at their own pace.
//
// All methods on ringBufferConsumer must be called from the same goroutine.
type ringBufferConsumer[T any] struct {
	rb           *RingBuffer[T]
	tail         uint
	seq          uint64
	totalSkipped atomic.Uint64
}

// Pop returns the next element from the buffer, its sequence number in the
// global push stream (zero-based), and advances the consumer's tail.
//
// Returns an error if the context is cancelled or if the consumer is already
// closed.
func (rbc *ringBufferConsumer[T]) Pop(ctx context.Context) (*T, uint64, error) {
	// Consumer already closed.
	if rbc.rb == nil {
		return nil, 0, fmt.Errorf("consumer already closed")
	}

	// Check ctx exit early without acquiring the lock.
	if ctx.Err() != nil {
		return nil, 0, ctx.Err()
	}

	// Attach a broadcast to wake waiting goroutines if this consumer's context
	// is cancelled.
	cond := rbc.rb.cond
	stop := context.AfterFunc(ctx, func() {
		cond.Broadcast()
	})
	defer stop()

	rbc.rb.mutex.Lock()
	defer rbc.rb.mutex.Unlock()

	// Loop until tail != head (i.e. there is an element to read).
	for rbc.tail == rbc.rb.head {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		// Atomically release the lock and sleep. Reacquires the lock when woken.
		rbc.rb.cond.Wait()
	}
	e := rbc.rb.buffer[rbc.tail]
	rbc.tail = (rbc.tail + 1) % rbc.rb.cap
	rbc.seq += 1

	return e, rbc.seq - 1 + rbc.Skipped(), nil
}

// Close removes the consumer from the ring buffer, freeing its slot in the
// consumer set. After Close, the consumer's tail is no longer tracked by Push,
// so slow consumer skipping will not apply to it.
//
// Calling Close multiple times is a no-op.
func (rbc *ringBufferConsumer[T]) Close() {
	if rbc.rb == nil {
		return
	}

	rbc.rb.mutex.Lock()
	defer rbc.rb.mutex.Unlock()

	if _, ok := rbc.rb.consumers[rbc]; ok {
		delete(rbc.rb.consumers, rbc)
		rbc.rb = nil
	}
}

// Skipped returns the total number of elements that were skipped for this
// consumer due to it being too slow. When the producer laps a consumer, the
// consumer's tail is advanced automatically and this counter is incremented.
func (rbc *ringBufferConsumer[T]) Skipped() uint64 {
	return rbc.totalSkipped.Load()
}

// RingBuffer is a fixed-capacity circular buffer with support for multiple
// independent consumers. A single producer pushes elements via Push; each
// consumer reads independently via its own RingBufferConsumer.
//
// When a consumer is too slow and the producer has lapped it, the consumer's
// tail is advanced automatically by Push to avoid blocking the producer. This
// is reported as a skipped consumer in the return value of Push.
//
// Push is safe to call concurrently with Pop and Close, but Push itself is
// intended to be called from a single goroutine.
type RingBuffer[T any] struct {
	mutex     *sync.Mutex
	cond      *sync.Cond
	head      uint
	buffer    []*T
	cap       uint
	consumers map[*ringBufferConsumer[T]]struct{}
}

// NewRingBuffer creates a new RingBuffer with the given capacity.
// Returns an error if capacity is less than or equal to zero.
func NewRingBuffer[T any](capacity int) (*RingBuffer[T], error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("invalid argument: capacity must be greater than zero")
	}
	mu := &sync.Mutex{}
	return &RingBuffer[T]{
		mutex:     mu,
		head:      0,
		buffer:    make([]*T, capacity),
		cap:       uint(capacity),
		consumers: make(map[*ringBufferConsumer[T]]struct{}),
		cond:      sync.NewCond(mu),
	}, nil
}

// NewConsumer creates a new consumer starting at the current head position.
// The consumer will only receive elements pushed after its creation.
func (rb *RingBuffer[T]) NewConsumer() *ringBufferConsumer[T] {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	cons := &ringBufferConsumer[T]{
		tail: rb.head,
		rb:   rb,
		seq:  0,
	}
	rb.consumers[cons] = struct{}{}
	return cons
}

// Push adds a new element to the ring buffer and wakes all waiting consumers.
// If a consumer is too slow and its tail collides with the new head, its tail
// is advanced by one to keep it consistent with the buffer state.
//
// Returns the number of slow consumers whose tails were skipped.
func (rb *RingBuffer[T]) Push(e *T) int {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	rb.buffer[rb.head] = e
	rb.head = (rb.head + 1) % rb.cap

	// Advance the tails of slow consumers that have been lapped.
	skipped := 0
	for cons := range rb.consumers {
		if rb.head == cons.tail {
			skipped++
			cons.tail = (cons.tail + 1) % rb.cap
			cons.totalSkipped.Add(1)
		}
	}

	// Broadcast after all state is consistent.
	rb.cond.Broadcast()

	return skipped
}

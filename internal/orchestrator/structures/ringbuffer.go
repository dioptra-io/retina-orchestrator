// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package structures

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// All methods on ringConsumer must be called from the same goroutine.
type ringConsumer[T any] struct {
	rb           *RingBuffer[T]
	tail         uint64
	seq          uint64
	totalSkipped atomic.Uint64
}

// Pop returns the next element and a monotonically increasing sequence number
// that reflects any skipped elements, allowing clients to detect gaps in the
// FIE stream. Blocks until an element is available or the context is cancelled.
func (rbc *ringConsumer[T]) Pop(ctx context.Context) (*T, uint64, error) {
	// Safe because Pop and Close must be called from the same goroutine.
	if rbc.rb == nil {
		return nil, 0, fmt.Errorf("consumer already closed")
	}

	if ctx.Err() != nil {
		return nil, 0, ctx.Err()
	}

	// Wake waiting goroutines if this consumer's context is cancelled.
	cond := rbc.rb.cond
	stop := context.AfterFunc(ctx, func() {
		cond.Broadcast()
	})
	defer stop()

	rbc.rb.mutex.Lock()
	defer rbc.rb.mutex.Unlock()

	for rbc.tail == rbc.rb.head {
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		// Atomically release the lock and sleep. Reacquires the lock when woken.
		rbc.rb.cond.Wait()
	}
	e := rbc.rb.buffer[rbc.tail]
	rbc.tail = (rbc.tail + 1) % rbc.rb.capacity
	rbc.seq++

	return e, rbc.seq - 1 + rbc.Skipped(), nil
}

// Close releases the consumer. After Close, the consumer's tail is no longer
// tracked by Push, so slow consumer skipping will not apply to it.
// Calling Close multiple times is a no-op.
func (rbc *ringConsumer[T]) Close() {
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

// Skipped returns the number of elements that were overwritten before
// this consumer could read them, indicating the consumer is falling
// behind the producer.
func (rbc *ringConsumer[T]) Skipped() uint64 {
	return rbc.totalSkipped.Load()
}

// RingBuffer is a fixed-capacity circular buffer with multiple independent
// consumers. Slow consumers are lapped automatically rather than blocking
// the producer.
//
// Push is safe to call concurrently with Pop and Close, but is intended
// to be called from a single goroutine.
type RingBuffer[T any] struct {
	mutex     *sync.Mutex
	cond      *sync.Cond
	head      uint64
	buffer    []*T
	capacity  uint64
	consumers map[*ringConsumer[T]]struct{}
}

// NewRingBuffer creates a new RingBuffer with the given capacity.
func NewRingBuffer[T any](capacity int) (*RingBuffer[T], error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("invalid argument: capacity must be greater than zero")
	}
	mu := &sync.Mutex{}
	return &RingBuffer[T]{
		mutex:     mu,
		buffer:    make([]*T, capacity),
		capacity:  uint64(capacity),
		consumers: make(map[*ringConsumer[T]]struct{}),
		cond:      sync.NewCond(mu),
	}, nil
}

// NewConsumer creates a new consumer starting at the current head position.
// The consumer will only receive elements pushed after its creation.
func (rb *RingBuffer[T]) NewConsumer() *ringConsumer[T] {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	cons := &ringConsumer[T]{
		tail: rb.head,
		rb:   rb,
	}
	rb.consumers[cons] = struct{}{}
	return cons
}

// Push adds a new element to the ring buffer and wakes all waiting consumers.
// Returns the number of slow consumers whose tails were skipped.
func (rb *RingBuffer[T]) Push(e *T) int {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	rb.buffer[rb.head] = e
	rb.head = (rb.head + 1) % rb.capacity

	// Advance the tails of slow consumers that have been lapped.
	skipped := 0
	for cons := range rb.consumers {
		if rb.head == cons.tail {
			skipped++
			cons.tail = (cons.tail + 1) % rb.capacity
			cons.totalSkipped.Add(1)
		}
	}

	// Broadcast after all state is consistent.
	rb.cond.Broadcast()

	return skipped
}

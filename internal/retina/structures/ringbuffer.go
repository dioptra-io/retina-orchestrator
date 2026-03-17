package structures

import (
	"context"
	"fmt"
	"sync"
)

type RingBufferConsumer[T any] struct {
	rb   *RingBuffer[T]
	tail uint
}

// Pop takes the element from the buffer and increases tail. If there is no
// other element then consumer goroutine sleeps until context cancellation or
// a push operation.
//
// All the consumer interactions are assumed to be done using the same
// goroutine.
func (rbc *RingBufferConsumer[T]) Pop(ctx context.Context) (*T, error) {
	// Consumer already closed.
	if rbc.rb == nil {
		return nil, fmt.Errorf("consumer already closed")
	}

	// Check ctx exit early without acquiring the lock.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Attach broadcast to wake the routines up if one consumer's context is
	// cancelled.
	stop := context.AfterFunc(ctx, func() {
		rbc.rb.cond.Broadcast()
	})
	defer stop() // De-attach the after function.

	rbc.rb.mutex.Lock()
	defer rbc.rb.mutex.Unlock()

	// Loop until tail != head.
	for rbc.tail == rbc.rb.head {
		// Check for the ctx cancellation here.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Release the lock and wait, when woken up regain the lock and check.
		rbc.rb.cond.Wait()
	}
	e := rbc.rb.buffer[rbc.tail]
	rbc.tail = (rbc.tail + 1) % rbc.rb.cap

	return e, nil
}

// Close removes the consumer from the ring buffer. Calling this multiple times
// would be a nop.
func (rbc *RingBufferConsumer[T]) Close() {
	// Consumer already closed.
	if rbc.rb == nil {
		return
	}

	rbc.rb.mutex.Lock()
	defer rbc.rb.mutex.Unlock()

	// Remove itself from the consumers set.
	if _, ok := rbc.rb.consumers[rbc]; ok {
		delete(rbc.rb.consumers, rbc)
		rbc.rb = nil
	}
}

type RingBuffer[T any] struct {
	mutex     *sync.Mutex
	cond      *sync.Cond
	head      uint
	buffer    []*T
	cap       uint
	consumers map[*RingBufferConsumer[T]]struct{}
}

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
		consumers: make(map[*RingBufferConsumer[T]]struct{}),
		cond:      sync.NewCond(mu),
	}, nil
}

func (rb *RingBuffer[T]) NewConsumer() *RingBufferConsumer[T] {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	// Start from the last known position of the head.
	cons := &RingBufferConsumer[T]{
		tail: rb.head,
		rb:   rb,
	}

	// Add the created consumer to the consumers.
	rb.consumers[cons] = struct{}{}
	return cons
}

// Push adds a new element to the ring buffer. This is expected to be called
// from a single goroutine. Returns the number of slow consumers skipped.
func (rb *RingBuffer[T]) Push(e *T) int {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	// Put the element to the buffer and increase the head.
	rb.buffer[rb.head] = e
	rb.head = (rb.head + 1) % rb.cap

	// Increase the tails of the slow consumers.
	skipped := 0
	for cons := range rb.consumers {
		if rb.head == cons.tail {
			skipped += 1
			cons.tail = (cons.tail + 1) % rb.cap
		}
	}

	// Broadcast after updating the slow consumers.
	rb.cond.Broadcast()

	return skipped
}

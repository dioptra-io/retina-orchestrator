package structures

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrClosed denotes the underlying system is already closed.
	ErrClosed = errors.New("closed")
)

// RingBuffer is an implementation for the orchestrator specifically to be used
// by the streaming clients. The main goal is to make sure multiple readers can
// access the elements pushed to the ring buffer. If the ring buffer is full
// then adding a new element would override the oldest element, so all the
// slow consumers would miss that element. Missing an element is communicated to
// the consumers via the sequence numbers returned by the Next() method.
type RingBuffer[T any] struct {
	// mu protects against races.
	mu sync.Mutex
	// buffer is the actual buffer.
	buffer []T
	// capacity is the capacity of the buffer.
	capacity uint64
	// totalPushed tracks total number of elements pushed (used for sequence
	// numbers).
	totalPushed uint64
	// closed indicates if the ring buffer has been closed.
	closed bool
	// notify is used to signal consumers when new data is available.
	notify chan struct{}
}

// NewRingBuffer creates a new ring buffer with the specified type and capacity.
func NewRingBuffer[T any](capacity uint64) *RingBuffer[T] {
	return &RingBuffer[T]{
		buffer:   make([]T, capacity),
		capacity: capacity,
		notify:   make(chan struct{}),
	}
}

// Push adds a new element to the buffer.
// If the context is cancelled it returns the context's error.
// If the ring buffer is closed it returns ErrClosed.
func (rb *RingBuffer[T]) Push(ctx context.Context, element *T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	rb.mu.Lock()
	if rb.closed {
		rb.mu.Unlock()
		return ErrClosed
	}

	rb.buffer[rb.totalPushed%rb.capacity] = *element
	rb.totalPushed++

	// Signal waiters by closing and replacing the channel.
	old := rb.notify
	rb.notify = make(chan struct{})
	rb.mu.Unlock()

	close(old)
	return nil
}

// Close closes the ring buffer and wakes all waiting consumers.
func (rb *RingBuffer[T]) Close() {
	rb.mu.Lock()
	if rb.closed {
		rb.mu.Unlock()
		return
	}
	rb.closed = true
	old := rb.notify
	rb.notify = make(chan struct{})
	rb.mu.Unlock()

	close(old)
}

// NewConsumer creates a new consumer starting from the current position.
func (rb *RingBuffer[T]) NewConsumer() *RingBufferConsumer[T] {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return &RingBufferConsumer[T]{
		nextSeq:    rb.totalPushed,
		ringBuffer: rb,
	}
}

// RingBufferConsumer is the consumer of the RingBuffer.
type RingBufferConsumer[T any] struct {
	// nextSeq is the sequence number of the next item to consume.
	nextSeq uint64
	// ringBuffer is the pointer to the ring buffer.
	ringBuffer *RingBuffer[T]
	// closed tells if the consumer is closed.
	closed bool
}

// Next returns the next element from the ring buffer. Blocks if no element is
// available. Returns the element, its sequence number, and any error.
// If the context is cancelled it returns the context's error.
// If the consumer or ring buffer is closed it returns ErrClosed.
// If elements were skipped (slow consumer), the sequence number will show the gap.
func (c *RingBufferConsumer[T]) Next(ctx context.Context) (*T, uint64, error) {
	for {
		if c.closed {
			return nil, 0, ErrClosed
		}

		rb := c.ringBuffer
		rb.mu.Lock()

		if rb.closed {
			rb.mu.Unlock()
			return nil, 0, ErrClosed
		}

		// Check if we're too far behind (data was overwritten).
		if rb.totalPushed > rb.capacity && c.nextSeq < rb.totalPushed-rb.capacity {
			c.nextSeq = rb.totalPushed - rb.capacity
		}

		// Check if data is available.
		if c.nextSeq < rb.totalPushed {
			elem := rb.buffer[c.nextSeq%rb.capacity]
			seq := c.nextSeq
			c.nextSeq++
			rb.mu.Unlock()
			return &elem, seq, nil
		}

		// Get notify channel while holding lock.
		notify := rb.notify
		rb.mu.Unlock()

		// Wait for data or cancellation.
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-notify:
			// Loop and check again.
		}
	}
}

// Close closes the consumer. After this point the consumer cannot be used.
// It is idempotent meaning Close can be called multiple times.
func (c *RingBufferConsumer[T]) Close() {
	c.closed = true
}

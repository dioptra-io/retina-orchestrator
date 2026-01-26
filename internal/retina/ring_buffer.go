package retina

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
	mu       sync.Mutex
	cond     *sync.Cond
	buffer   []T
	capacity uint64
	// writePos is where the next element will be written
	writePos uint64
	// totalPushed tracks total number of elements pushed (used for sequence numbers)
	totalPushed  uint64
	consumers    map[uint64]*RingBufferConsumer[T]
	nextConsumer uint64
	closed       bool
}

// NewRingBuffer creates a new ring buffer with the specified type and capacity.
func NewRingBuffer[T any](capacity uint64) *RingBuffer[T] {
	rb := &RingBuffer[T]{
		buffer:    make([]T, capacity),
		capacity:  capacity,
		consumers: make(map[uint64]*RingBufferConsumer[T]),
	}
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

// NewConsumer creates a new consumer starting from the current position.
func (rb *RingBuffer[T]) NewConsumer() *RingBufferConsumer[T] {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	consumer := &RingBufferConsumer[T]{
		id:         rb.nextConsumer,
		nextSeq:    rb.totalPushed,
		ringBuffer: rb,
	}

	rb.consumers[rb.nextConsumer] = consumer
	rb.nextConsumer++

	return consumer
}

// Push adds a new element to the buffer.
func (rb *RingBuffer[T]) Push(ctx context.Context, element *T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.closed {
		return ErrClosed
	}

	rb.buffer[rb.writePos%rb.capacity] = *element
	rb.writePos++
	rb.totalPushed++

	rb.cond.Broadcast()
	return nil
}

// Close closes the ring buffer and wakes all waiting consumers.
func (rb *RingBuffer[T]) Close() {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.closed = true
	rb.cond.Broadcast()
}

// RingBufferConsumer is the client of the RingBuffer.
type RingBufferConsumer[T any] struct {
	id         uint64
	nextSeq    uint64
	ringBuffer *RingBuffer[T]
	closed     bool
}

// Next returns the next element. Blocks if no element is available.
// Returns the element, its sequence number, and any error.
// If elements were skipped (slow consumer), the sequence number will show the gap.
func (c *RingBufferConsumer[T]) Next(ctx context.Context) (*T, uint64, error) {
	rb := c.ringBuffer

	rb.mu.Lock()
	defer rb.mu.Unlock()

	for {
		// Check if consumer is closed
		if c.closed {
			return nil, 0, ErrClosed
		}

		// Check if ring buffer is closed
		if rb.closed {
			return nil, 0, ErrClosed
		}

		// Check context
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}

		// Check if consumer is too far behind (data was overwritten)
		oldestAvailable := uint64(0)
		if rb.totalPushed > rb.capacity {
			oldestAvailable = rb.totalPushed - rb.capacity
		}
		if c.nextSeq < oldestAvailable {
			// Skip to oldest available
			c.nextSeq = oldestAvailable
		}

		// Check if data is available
		if c.nextSeq < rb.totalPushed {
			idx := c.nextSeq % rb.capacity
			element := rb.buffer[idx]
			seq := c.nextSeq
			c.nextSeq++
			return &element, seq, nil
		}

		// No data available, wait
		rb.cond.Wait()
	}
}

// Close closes the consumer and removes it from the ring buffer.
func (c *RingBufferConsumer[T]) Close() {
	rb := c.ringBuffer

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if c.closed {
		return
	}

	c.closed = true
	delete(rb.consumers, c.id)
	rb.cond.Broadcast()
}

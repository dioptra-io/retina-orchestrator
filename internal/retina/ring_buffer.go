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

// RingBuffer is an implementation of for the orchestrator. It is intended to
// have many consumers identified with clientids.
// If the client is slow then the tails are pushed meaning slow consumers can
// miss elements.
//
// TODO: The implementation is not tested. This may need some changes.
type RingBuffer[T any] struct {
	// mutex is used to protect against races.
	mutex sync.Mutex
	// cond is used for signaling consumers when new data is available.
	cond *sync.Cond
	// buffer is the actual buffer.
	buffer []T
	// capacity is the capacity of the buffer.
	capacity uint64
	// head is the head index.
	head uint64
	// totalPushed tracks total number of elements pushed (used for sequence numbers).
	totalPushed uint64
	// consumers is a map of clients used to keep track of the tails.
	consumers map[uint64]*RingBufferConsumer[T]
	// nextConsumerID is the next id to assign to a consumer.
	nextConsumerID uint64
}

// NewRingBuffer creates a new ring buffer with the specified type and capacity.
func NewRingBuffer[T any](capacity uint64) *RingBuffer[T] {
	rb := &RingBuffer[T]{
		buffer:    make([]T, capacity),
		capacity:  capacity,
		head:      0,
		consumers: make(map[uint64]*RingBufferConsumer[T]),
	}
	rb.cond = sync.NewCond(&rb.mutex)
	return rb
}

// NewConsumer creates a new client and returns it. The RingBufferConsumer can
// be used to interact with the ring buffer.
func (rb *RingBuffer[T]) NewConsumer() *RingBufferConsumer[T] {
	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	consumer := &RingBufferConsumer[T]{
		id:             rb.nextConsumerID,
		sequenceNumber: rb.totalPushed,
		tail:           rb.head,
		ringBuffer:     rb,
		closed:         false,
	}

	rb.consumers[rb.nextConsumerID] = consumer
	rb.nextConsumerID++

	return consumer
}

// Push adds a new element and pushes the head index. If there are any slow
// consumers it increments their tail and their sequence number.
// In the future implementations this can block so a context needs to be
// provided. If the context is cancelled it returns context's error.
func (rb *RingBuffer[T]) Push(ctx context.Context, element *T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	// Store the element at the current head position
	rb.buffer[rb.head] = *element

	// Move head forward
	oldHead := rb.head
	rb.head = (rb.head + 1) % rb.capacity
	rb.totalPushed++

	// Check all consumers and push their tails if they're about to be overwritten
	for _, consumer := range rb.consumers {
		if consumer.closed {
			continue
		}
		// If consumer's tail is at the position we just wrote to (meaning it's now
		// pointing to old data that will be overwritten next cycle), push it forward
		// But we need to check if the consumer is behind by a full cycle
		// A consumer is "slow" if its tail would be overwritten by the new head
		if consumer.tail == rb.head && consumer.sequenceNumber < rb.totalPushed {
			// Consumer's tail is at the new head position, meaning it's a full
			// cycle behind. Push its tail forward.
			consumer.tail = (consumer.tail + 1) % rb.capacity
			consumer.sequenceNumber++
		}
	}

	// Signal all waiting consumers that new data is available
	rb.cond.Broadcast()

	_ = oldHead // suppress unused variable warning
	return nil
}

// RingBufferConsumer is the client of the RingBuffer. It implements the pop and
// the close method.
type RingBufferConsumer[T any] struct {
	// id is the id of the ringBufferConsumer.
	id uint64
	// sequence number of next item to consume. Every time the tail is increased
	// the sequenceNumber is also increased. Different from the tail the modulo
	// of it is not taken.
	sequenceNumber uint64
	// tail is the current position of the consumer on buffer.
	tail uint64
	// ringBuffer is the pointer to the actualt ring buffer.
	ringBuffer *RingBuffer[T]
	// closed tells if the client is no longer on the ring buffer.
	closed bool
}

// Pop pops the element from the ring buffer and retuns the element and the
// sequenceNumber. If there are no new elements then it blocks.
// If the context is cancelled it returns the context's error.
// If the consumer is closed it returns ErrClosed error.
func (rbc *RingBufferConsumer[T]) Pop(ctx context.Context) (*T, uint64, error) {
	rb := rbc.ringBuffer

	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	// Check if already closed
	if rbc.closed {
		return nil, 0, ErrClosed
	}

	// Wait until there's data available or context is cancelled or consumer is closed
	for rbc.sequenceNumber >= rb.totalPushed {
		// Check context before waiting
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		default:
		}

		// Check if closed
		if rbc.closed {
			return nil, 0, ErrClosed
		}

		// No data available, wait for signal
		// We need to wait with the ability to check context cancellation
		// Use a channel-based approach for context awareness
		done := make(chan struct{})
		go func() {
			rb.mutex.Lock()
			rb.cond.Wait()
			rb.mutex.Unlock()
			close(done)
		}()

		rb.mutex.Unlock()

		select {
		case <-ctx.Done():
			// Context cancelled, need to wake up the waiting goroutine
			rb.cond.Broadcast()
			<-done // Wait for goroutine to finish
			rb.mutex.Lock()
			return nil, 0, ctx.Err()
		case <-done:
			rb.mutex.Lock()
			// Continue the loop to check if data is available
		}
	}

	// Check again if closed (might have been closed while waiting)
	if rbc.closed {
		return nil, 0, ErrClosed
	}

	// Get the element at tail position
	element := rb.buffer[rbc.tail]
	seqNum := rbc.sequenceNumber

	// Move tail forward
	rbc.tail = (rbc.tail + 1) % rb.capacity
	rbc.sequenceNumber++

	return &element, seqNum, nil
}

// Close closes the client and removes it from the rung buffer, after this point
// the consumer cannot be used and it is not affected.
// It is idenpotent meaning Close can be called multiple times.
func (rbc *RingBufferConsumer[T]) Close() {
	rb := rbc.ringBuffer

	rb.mutex.Lock()
	defer rb.mutex.Unlock()

	if rbc.closed {
		return
	}

	rbc.closed = true
	delete(rb.consumers, rbc.id)

	// Wake up any waiting Pop calls
	rb.cond.Broadcast()
}

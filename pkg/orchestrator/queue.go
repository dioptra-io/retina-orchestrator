package orchestrator

import (
	"context"
	"sync"
)

// queue provides a thread-safe, keyed message queue where a single producer can send messages to specific consumers
// identified by unique keys.
//
// Each key has exactly one consumer. Push sends a message to a specific key, and Get retrieves messages for that key.
// Both operations respect context cancellation.
//
// The zero value is not usable; create instances with NewQueue.
type queue[T any] struct {
	mu     sync.Mutex
	queues map[string]chan T

	ctx    context.Context
	cancel context.CancelFunc

	bufferSize int
}

// newQueue creates a new Queue. The provided context controls the lifetime of the queue; cancelling it closes all
// internal channels.
//
// The bufferSize parameter controls the buffer size for each key's channel.
func newQueue[T any](ctx context.Context, bufferSize int) *queue[T] {
	ctx, cancel := context.WithCancel(ctx)

	q := &queue[T]{
		queues:     make(map[string]chan T),
		ctx:        ctx,
		cancel:     cancel,
		bufferSize: bufferSize,
	}

	return q
}

// getOrCreateChan returns the channel for the given key, creating it if necessary.
func (q *queue[T]) getOrCreateChan(key string) chan T {
	q.mu.Lock()
	defer q.mu.Unlock()

	ch, ok := q.queues[key]
	if !ok {
		ch = make(chan T, q.bufferSize)
		q.queues[key] = ch
	}
	return ch
}

// Push sends a message to the consumer identified by key.
//
// Blocks until the message is received or a context is cancelled. Returns ctx.Err() if the provided context is
// cancelled, or ErrQueueClosed if the queue has been closed.
func (q *queue[T]) Push(ctx context.Context, key string, msg T) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ch := q.getOrCreateChan(key)

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-q.ctx.Done():
		return ErrQueueClosed

	case ch <- msg:
		return nil
	}
}

// Get retrieves the next message for the given key.
//
// Blocks until a message is available or a context is cancelled. Returns ctx.Err() if the provided context is
// cancelled, or ErrQueueClosed if the queue has been closed.
func (q *queue[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T

	if ctx.Err() != nil {
		return zero, ctx.Err()
	}

	ch := q.getOrCreateChan(key)

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-q.ctx.Done():
		return zero, ErrQueueClosed
	case msg := <-ch:
		return msg, nil
	}
}

// DeleteKey removes the channel for the given key, closing it and freeing resources.
//
// Any goroutines blocked on Get for this key will receive ErrQueueClosed.
// Any pending messages in the channel are discarded.
//
// Returns true if the key existed and was deleted, false otherwise.
func (q *queue[T]) DeleteKey(key string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	ch, exists := q.queues[key]
	if !exists {
		return false
	}

	close(ch)
	delete(q.queues, key)
	return true
}

// Close shuts down the queue and closes all internal channels.
//
// After Close returns, Push and Get will return ErrQueueClosed.
func (q *queue[T]) Close() {
	q.cancel()

	q.mu.Lock()
	defer q.mu.Unlock()

	for key, ch := range q.queues {
		close(ch)
		delete(q.queues, key)
	}
	q.queues = nil
}

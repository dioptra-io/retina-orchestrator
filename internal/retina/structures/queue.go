package structures

import (
	"context"
	"fmt"
	"sync"
)

// queueConsumer is a consumer of a Queue. It holds a dedicated channel for
// receiving elements pushed to it.
//
// All methods on queueConsumer must be called from the same goroutine.
type queueConsumer[T any] struct {
	id    string
	ch    chan *T
	queue *Queue[T]
}

// Pop returns the next element from the consumer's queue. If no element is
// available, Pop blocks until one is pushed or the context is cancelled.
//
// Returns an error if the context is cancelled or if the consumer is already
// closed.
func (qc *queueConsumer[T]) Pop(ctx context.Context) (*T, error) {
	if qc.queue == nil {
		return nil, fmt.Errorf("consumer already closed")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case item, open := <-qc.ch:
		if !open {
			return nil, fmt.Errorf("consumer already closed")
		}
		return item, nil
	}
}

// Close removes the consumer from the queue. Calling Close multiple times is a
// no-op.
func (qc *queueConsumer[T]) Close() {
	if qc.queue == nil {
		return
	}

	qc.queue.mu.Lock()
	defer qc.queue.mu.Unlock()

	if _, ok := qc.queue.consumers[qc.id]; ok {
		delete(qc.queue.consumers, qc.id)
		close(qc.ch)
		qc.queue = nil
	}
}

// Queue is a pub/sub queue that delivers elements to individual consumers.
// Each consumer has its own buffered channel and receives only elements pushed
// directly to it.
//
// Push is safe to call concurrently with Pop and Close.
type Queue[T any] struct {
	mu         sync.Mutex
	consumers  map[string]*queueConsumer[T]
	bufferSize int
}

// NewQueue creates a new Queue with the given per-consumer buffer size.
func NewQueue[T any](bufferSize int) (*Queue[T], error) {
	if bufferSize < 0 {
		return nil, fmt.Errorf("buffer size cannot be negative")
	}

	return &Queue[T]{
		consumers:  make(map[string]*queueConsumer[T]),
		bufferSize: bufferSize,
	}, nil
}

// NewConsumer creates a new QueueConsumer with a dedicated buffered channel.
// If the given id already exists it returns an error.
func (q *Queue[T]) NewConsumer(id string) (*queueConsumer[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.consumers[id]; ok {
		return nil, fmt.Errorf("consumer with id already exists")
	}

	consumer := &queueConsumer[T]{
		id:    id,
		ch:    make(chan *T, q.bufferSize),
		queue: q,
	}
	q.consumers[id] = consumer
	return consumer, nil
}

// Push sends an element to a specific consumer. Blocks if the consumer's buffer
// is full until space is available or the context is cancelled.
//
// Returns an error if the context is cancelled or if the consumer is closed.
func (q *Queue[T]) Push(ctx context.Context, id string, item *T) (err error) {
	q.mu.Lock()
	if _, ok := q.consumers[id]; !ok {
		q.mu.Unlock()
		return fmt.Errorf("consumer is not registered or already closed")
	}
	ch := q.consumers[id].ch
	q.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("consumer already closed")
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case ch <- item:
		return nil
	}
}

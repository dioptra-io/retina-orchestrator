// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

// Package structures provides generic data structures for the orchestrator,
// including a per-consumer queue and a ring buffer for FIE streaming.
package structures

import (
	"context"
	"fmt"
	"sync"
)

// All methods on consumer must be called from the same goroutine.
type consumer[T any] struct {
	id    string
	ch    chan *T
	done  chan struct{}
	queue *Queue[T]
}

// Pop returns the next element, blocking until one is available, the context
// is cancelled, or the consumer is closed.
func (qc *consumer[T]) Pop(ctx context.Context) (*T, error) {
	// Drain any buffered item first before blocking.
	select {
	case item := <-qc.ch:
		return item, nil
	default:
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-qc.done:
		return nil, fmt.Errorf("consumer closed")
	case item := <-qc.ch:
		return item, nil
	}
}

// Calling Close multiple times is a no-op.
func (qc *consumer[T]) Close() {
	if qc.queue == nil {
		return
	}

	qc.queue.mu.Lock()
	defer qc.queue.mu.Unlock()

	if _, ok := qc.queue.consumers[qc.id]; ok {
		delete(qc.queue.consumers, qc.id)
		close(qc.done)
		qc.queue = nil
	}
}

// Queue delivers elements to individual named consumers.
// Each consumer has its own buffered channel and receives only elements
// pushed directly to it by ID.
type Queue[T any] struct {
	mu         sync.Mutex
	consumers  map[string]*consumer[T]
	bufferSize int
}

// NewQueue creates a new Queue with the given per-consumer buffer size.
func NewQueue[T any](bufferSize int) (*Queue[T], error) {
	if bufferSize < 0 {
		return nil, fmt.Errorf("buffer size cannot be negative: got %d", bufferSize)
	}

	return &Queue[T]{
		consumers:  make(map[string]*consumer[T]),
		bufferSize: bufferSize,
	}, nil
}

// Returns an error if a consumer with the given id already exists.
func (q *Queue[T]) NewConsumer(id string) (*consumer[T], error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if _, ok := q.consumers[id]; ok {
		return nil, fmt.Errorf("consumer %q already exists", id)
	}

	c := &consumer[T]{
		id:    id,
		ch:    make(chan *T, q.bufferSize),
		done:  make(chan struct{}),
		queue: q,
	}
	q.consumers[id] = c
	return c, nil
}

// Push sends an element to a specific consumer. Blocks if the consumer's
// buffer is full until space is available or the context is cancelled.
// Push is safe to call concurrently with Pop and Close.
//
// Returns an error if the context is cancelled, if the consumer is not found,
// or if the consumer is closed.
func (q *Queue[T]) Push(ctx context.Context, id string, item *T) error {
	q.mu.Lock()
	if _, ok := q.consumers[id]; !ok {
		q.mu.Unlock()
		return fmt.Errorf("consumer %q not found", id)
	}
	// Capture both ch and done under the lock so Close() cannot race between
	// the two reads.
	ch, done := q.consumers[id].ch, q.consumers[id].done
	q.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return fmt.Errorf("consumer %q closed", id)
	case ch <- item:
		return nil
	}
}

// TryPush attempts to send an element to a specific consumer without blocking.
// Returns an error if the consumer is not registered or the buffer is full.
func (q *Queue[T]) TryPush(id string, item *T) error {
	q.mu.Lock()
	consumer, ok := q.consumers[id]
	q.mu.Unlock()
	if !ok {
		return fmt.Errorf("consumer not registered")
	}
	select {
	case consumer.ch <- item:
		return nil
	default:
		return fmt.Errorf("consumer buffer full")
	}
}

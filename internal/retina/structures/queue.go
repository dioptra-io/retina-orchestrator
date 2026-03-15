package structures

import (
	"context"
	"fmt"
	"sync"
)

type Queue[T any] struct {
	mu          sync.RWMutex
	subscribers map[string]chan *T
	bufferSize  int
}

func NewQueue[T any](bufferSize int) *Queue[T] {
	return &Queue[T]{
		subscribers: make(map[string]chan *T),
		bufferSize:  bufferSize,
	}
}

func (q *Queue[T]) Subscribe(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.subscribers[id] = make(chan *T, q.bufferSize)
}

func (q *Queue[T]) Unsubscribe(id string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if ch, ok := q.subscribers[id]; ok {
		close(ch)
		delete(q.subscribers, id)
	}
}

func (q *Queue[T]) Push(ctx context.Context, id string, item *T) (err error) {
	q.mu.Lock()
	ch, ok := q.subscribers[id]
	if !ok {
		q.mu.Unlock()
		return fmt.Errorf("subscriber %q not found", id)
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("subscriber %q was unsubscribed", id)
		}
	}()

	select {
	case ch <- item:
		q.mu.Unlock()
		return nil
	case <-ctx.Done():
		q.mu.Unlock()
		return ctx.Err()
	}
}

func (q *Queue[T]) Pop(ctx context.Context, id string) (*T, error) {
	q.mu.Lock()
	ch, ok := q.subscribers[id]
	if !ok {
		q.mu.Unlock()
		return nil, fmt.Errorf("subscriber %q not found", id)
	}

	select {
	case item, open := <-ch:
		if !open {
			q.mu.Unlock()
			return nil, fmt.Errorf("subscriber %q queue is closed", id)
		}
		q.mu.Unlock()
		return item, nil
	case <-ctx.Done():
		q.mu.Unlock()
		return nil, ctx.Err()
	}
}

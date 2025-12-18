package orchestrator

import (
	"context"
	"sync"
	"sync/atomic"
)

// broadcaster provides a thread-safe mechanism for broadcasting messages to multiple subscribers. It supports graceful
// shutdown via context cancellation and allows subscribers to unsubscribe independently.
//
// The zero value is not usable; create instances with NewBroadcaster.
type broadcaster[T any] struct {
	mu   sync.RWMutex
	subs map[chan T]struct{}

	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc

	dropMessageOnSlowClients bool
}

// newBroadcaster creates a new Broadcaster. The provided context controls the lifetime of the broadcaster; cancelling
// it is equivalent to calling Close.
//
// If drop is true, messages are dropped for subscribers whose buffers are full. If false, Broadcast blocks until all
// subscribers have received the message.
func newBroadcaster[T any](ctx context.Context, drop bool) *broadcaster[T] {
	ctx, cancel := context.WithCancel(ctx)

	return &broadcaster[T]{
		subs:                     make(map[chan T]struct{}),
		ctx:                      ctx,
		cancel:                   cancel,
		dropMessageOnSlowClients: drop,
	}
}

// Subscribe registers a new subscriber and returns a channel for receiving broadcasts. The buffer parameter controls
// the channel's buffer size.
//
// The subscription remains active until ctx is cancelled, the broadcaster is closed, or Close is called. The returned
// channel is closed when the subscription ends.
//
// Returns ErrBroadcasterClosed if the broadcaster has been closed, or ctx.Err() if the provided context is already
// cancelled.
func (b *broadcaster[T]) Subscribe(ctx context.Context, buffer int) (<-chan T, error) {
	// in case the context is already cancelled
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed.Load() {
		return nil, ErrBroadcasterClosed
	}

	ch := make(chan T, buffer)
	b.subs[ch] = struct{}{}

	go func() {
		select {
		case <-ctx.Done():
		case <-b.ctx.Done():
		}

		b.mu.Lock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
		b.mu.Unlock()
	}()
	return ch, nil
}

// Broadcast sends a message to all current subscribers.
//
// If the broadcaster was created with drop=true, the message is skipped for any subscriber whose buffer is full.
// Otherwise, this method blocks until all subscribers have received the message.
//
// Returns ErrBroadcasterClosed if the broadcaster has been closed, or ctx.Err() if the provided context is cancelled.
func (b *broadcaster[T]) Broadcast(ctx context.Context, msg T) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.closed.Load() {
		return ErrBroadcasterClosed
	}

	for ch := range b.subs {
		if b.dropMessageOnSlowClients {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case <-b.ctx.Done():
				return ErrBroadcasterClosed

			case ch <- msg:

			default:
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case <-b.ctx.Done():
				return ErrBroadcasterClosed

			case ch <- msg:
			}
		}
	}

	return nil
}

// Close shuts down the broadcaster and closes all subscriber channels. It is safe to call concurrently; subsequent
// calls return ErrBroadcasterClosed.
//
// After Close returns, Subscribe and Broadcast will return ErrBroadcasterClosed.
func (b *broadcaster[T]) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return ErrBroadcasterClosed
	}

	b.cancel()
	b.mu.Lock()

	defer b.mu.Unlock()
	for ch := range b.subs {
		delete(b.subs, ch)
		close(ch)
	}
	b.subs = nil

	return nil
}

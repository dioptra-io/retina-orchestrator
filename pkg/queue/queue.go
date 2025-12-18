package queue

import (
	"context"
	"errors"
	"sync"
)

var (
	// ErrSubscriberAlreadyExists is returned when attempting to subscribe with an ID that is already in use.
	ErrSubscriberAlreadyExists = errors.New("subscriber already exists")

	// ErrSubscriberDoesNotExist is returned when attempting to operate on a subscriber ID that is not registered.
	ErrSubscriberDoesNotExist = errors.New("subscriber does not exist")

	// ErrQueueClosed is returned when attempting to perform operations on a closed queue.
	ErrQueueClosed = errors.New("queue is closed")

	// ErrDropedOneOrMoreMessages is returned when sending or broadcasting a message one of the consumers unsubscribe.
	ErrDropedOneOrMoreMessages = errors.New("dropped one or more messages")

	// ErrEmptyID is returned when the given subscriber id is empty.
	ErrEmptyID = errors.New("empty id")
)

// Queue is a thread-safe pub/sub message queue that supports multiple consumers. It uses generics to allow type-safe
// message passing between producers and consumers. Producers and consumers are expected to operate on different
// goroutines.
type Queue[T any] struct {
	mu          sync.RWMutex
	subscribers map[string]chan *T
	bufferSize  int
	closed      bool
}

// NewQueue creates a new Queue with the specified buffer size for each subscriber's channel. The buffer size determines
// how many messages can be queued per subscriber before Send or Broadcast operations block. A minimum buffer size of 1
// is enforced.
func NewQueue[T any](bufferSize int) *Queue[T] {
	if bufferSize < 1 {
		bufferSize = 1
	}
	return &Queue[T]{
		subscribers: make(map[string]chan *T),
		bufferSize:  bufferSize,
		closed:      false,
	}
}

// SubscribeWithID registers a consumer with a specific pre-determined ID. This is useful when the caller needs to
// control ID assignment, such as when correlating subscriber IDs with external identifiers.
//
// Returns
//   - ErrEmptyID if the given id is empty.
//   - ErrQueueClosed if the queue is closed.
//   - ErrSubscriberAlreadyExists if the ID is already registered.
func (q *Queue[T]) SubscribeWithID(id string) error {
	if id == "" {
		return ErrEmptyID
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrQueueClosed
	}

	if _, exists := q.subscribers[id]; exists {
		return ErrSubscriberAlreadyExists
	}

	q.subscribers[id] = make(chan *T, q.bufferSize)

	return nil
}

// Unsubscribe removes a consumer by ID and closes its message channel. Any pending messages in the subscriber's channel
// are discarded. After unsubscription, the ID can be reused via SubscribeWithID.
//
// Calling this after the Close() will return nil.
//
// Returns
//   - ErrSubscriberDoesNotExist if the ID is not registered.
func (q *Queue[T]) Unsubscribe(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil
	}

	subChan, exists := q.subscribers[id]
	if !exists {
		return ErrSubscriberDoesNotExist
	}

	close(subChan)
	delete(q.subscribers, id)

	return nil
}

// Get retrieves the next message from the subscriber's queue. This method blocks until a message is available, the
// context is cancelled, the queue is closed, or the consumer with ID unsubscribes. It is intended to be called by the
// consumer goroutine.
//
// Returns:
//   - (*T, nil) when a message is successfully retrieved
//   - (nil, ctx.Err()) when the context is cancelled or times out
//   - (nil, ErrQueueClosed) when the queue is closed
//   - (nil, ErrSubscriberDoesNotExist) when the consumer with ID unsubscribes.
func (q *Queue[T]) Get(ctx context.Context, id string) (*T, error) {
	q.mu.RLock()

	if q.closed {
		q.mu.RUnlock()
		return nil, ErrQueueClosed
	}

	subChan, exists := q.subscribers[id]
	q.mu.RUnlock()

	if !exists {
		return nil, ErrSubscriberDoesNotExist
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case msg, ok := <-subChan:
		if !ok {
			q.mu.RLock()
			closed := q.closed
			q.mu.RUnlock()
			if closed {
				return nil, ErrQueueClosed
			}
			return nil, ErrSubscriberDoesNotExist
		}
		return msg, nil
	}
}

// Whisper sends the message to the subscriber with the specified ID. This method blocks until the message is sent, the
// context is cancelled, the queue is closed, or the consumer with ID unsubscribes. It is intended to be called by the
// producer goroutine.
//
// Returns:
//   - nil when the message is successfully delivered
//   - ctx.Err() if the context is cancelled.
//   - ErrQueueClosed when the queue is closed
//   - ErrSubscriberDoesNotExist if the channel is closed.
func (q *Queue[T]) Whisper(ctx context.Context, id string, t *T) error {
	q.mu.RLock()

	if q.closed {
		q.mu.RUnlock()
		return ErrQueueClosed
	}

	subChan, exists := q.subscribers[id]
	if !exists {
		q.mu.RUnlock()
		return ErrSubscriberDoesNotExist
	}
	q.mu.RUnlock()

	return q.locklessWhisper(ctx, t, subChan)
}

// Broadcast sends the message to the all the consumers with available. This method blocks until the message is sent to
// all consumers, the context is cancelled, the queue is closed, or the consumer with ID unsubscribes. It is intended
// to be called by the producer goroutine.
//
// Returns:
//   - nil when the message is successfully delivered
//   - ctx.Err() if the context is cancelled.
//   - ErrQueueClosed when the queue is closed
//   - ErrSubscriberDoesNotExist if the channel is closed.
func (q *Queue[T]) Broadcast(ctx context.Context, t *T) error {
	q.mu.RLock()

	if q.closed {
		q.mu.RUnlock()
		return ErrQueueClosed
	}

	subscribersCopy := make([]chan *T, 0, len(q.subscribers))
	for _, v := range q.subscribers {
		subscribersCopy = append(subscribersCopy, v)
	}
	q.mu.RUnlock()

	var returnErr error
	for _, subChan := range subscribersCopy {
		// Only return if the context is cancelled or timed-out. If the consumer unsubscribed then continue with others
		if err := q.locklessWhisper(ctx, t, subChan); err != nil && err == ctx.Err() {
			return err
		} else if err == ErrSubscriberDoesNotExist {
			returnErr = err
		}
	}

	return returnErr
}

// locklessWhisper sends the given message to the given channel with the specified context without any locks. This means
// that the queue we are sending can be closed at any time, this is why it returns ErrSubscriberDoesNotExist.
//
// Returns:
//   - ErrSubscriberDoesNotExist if the channel is closed.
//   - ctx.Err() if the context is cancelled.
func (q *Queue[T]) locklessWhisper(ctx context.Context, t *T, subChan chan *T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// We may want to check for what kind of error this is that caused a panic. But there is only one place
			// which is the closed channel. So we will recover and set the return value as ErrSubscriberDoesNotExist.
			//
			// The reason why we are doing this is because we don't want the mutex to cover on subChan insert. If it
			// does this might cause a deadlock since the Close cannot be called without the mutex being freed.
			err = ErrSubscriberDoesNotExist
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()

	// We are out of the critical section so if the channel is closed this will cause a panic.
	case subChan <- t:
	}

	return
}

// GetIDs retrieves the IDs of the subscribed consumers.
//
// Returns ErrQueueClosed if the queue is closed.
func (q *Queue[T]) GetIDs() ([]string, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.closed {
		return nil, ErrQueueClosed
	}

	ids := make([]string, 0, len(q.subscribers))
	for id := range q.subscribers {
		ids = append(ids, id)
	}

	return ids, nil
}

// Close shuts down the queue and releases all resources. All subscriber channels are closed, which will cause pending
// Get calls to return ErrQueueClosed. Subsequent calls to Send or Broadcast will return ErrQueueClosed.
//
// This method is idempotent; calling it multiple times has no additional effect.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}
	q.closed = true

	for _, subChan := range q.subscribers {
		close(subChan)
	}
	q.subscribers = make(map[string]chan *T)
}

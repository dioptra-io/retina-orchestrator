package orchestrator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrIDDoesNotExist           = errors.New("id does not exist")
	ErrIDAlreadyExist           = errors.New("id already exist")
	ErrDroppedAtLeastOneMessage = errors.New("dropped at least one message")
)

type consumer[T any] struct {
	consumerChan chan *T
	done         chan struct{} // Closed when consumer is unregistered
	unregistered atomic.Bool
}

type Messenger[T any] struct {
	mu               sync.RWMutex
	consumers        map[string]*consumer[T]
	notificationChan chan struct{}
}

func NewMessenger[T any]() *Messenger[T] {
	return &Messenger[T]{
		consumers:        make(map[string]*consumer[T]),
		notificationChan: make(chan struct{}),
	}
}

func (m *Messenger[T]) RegisterAs(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	cons, ok := m.consumers[id]

	if ok && !cons.unregistered.Load() {
		return ErrIDAlreadyExist
	} else if ok && cons.unregistered.Load() {
		cons.unregistered.Store(false)
		cons.consumerChan = make(chan *T, 100)
		cons.done = make(chan struct{})
		return nil
	} else {
		m.consumers[id] = &consumer[T]{
			consumerChan: make(chan *T, 100),
			done:         make(chan struct{}),
		}
		return nil
	}
}

func (m *Messenger[T]) UnregisterAs(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cons, ok := m.consumers[id]

	if ok && !cons.unregistered.Load() {
		cons.unregistered.Store(true)

		// Close done channel to unblock any GetAs calls
		close(cons.done)

		// Notify producers by closing notificationChan
		select {
		case <-m.notificationChan:
			return
		default:
			close(m.notificationChan)
			return
		}
	}
}

func (m *Messenger[T]) GetAs(ctx context.Context, id string) (*T, error) {
	m.mu.RLock()

	cons, ok := m.consumers[id]
	if !ok {
		m.mu.RUnlock()
		return nil, ErrIDDoesNotExist
	}

	if cons.unregistered.Load() {
		m.mu.RUnlock()
		return nil, ErrIDDoesNotExist
	}

	// Cache channels to prevent races
	consChan := cons.consumerChan
	doneChan := cons.done

	m.mu.RUnlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-doneChan:
		return nil, ErrIDDoesNotExist

	case msg := <-consChan:
		return msg, nil
	}
}

func (m *Messenger[T]) SendTo(ctx context.Context, id string, msg *T) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = ErrIDDoesNotExist
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		m.mu.RLock()

		cons, ok := m.consumers[id]
		if !ok {
			m.mu.RUnlock()
			return ErrIDDoesNotExist
		}

		// Cache channels to prevent races
		notifChan := m.notificationChan
		consChan := cons.consumerChan

		m.mu.RUnlock()

		select {
		case <-ctx.Done():
			return ctx.Err()

		case consChan <- msg:
			return nil

		case <-notifChan:
			m.cleanup(notifChan)
		}
	}
}

func (m *Messenger[T]) SendAll(ctx context.Context, msg *T) error {
	m.mu.RLock()

	consumerIDs := make([]string, 0, len(m.consumers))
	for id := range m.consumers {
		consumerIDs = append(consumerIDs, id)
	}

	m.mu.RUnlock()

	droppedCount := 0

	for _, id := range consumerIDs {
		if err := m.SendTo(ctx, id, msg); err != nil {
			if err == ctx.Err() {
				return err
			}
			if err == ErrIDDoesNotExist {
				droppedCount++
			}
		}
	}

	if droppedCount == 0 {
		return nil
	}
	return ErrDroppedAtLeastOneMessage
}

func (m *Messenger[T]) cleanup(oldNotifChan chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Another goroutine already cleaned up
	if m.notificationChan != oldNotifChan {
		return
	}

	// Remove unregistered consumers (don't close consumerChan to avoid race)
	for id, cons := range m.consumers {
		if cons.unregistered.Load() {
			delete(m.consumers, id)
		}
	}

	// Restore notification channel
	m.notificationChan = make(chan struct{})
}

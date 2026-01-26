package retina

import (
	"context"
	"errors"
	"sync"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
)

var (
	// ErrChannelClosed is returned when attempting to send or receive on a
	// closed channel.
	ErrChannelClosed = errors.New("channel is closed")
	// ErrKeyNotFound is returned when the specified key does not exist in the
	// queue.
	ErrKeyNotFound = errors.New("key not found")
	// ErrKeyExists is returned when attempting to add a key that already exists
	// in the queue.
	ErrKeyExists = errors.New("key already exists")
)

// AgentQueue is an implementation of a thread safe set of agents and channels.
type AgentQueue[T any] struct {
	set     map[string]*api.AgentInfo
	channel map[string]chan *T
	mu      sync.Mutex
}

func NewSafeMap[T any]() *AgentQueue[T] {
	return &AgentQueue[T]{
		set:     make(map[string]*api.AgentInfo),
		channel: make(map[string]chan *T),
	}
}

// Elements returns the current set of elements.
func (s *AgentQueue[T]) Elements() []*api.AgentInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	array := make([]*api.AgentInfo, 0, len(s.set))
	for _, e := range s.set {
		array = append(array, e)
	}
	return array
}

// Contains checks if the given key is in the map. Check is done in O(1).
func (s *AgentQueue[T]) Contains(key string) (*api.AgentInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.set[key]; ok {
		return e, nil
	}
	return nil, ErrKeyNotFound
}

// AddAgent adds the element to the map. Returns true if element does not
// already exist.
func (s *AgentQueue[T]) AddAgent(key string, e *api.AgentInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.set[key]; ok {
		return ErrKeyExists
	}
	s.set[key] = e
	s.channel[key] = make(chan *T)
	return nil
}

// RemoveAgent removes the key from the map. Returns false if element does not
// already exist.
func (s *AgentQueue[T]) RemoveAgent(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.set[key]; !ok {
		return ErrKeyNotFound
	}
	delete(s.set, key)
	delete(s.channel, key)
	return nil
}

// Send sends a value to the channel associated with the given key. It is
// best-effort: returns an error if the context is cancelled, the key doesn't
// exist, or the channel is closed.
func (s *AgentQueue[T]) Send(ctx context.Context, key string, value *T) (err error) {
	s.mu.Lock()
	ch, ok := s.channel[key]
	s.mu.Unlock()

	if !ok {
		return ErrKeyNotFound
	}

	// Recover from panic if channel is closed during send
	defer func() {
		if r := recover(); r != nil {
			err = ErrChannelClosed
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case ch <- value:
		return nil
	}
}

// Receive receives a value from the channel associated with the given key. It
// is best-effort: returns an error if the context is cancelled, the key doesn't
// exist, or the channel is closed.
func (s *AgentQueue[T]) Receive(ctx context.Context, key string) (*T, error) {
	s.mu.Lock()
	ch, ok := s.channel[key]
	s.mu.Unlock()

	if !ok {
		return nil, ErrKeyNotFound
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case value, ok := <-ch:
		if !ok {
			return nil, ErrChannelClosed
		}
		return value, nil
	}
}

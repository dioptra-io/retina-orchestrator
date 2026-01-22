package retina

import "sync"

// SafeMap is an implementation of a thread safe set ot Ts.
type SafeMap[T any] struct {
	set map[string]*T
	mu  sync.Mutex
}

func NewSafeMap[T any]() *SafeMap[T] {
	return &SafeMap[T]{
		set: make(map[string]*T),
	}
}

// Elements returns the current set of elements.
func (s *SafeMap[T]) Elements() []*T {
	s.mu.Lock()
	defer s.mu.Unlock()

	array := make([]*T, len(s.set))
	for _, e := range s.set {
		array = append(array, e)
	}
	return array
}

// Contains checks if the given key is in the map. Check is done in O(1).
func (s *SafeMap[T]) Contains(key string) (*T, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.set[key]
	return e, ok
}

// Add adds the element to the map. Returns true if element does not already
// exist.
func (s *SafeMap[T]) Add(key string, e *T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.set[key]; ok {
		return false
	}
	s.set[key] = e
	return true
}

// Pop removes the key from the map. Returns false if element does not already
// exist.
func (s *SafeMap[T]) Pop(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.set[key]; !ok {
		return false
	}
	delete(s.set, key)
	return true
}

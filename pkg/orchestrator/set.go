package orchestrator

import (
	"sync"
)

// set provides a thread-safe set implementation for storing unique values.
//
// The zero value is not usable; create instances with NewSet.
type set[T comparable] struct {
	mu    sync.RWMutex
	items map[T]struct{}
}

// newSet creates a new empty Set.
func newSet[T comparable]() *set[T] {
	return &set[T]{
		items: make(map[T]struct{}),
	}
}

// Add inserts a value into the set.
//
// Returns true if the value was added, false if it already existed.
func (s *set[T]) Add(value T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[value]; exists {
		return false
	}
	s.items[value] = struct{}{}
	return true
}

// Remove deletes a value from the set.
//
// Returns true if the value was removed, false if it didn't exist.
func (s *set[T]) Remove(value T) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[value]; !exists {
		return false
	}
	delete(s.items, value)
	return true
}

// Contains reports whether the value is in the set.
func (s *set[T]) Contains(value T) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, exists := s.items[value]
	return exists
}

// Len returns the number of elements in the set.
func (s *set[T]) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items)
}

// Clear removes all elements from the set.
func (s *set[T]) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = make(map[T]struct{})
}

// Values returns a slice containing all elements in the set.
// The order is not guaranteed.
func (s *set[T]) Values() []T {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values := make([]T, 0, len(s.items))
	for item := range s.items {
		values = append(values, item)
	}
	return values
}

// Union returns a new set containing all elements from both sets.
func (s *set[T]) Union(other *set[T]) *set[T] {
	result := newSet[T]()

	s.mu.RLock()
	for item := range s.items {
		result.items[item] = struct{}{}
	}
	s.mu.RUnlock()

	other.mu.RLock()
	for item := range other.items {
		result.items[item] = struct{}{}
	}
	other.mu.RUnlock()

	return result
}

// Intersection returns a new set containing elements present in both sets.
func (s *set[T]) Intersection(other *set[T]) *set[T] {
	result := newSet[T]()

	s.mu.RLock()
	defer s.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	// Iterate over the smaller set for efficiency
	small, large := s.items, other.items
	if len(s.items) > len(other.items) {
		small, large = other.items, s.items
	}

	for item := range small {
		if _, exists := large[item]; exists {
			result.items[item] = struct{}{}
		}
	}

	return result
}

// Difference returns a new set containing elements in s that are not in other.
func (s *set[T]) Difference(other *set[T]) *set[T] {
	result := newSet[T]()

	s.mu.RLock()
	defer s.mu.RUnlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	for item := range s.items {
		if _, exists := other.items[item]; !exists {
			result.items[item] = struct{}{}
		}
	}

	return result
}

package retina

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

var (
	// ErrNoDirectives is used to tell there are no directives.
	ErrNoDirectives = errors.New("there needs to be at least one directive")
)

// PDScheduler is a set of probing directives. It is used to generate the
// probing directives and implement responsible probing.
type PDScheduler struct {
	// mutex protects the set state.
	mutex sync.Mutex
	// directives is the set of directives that is used.
	directives []*api.ProbingDirective
	// cooldowns tracks the last time each directive was selected. If this time
	// is in the future then it cannot be selected.
	cooldowns []time.Time
	// cooldownPeriod is the minimum time between selections of the same
	// directive.
	cooldownPeriod time.Duration
}

// NewProbingDirectiveScheduler creates a new set with a cooldown interwal of 1
// seconds.
func NewProbingDirectiveScheduler(cooldown time.Duration) *PDScheduler {
	return &PDScheduler{
		directives:     make([]*api.ProbingDirective, 0),
		cooldowns:      make([]time.Time, 0),
		cooldownPeriod: cooldown,
	}
}

// Select selects a probing directive from the list. If the earliest one has a
// cooldown then it waits for the cooldown. If the context is cancelled during
// the wait then it returns the context cancelled error.
func (s *PDScheduler) Select(ctx context.Context) (*api.ProbingDirective, error) {
	// If the given context is cancelled, then immediately return the error.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if len(s.directives) == 0 {
		return nil, ErrNoDirectives
	}

	earliestIndex := s.earliest()
	now := time.Now()

	if s.cooldowns[earliestIndex].Before(now) {
		return s.directives[earliestIndex], nil
	}

	// We take the current time again because otherwise we will be waiting
	// little longer.
	timer := time.NewTimer(time.Until(s.cooldowns[earliestIndex]))
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-timer.C:
		return s.directives[earliestIndex], nil
	}
}

// Set sets the current set with the given set of probing directives. When added
// the array is shuffled to ensure pseudo-randomness.
func (s *PDScheduler) Set(directives []*api.ProbingDirective) error {
	if len(directives) == 0 {
		return ErrNoDirectives
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Copy and shuffle the directives
	shuffled := make([]*api.ProbingDirective, len(directives))
	copy(shuffled, directives)

	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	cooldowns := make([]time.Time, len(shuffled))
	for i := range len(shuffled) {
		cooldowns[i] = time.Now()
	}

	s.directives = shuffled
	s.cooldowns = cooldowns

	return nil
}

// earliest gets the earliest probing directive's index. This method is not
// thread safe.
// The current implementation does the selection by linear search, this can be
// more efficient if a heap is used.
//
// TODO: implement this using a heap to reduce the complexity.
func (s *PDScheduler) earliest() int {
	// This needs to be done via heap to reduce complexity from O(n) to O(logn)
	earliestIndex := 0
	for i := range len(s.directives) {
		currentCooldown := s.cooldowns[i]

		if s.cooldowns[earliestIndex].After(currentCooldown) {
			earliestIndex = i
		}
	}

	return earliestIndex
}

package retina

import (
	"context"
	"errors"
	"math/rand"
	"sync"

	"github.com/dioptra-io/retina-commons/api/v1"
)

var (
	// ErrEmptyPDSet is thrown when the given array of PDs has 0 size.
	ErrEmptyPDSet = errors.New("given probing directive array is empty")
)

// PDIssuer is the implementation of the issuing algorithm for the probing
// directives.
//
// It satisfies the responsible probing constraints by assigning a probability
// value of issuing the PD. If the result of the Bernoulli experiment is lower
// than the probability, then the PD is skipped. If not it is returned.
//
// If there is no PD then it blocks the goroutine until the PDs is set.
type PDIssuer struct {
	// buffer is the actual buffer of probing directives.
	buffer []*api.ProbingDirective

	// inidicies is used to shuffle the order of the probing directives.
	inidicies []int

	// next is the pointer to the current index on the buffer.
	next uint

	// random is used to generate random variables.
	random rand.Rand

	// signalChan is used to wake up the sleeping goroutine. This is closed
	// after setting the buffer and not used again.
	signalChan chan struct{}

	// once is used to close the signalChan.
	once sync.Once

	// mutex is used to protect the buffer and inidicies from race conditions.
	mutex sync.Mutex
}

func NewPDIssuer() *PDIssuer {
	return &PDIssuer{
		buffer:     make([]*api.ProbingDirective, 0),
		inidicies:  make([]int, 0),
		next:       0,
		signalChan: make(chan struct{}),
	}
}

// Issue selects a directive from the buffer.
//
// If the buffer is empty (meaning the PDs are not set by the retina-generator),
// then the method blocks until the context cancellation. Returns the context's
// error.
func (i *PDIssuer) Issue(ctx context.Context) (*api.ProbingDirective, error) {
	// This checks the context cancellation and the signall channel, if there
	// are no elements in the buffer this makes the Issue method wait until
	// there are elements.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-i.signalChan:
	}

	i.mutex.Lock()
	defer i.mutex.Unlock()

	// The problem here is that we acquire the lock and do a busy for loop. If
	// for some reason we cannot select the PD (all probabilities returns zero)
	// then we deadlock. This should not happen on the prob methiod.
	for {
		// Check for context cancellation.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		default:
		}

		// Get the current PD.
		currentPD := i.buffer[i.inidicies[i.next]]
		i.next = (i.next + 1) % uint(len(i.buffer))

		// Make a Bernoulli experiment.
		if i.random.Float32() < i.prob(i.next-1, currentPD) {
			return currentPD, nil
		}
	}
}

// Set reinitializes the buffer and the inidicies, and wakes up all the the
// Issuer goroutines.
//
// If the given array of PDs has zero length, it returns ErrEmptyPDSet.
func (i *PDIssuer) Set(pds []*api.ProbingDirective) error {
	if len(pds) == 0 {
		return ErrEmptyPDSet
	}
	// Try to acquire the lock and initialize the buffer and the inidicies.
	i.mutex.Lock()
	defer i.mutex.Unlock()

	i.buffer = pds
	i.inidicies = make([]int, len(pds))

	// Initially this is a one to one map.
	for j := range len(i.buffer) {
		i.inidicies[j] = j
	}

	// Closing the signal channel unblocks all the callers of the Issue. This is
	// only done once to prevent double free.
	i.once.Do(func() {
		close(i.signalChan)
	})

	return nil
}

// prob is used to compute the probability of the currently selected probing
// directive to be selected.
//
// As default it returns 1.0 but the actual algorithm will be discussed in the
// design document.
func (i *PDIssuer) prob(index uint, currentPD *api.ProbingDirective) float32 {
	return 1.0
}

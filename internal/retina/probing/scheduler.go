package probing

import (
	"bufio"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// directiveEntry is the entry of a directive. It contains the last hit
// addresses, issuance probability and a pointer to the directive itself.
type directiveEntry struct {
	lastHitNearAddress net.IP
	lastHitFarAddress  net.IP
	issuanceProb       float64
	directive          *api.ProbingDirective
}

// impactEntry stores the current state of the impacts to that address.
type impactEntry struct {
	// directives is a set of ProbingDirective IDs that impacts this address.
	directives map[uint64]*directiveEntry
}

// Scheduler is the implementation of the ProbingDirective Scheduler. It
// implements the ResponsibleProbing algorithm.
type Scheduler struct {
	// mutex is used to prevent race condiditons.
	mutex sync.Mutex
	// directiveMap maps the ProbingDirective's ID into a directiveMapEntry.
	// Entry contains the reference to the actual ProbingDirective, issuance
	// probability, near and far address.
	directiveMap map[uint64]*directiveEntry
	// addressImpactMap maps an address to an addressImpactMapEntry. Entry
	// contains a list of directives that impacts this address.
	addressImpactMap map[string]*impactEntry
	// current is the current index.
	current uint64
	// lastIssue is the last issue time.
	lastIssue time.Time
	// numPDs denotes the number of PDs loaded into the scheduler.
	numPDs int
	// cyclePeriod is the minimum time required between two directives.
	cyclePeriod time.Duration
	// issuePeriod is the time between two directive issues.
	issuePeriod time.Duration
	// randomizer is used to randomize the indices.
	randomizer *randomizer
	// random used for bernoulli experiment.
	random *rand.Rand
}

func NewScheduler(seed uint64, issueRate float64, pdFile string) (*Scheduler, error) {
	pds, err := readPDs(pdFile)
	if err != nil {
		return nil, fmt.Errorf("cannot read from file: %w", err)
	}
	return newSchedulerFromPDs(seed, issueRate, pds)
}

// NewScheduler creates a new empty scheduler.
func newSchedulerFromPDs(seed uint64, issueRate float64, pds []*api.ProbingDirective) (*Scheduler, error) {
	if len(pds) == 0 {
		return nil, fmt.Errorf("invalid arguments: pds length cannot be zero")
	}

	if issueRate <= 0.0 {
		return nil, fmt.Errorf("invalid arguments: issue rate cannot be zero or negative")
	}

	// Create the directiveMap and populate with the given set of directives.
	directiveMap := make(map[uint64]*directiveEntry, len(pds))
	indices := make([]uint64, 0, len(pds))
	for _, pd := range pds {
		// Register to the directive map.
		// Default values is issuance prob of 1.0 and nil near and far
		// addresses.
		directiveMap[pd.ProbingDirectiveID] = &directiveEntry{
			lastHitNearAddress: nil,
			lastHitFarAddress:  nil,
			directive:          pd,
			issuanceProb:       1.0,
		}

		// Add to the indices array.
		indices = append(indices, pd.ProbingDirectiveID)
	}

	randomizer, err := newRandomizer(seed, indices)
	if err != nil {
		return nil, fmt.Errorf("cannot create randomizer: %w", err)
	}

	return &Scheduler{
		directiveMap:     directiveMap,
		addressImpactMap: make(map[string]*impactEntry),
		current:          0,
		numPDs:           len(pds),
		cyclePeriod:      time.Duration(len(pds)) * time.Second / time.Duration(issueRate),
		issuePeriod:      time.Second / time.Duration(issueRate),
		randomizer:       randomizer,
		random:           rand.New(rand.NewPCG(seed, 0)), // #nosec G404
	}, nil
}

// Issue issues a new ProbingDirective and also applies ratelimiting. If the
// expriment fails then it returns nil.
func (s *Scheduler) Issue() *api.ProbingDirective {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Get the corresponding index from the randomizer.
	randomIndex := s.randomizer.Next()

	// Get the directiveMapEntry and increment the current counter.
	dEntry := s.directiveMap[randomIndex]
	s.current = (s.current + 1) % uint64(s.numPDs) // #nosec G115

	// Apply rate-limiting.
	if !s.lastIssue.IsZero() {
		time.Sleep(time.Until(s.lastIssue.Add(s.issuePeriod)))
	}
	s.lastIssue = time.Now()

	if s.random.Float64() < dEntry.issuanceProb {
		return dEntry.directive
	}
	return nil
}

// Update is invoked when there is a returned ForwardingInfoElement returned. It
// returns a non-nil error.
func (s *Scheduler) Update(fie *api.ForwardingInfoElement) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	dEntry, ok := s.directiveMap[fie.ProbingDirectiveID]
	if !ok {
		return fmt.Errorf("given probing directive id is not recognized")
	}

	// Get the old addresses.
	oldNearAddress, oldFarAddress := dEntry.lastHitNearAddress, dEntry.lastHitFarAddress

	// Update the last hit addresses.
	// This means the last hit addresses can be nil.
	dEntry.lastHitNearAddress = fie.NearInfo.ReplyAddress
	dEntry.lastHitFarAddress = fie.FarInfo.ReplyAddress

	// Do the book-keeping.
	if !oldNearAddress.Equal(dEntry.lastHitNearAddress) {
		s.removeImpact(oldNearAddress, dEntry)
		s.addImpact(dEntry.lastHitNearAddress, dEntry)
	}
	if !oldFarAddress.Equal(dEntry.lastHitFarAddress) {
		s.removeImpact(oldFarAddress, dEntry)
		s.addImpact(dEntry.lastHitFarAddress, dEntry)
	}

	// Update the issuance probability.
	numNearImpacts, numFarImpacts := 0, 0
	if dEntry.lastHitNearAddress != nil {
		if entry, ok := s.addressImpactMap[dEntry.lastHitNearAddress.To16().String()]; ok {
			numNearImpacts = len(entry.directives)
		}
	}
	if dEntry.lastHitFarAddress != nil {
		if entry, ok := s.addressImpactMap[dEntry.lastHitFarAddress.To16().String()]; ok {
			numFarImpacts = len(entry.directives)
		}
	}

	denominator := max(numNearImpacts, numFarImpacts)

	if denominator == 0 {
		dEntry.issuanceProb = 1.0
	} else {
		dEntry.issuanceProb = 1.0 / float64(denominator)
	}

	return nil
}

func (s *Scheduler) addImpact(address net.IP, dEntry *directiveEntry) {
	if address == nil {
		return
	}
	if _, ok := s.directiveMap[dEntry.directive.ProbingDirectiveID]; !ok {
		return
	}
	iEntry, ok := s.addressImpactMap[address.To16().String()]
	if !ok {
		iEntry = &impactEntry{
			directives: make(map[uint64]*directiveEntry),
		}
		s.addressImpactMap[address.To16().String()] = iEntry
	}
	iEntry.directives[dEntry.directive.ProbingDirectiveID] = dEntry
}

func (s *Scheduler) removeImpact(address net.IP, dEntry *directiveEntry) {
	if address == nil {
		return
	}
	if _, ok := s.directiveMap[dEntry.directive.ProbingDirectiveID]; !ok {
		return
	}
	iEntry, ok := s.addressImpactMap[address.To16().String()]
	if ok {
		delete(iEntry.directives, dEntry.directive.ProbingDirectiveID)
		if len(iEntry.directives) == 0 {
			delete(s.addressImpactMap, address.To16().String())
		}
	}
}

// readPDs is a utility function that reads the probing directives from the
// given file.
func readPDs(filepath string) ([]*api.ProbingDirective, error) {
	f, err := os.Open(filepath) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	var results []*api.ProbingDirective
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var obj api.ProbingDirective
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			return nil, fmt.Errorf("cannot unmarshal line: %w", err)
		}
		results = append(results, &obj)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return results, nil
}

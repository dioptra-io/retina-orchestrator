package retina

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

var (
	ErrExperimentFailed = errors.New("experiment failed")
)

type directiveMapEntry struct {
	lastHitNearAddress net.IP
	lastHitFarAddress  net.IP
	issuanceProb       float64
	directive          *api.ProbingDirective
}

type addressImpactMapEntry struct {
	// currentExpectedImpact is the sum of the issuance probabilies of the
	// directiveIDs.
	currentExpectedImpact float64

	// directives is a set of ProbingDirective IDs that impacts this address.
	directives map[uint64]*directiveMapEntry
}

type addressImpactMap map[string]*addressImpactMapEntry

func newAddressImpactMap() addressImpactMap {
	return make(map[string]*addressImpactMapEntry, 0)
}

// UpdateAddressAndNormalize first removes the old address from the map only if
// removing the directiveID from the directives results in an empty set. Then
// the new address is inserted. Finally, the normalization of the directive
// probabily is recomputed.
//
// The probability adjustment also depends on other factors such as usefullness
// but for now this is the simplest form. Furhter research on this is required.
func (aim addressImpactMap) UpdateAddressAndNormalize(oldAddress, newAddress net.IP, directiveID uint64, impactCap float64) {
	// Remove the old address
	if addressImpactMapEntryObj, ok := aim[oldAddress.String()]; ok {
		delete(addressImpactMapEntryObj.directives, directiveID)
		if len(addressImpactMapEntryObj.directives) == 0 {
			delete(aim, oldAddress.String())
		}
	}

	// TODO: implement.

	// Normalization should be done for a single directive probability. However
	// this might cause ossiliations but we will see.
}

// scheduler is the implementation of the ProbingDirective scheduler. It
// implements the ResponsibleProbing algorithm.
type scheduler struct {
	// directiveMap maps the ProbingDirective's ID into a directiveMapEntry.
	// Entry contains the reference to the actual ProbingDirective, issuance
	// probability, near and far address.
	directiveMap map[uint64]*directiveMapEntry

	// addressImpactMap maps an address to an addressImpactMapEntry. Entry
	// contains a list of directives that impacts this address.
	addressImpactMap addressImpactMap

	// current is the current index.
	current uint64

	// lastIssue is the last issue time.
	lastIssue time.Time

	// numPDs denotes the number of PDs loaded into the scheduler.
	numPDs int

	// issuePeriod is the minimum time required between two directives.
	issuePeriod time.Duration

	// randomizer is used to randomize the indicies.
	randomizer *randomizer
}

// newScheduler creates a new empty scheduler.
func newScheduler(seed uint64, issueRate float64, pds []*api.ProbingDirective) (*scheduler, error) {
	if len(pds) == 0 {
		return nil, fmt.Errorf("invalid arguments: pds length cannot be zero")
	}

	if issueRate <= 0.0 {
		return nil, fmt.Errorf("invalid arguments: issue rate cannot be zero or negative")
	}

	// Create the directiveMap and populate with the given set of directives.
	directiveMap := make(map[uint64]*directiveMapEntry, len(pds))
	indicies := make([]uint64, 0, len(pds))
	for _, pd := range pds {
		// Register to the directive map.
		directiveMap[pd.ProbingDirectiveID] = &directiveMapEntry{
			lastHitNearAddress: nil,
			lastHitFarAddress:  nil,
			directive:          pd,
			issuanceProb:       1.0,
		}

		// Add to the indicies array.
		indicies = append(indicies, pd.ProbingDirectiveID)
	}

	randomizer, err := newRandomizer(seed, indicies)
	if err != nil {
		return nil, fmt.Errorf("cannot create randomizer: %w", err)
	}

	return &scheduler{
		directiveMap:     directiveMap,
		addressImpactMap: newAddressImpactMap(),
		current:          0,
		numPDs:           len(pds),
		issuePeriod:      time.Second / time.Duration(issueRate),
		randomizer:       randomizer,
	}, nil
}

// Issue issues a new ProbingDirective and also applies ratelimiting. If the
// expriment fails then it returns ErrExperimentFailed error.
func (s *scheduler) Issue() (*api.ProbingDirective, error) {
	// randomIndex := s.current // Non-randomized version
	randomIndex := s.randomizer.Next() // Randomized version

	// Get the directiveMapEntry and increment the current counter.
	pdEntry := s.directiveMap[randomIndex]
	s.current = (s.current + 1) % uint64(s.numPDs) // #nosec G115

	// Apply rate-limiting.
	now := time.Now()
	if s.lastIssue.Add(s.issuePeriod).After(now) {
		time.Sleep(now.Sub(s.lastIssue.Add(s.issuePeriod)))
	}
	s.lastIssue = time.Now()

	return pdEntry.directive, nil
}

// Update is invoked when there is a returned ForwardingInfoElement returned. It
// returns a non-nil error.
func (s *scheduler) Update(fie *api.ForwardingInfoElement, impactCap float64) error {
	directiveMapEntryObj, ok := s.directiveMap[fie.ProbingDirectiveID]
	if !ok {
		return fmt.Errorf("given probing directive id is not recognized")
	}

	// This should never happened but if it does, this means there is an issue
	// with the book-keeping. Log and return the error.
	if directiveMapEntryObj.directive.ProbingDirectiveID != fie.ProbingDirectiveID {
		log.Printf("The probing directive id does not match on the ProbingDirective and ForwardingInfoElement.")
		log.Printf("This should never happen and it means there is an internal issue on the book-keeping.")
		return fmt.Errorf("probing directive id does not match on the probing directive and forwarding info element")
	}

	// Get the old addresses.
	oldNearAddress, oldFarAddress := directiveMapEntryObj.lastHitNearAddress, directiveMapEntryObj.lastHitFarAddress

	// Update the last hit addresses.
	// This means the last hit addresses can be nil.
	directiveMapEntryObj.lastHitNearAddress = fie.NearInfo.ReplyAddress
	directiveMapEntryObj.lastHitFarAddress = fie.FarInfo.ReplyAddress

	// Do the book-keeping for the addressImpactMap.
	s.addressImpactMap.UpdateAddressAndNormalize(oldNearAddress, directiveMapEntryObj.lastHitNearAddress, fie.ProbingDirectiveID, impactCap)
	s.addressImpactMap.UpdateAddressAndNormalize(oldFarAddress, directiveMapEntryObj.lastHitFarAddress, fie.ProbingDirectiveID, impactCap)

	return nil
}

// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// SchedulerConfig holds the configuration for the Scheduler.
// All fields are validated by Config.Validate() in orchestrator.go before
// NewScheduler is called.
type SchedulerConfig struct {
	Seed                       uint64
	IssuanceRate               float64
	PDPath                     string
	ActiveSetSize              int
	ConsecutiveMissesThreshold int
	MaxEvictions               int
}

// pdState holds the scheduling state for a single ProbingDirective, including
// the last observed near and far addresses, the current issuance probability,
// the consecutive miss count, and the eviction count.
type pdState struct {
	lastHitNearAddress net.IP
	lastHitFarAddress  net.IP
	issuanceProb       float64
	consecutiveMisses  int
	evictionCount      int
	directive          *api.ProbingDirective
}

// unusedPD is a lightweight representation of a ProbingDirective in the unused
// pool. It omits scheduling state (addresses, probability, miss count) that is
// only meaningful for active directives, reducing memory usage at scale.
type unusedPD struct {
	evictionCount int
	directive     *api.ProbingDirective
}

// promote converts an unusedPD to a pdState ready for insertion into the active set.
func (u *unusedPD) promote() *pdState {
	return &pdState{
		issuanceProb:  1.0,
		evictionCount: u.evictionCount,
		directive:     u.directive,
	}
}

// impactRecord stores the current impact state for a single address.
type impactRecord struct {
	// pds is the set of ProbingDirective IDs currently impacting this address.
	pds map[uint64]*pdState
}

// Scheduler implements the responsible probing algorithm. It schedules
// ProbingDirectives for issuance and updates their issuance probabilities
// based on incoming ForwardingInfoElements.
type Scheduler struct {
	logger  *slog.Logger
	mutex   sync.Mutex
	metrics *Metrics
	config  *SchedulerConfig

	// pdMap maps each ProbingDirective ID to its scheduling state, which holds
	// the directive itself, its issuance probability, and last hit addresses.
	pdMap map[uint64]*pdState
	// impactRecords maps each address to the set of directives impacting it.
	impactRecords map[string]*impactRecord
	// unusedByAgent maps each agent ID to its pool of unused PDs available
	// for replacement when a PD is evicted from the active set.
	unusedByAgent map[string][]*unusedPD

	// lastIssuance is the time of the last issued directive, used for rate limiting.
	lastIssuance   time.Time
	lastCycleBegin time.Time
	// issuancePeriod is the minimum time between two directive issuances,
	// derived from issuanceRate as time.Second / issuanceRate.
	issuancePeriod time.Duration

	randomizer *randomizer
	// random is used for the Bernoulli experiment in NextPD.
	random *rand.Rand
}

// NewScheduler creates a new Scheduler from the given configuration.
// Returns an error if the configuration is invalid or the PD file cannot be read.
func NewScheduler(config *SchedulerConfig, logger *slog.Logger, metrics *Metrics) (*Scheduler, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	pds, err := readPDs(config.PDPath)
	if err != nil {
		return nil, fmt.Errorf("cannot read from file: %w", err)
	}
	if len(pds) == 0 {
		return nil, fmt.Errorf("invalid arguments: pds length cannot be zero")
	}

	logger.Info("Scheduler loaded directives",
		slog.Int("count", len(pds)),
		slog.String("file", config.PDPath))

	pdMap := make(map[uint64]*pdState, config.ActiveSetSize)
	indices := make([]uint64, 0, config.ActiveSetSize)
	unusedByAgent := make(map[string][]*unusedPD)

	// First ActiveSetSize PDs go into the active set; the remainder into the unused pool.
	// The file should be shuffled before loading to avoid IP range bias in the active set.
	for i, pd := range pds {
		if i < config.ActiveSetSize {
			pdMap[pd.ProbingDirectiveID] = &pdState{
				directive:    pd,
				issuanceProb: 1.0,
			}
			indices = append(indices, pd.ProbingDirectiveID)
		} else {
			unusedByAgent[pd.AgentID] = append(unusedByAgent[pd.AgentID], &unusedPD{
				directive: pd,
			})
		}
	}

	randomizer, err := newRandomizer(config.Seed, indices)
	if err != nil {
		return nil, fmt.Errorf("cannot create randomizer: %w", err)
	}

	metrics.PDsTotal.Set(float64(len(pds)))
	metrics.PDsActiveTotal.Set(float64(len(pdMap)))
	for agentID, pool := range unusedByAgent {
		metrics.PDsUnusedTotal.WithLabelValues(agentID).Set(float64(len(pool)))
	}

	return &Scheduler{
		logger:         logger,
		metrics:        metrics,
		config:         config,
		pdMap:          pdMap,
		impactRecords:  make(map[string]*impactRecord),
		unusedByAgent:  unusedByAgent,
		issuancePeriod: time.Duration(float64(time.Second) / config.IssuanceRate),
		randomizer:     randomizer,
		random:         rand.New(rand.NewPCG(config.Seed, 0)), // #nosec G404
	}, nil
}

// NextPD returns the next ProbingDirective candidate. It blocks until the
// rate limit allows the next issuance, then runs a Bernoulli experiment to
// decide whether to issue the directive. If the Bernoulli experiment fails,
// the directive is replaced with a new candidate from the unused pool for the
// same agent. Returns nil if the unused pool is exhausted.
func (s *Scheduler) NextPD() *api.ProbingDirective {
	s.mutex.Lock()
	oldCycle := s.randomizer.Cycle()
	pd := s.pdMap[s.randomizer.Next()]
	newCycle := s.randomizer.Cycle()
	nextTime := s.lastIssuance.Add(s.issuancePeriod)
	var issue bool
	if pd != nil {
		// Bernoulli experiment is inside the mutex to avoid a data race on s.random,
		// which is also accessed in replacePD under the mutex.
		issue = s.random.Float64() < pd.issuanceProb
	}
	s.mutex.Unlock()

	if s.issuancePeriod >= 10*time.Millisecond {
		time.Sleep(time.Until(nextTime))
	} else {
		for time.Now().Before(nextTime) { // busylock
		}
	}

	s.mutex.Lock()
	s.lastIssuance = time.Now()
	s.mutex.Unlock()

	if oldCycle != newCycle {
		if !s.lastCycleBegin.IsZero() {
			cycleDuration := time.Since(s.lastCycleBegin)
			s.metrics.CycleDurationSeconds.Observe(float64(cycleDuration.Seconds()))
		}
		s.lastCycleBegin = time.Now()
		s.metrics.CyclesTotal.Inc()
	}

	if pd == nil {
		return nil
	}
	if issue {
		return pd.directive
	}

	s.mutex.Lock()
	replacement := s.replacePD(pd)
	s.mutex.Unlock()
	s.metrics.PDsReplacedBernoulliTotal.WithLabelValues(pd.directive.AgentID).Inc()
	s.logger.Debug("PD replaced (Bernoulli)",
		slog.Uint64("pd_id", pd.directive.ProbingDirectiveID))
	if replacement != nil {
		return replacement.directive
	}
	s.logger.Error("No replacement available, pool exhausted",
		slog.String("agent_id", pd.directive.AgentID))
	return nil
}

// recycleOrEvict returns the PD to the unused pool or permanently evicts it
// if MaxEvictions has been reached. Must be called with s.mutex held.
func (s *Scheduler) recycleOrEvict(pd *pdState) {
	agentID := pd.directive.AgentID
	if pd.evictionCount < s.config.MaxEvictions {
		s.unusedByAgent[agentID] = append(s.unusedByAgent[agentID], &unusedPD{
			evictionCount: pd.evictionCount + 1,
			directive:     pd.directive,
		})
		s.metrics.PDsUnusedTotal.WithLabelValues(agentID).Inc()
	} else {
		s.metrics.PDsEvictedTotal.WithLabelValues(agentID).Inc()
		s.logger.Debug("PD permanently evicted",
			slog.Uint64("pd_id", pd.directive.ProbingDirectiveID))
	}
}

// replacePD replaces the given PD in the active set with a random draw from
// the unused pool for the same agent. Must be called with s.mutex held.
func (s *Scheduler) replacePD(pd *pdState) *pdState {
	// Clean up impact records
	s.removeImpact(pd.lastHitNearAddress, pd)
	s.removeImpact(pd.lastHitFarAddress, pd)

	agentID := pd.directive.AgentID

	// Remove from active set
	delete(s.pdMap, pd.directive.ProbingDirectiveID)
	s.metrics.PDsActiveTotal.Dec()

	// Draw replacement before returning pd to the unused pool — if pd were added
	// first, it could be randomly redrawn as its own replacement.
	unused := s.unusedByAgent[agentID]
	if len(unused) == 0 {
		s.logger.Warn("Unused pool exhausted for agent", slog.String("agent_id", agentID))
		s.recycleOrEvict(pd)
		return nil
	}

	idx := s.random.IntN(len(unused))
	rawReplacement := unused[idx]
	// Swap and shrink unused pool — O(1) removal without preserving order.
	unused[idx] = unused[len(unused)-1]
	unused[len(unused)-1] = nil // avoid memory leak
	s.unusedByAgent[agentID] = unused[:len(unused)-1]
	s.metrics.PDsUnusedTotal.WithLabelValues(agentID).Dec()

	// Promote replacement to active pdState and add to active set
	replacement := rawReplacement.promote()
	s.pdMap[replacement.directive.ProbingDirectiveID] = replacement
	s.randomizer.Replace(pd.directive.ProbingDirectiveID, replacement.directive.ProbingDirectiveID)
	s.metrics.PDsActiveTotal.Inc()

	// Replacement is issued naturally in the next NextPD call via the randomizer.
	s.recycleOrEvict(pd)

	return replacement
}

// UpdateFromFIE updates the scheduling state of a directive based on an
// incoming ForwardingInfoElement. It records the near and far addresses
// observed in the FIE, recalculates the issuance probability according to
// the number of directives currently impacting those addresses, and tracks
// consecutive misses — defined as FIEs where either near or far reply is
// absent — to trigger replacement of unresponsive directives. A directive
// is considered yielding only when both near and far replies are present.
// Returns an error if the directive ID is not recognized.
func (s *Scheduler) UpdateFromFIE(fie *api.ForwardingInfoElement) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	pd, ok := s.pdMap[fie.ProbingDirectiveID]
	// The PD may have been replaced since the probe was issued; stale FIEs are expected and ignored.
	if !ok {
		s.logger.Debug("FIE for replaced PD ignored",
			slog.Uint64("pd_id", fie.ProbingDirectiveID))
		return nil
	}
	oldNearAddress, oldFarAddress := pd.lastHitNearAddress, pd.lastHitFarAddress

	// Last hit addresses can be nil (e.g. on probe timeout).
	pd.lastHitNearAddress = nil
	if fie.NearInfo != nil {
		pd.lastHitNearAddress = fie.NearInfo.ReplyAddress
	}
	pd.lastHitFarAddress = nil
	if fie.FarInfo != nil {
		pd.lastHitFarAddress = fie.FarInfo.ReplyAddress
	}

	if ipKey(oldNearAddress) != ipKey(pd.lastHitNearAddress) {
		s.removeImpact(oldNearAddress, pd)
		s.recordImpact(pd.lastHitNearAddress, pd)
	}
	if ipKey(oldFarAddress) != ipKey(pd.lastHitFarAddress) {
		s.removeImpact(oldFarAddress, pd)
		s.recordImpact(pd.lastHitFarAddress, pd)
	}

	numNearImpacts, numFarImpacts := 0, 0
	if pd.lastHitNearAddress != nil {
		if rec, ok := s.impactRecords[ipKey(pd.lastHitNearAddress)]; ok {
			numNearImpacts = len(rec.pds)
		}
	}
	if pd.lastHitFarAddress != nil {
		if rec, ok := s.impactRecords[ipKey(pd.lastHitFarAddress)]; ok {
			numFarImpacts = len(rec.pds)
		}
	}

	maxImpacts := max(numNearImpacts, numFarImpacts)
	if maxImpacts == 0 {
		pd.issuanceProb = 1.0
	} else {
		pd.issuanceProb = 1.0 / float64(maxImpacts)
	}

	if fie.NearInfo == nil || fie.FarInfo == nil {
		pd.consecutiveMisses++
		if pd.consecutiveMisses >= s.config.ConsecutiveMissesThreshold {
			s.metrics.PDsReplacedMissTotal.WithLabelValues(pd.directive.AgentID).Inc()
			s.logger.Debug("PD replaced (consecutive misses)",
				slog.Uint64("pd_id", pd.directive.ProbingDirectiveID))
			// Return value not used — replacement is issued naturally in the next NextPD call.
			s.replacePD(pd)
		}
	} else {
		pd.consecutiveMisses = 0
	}

	return nil
}

// recordImpact records that the given PD is impacting the specified address.
// Creates a new impact record for the address if none exists yet.
func (s *Scheduler) recordImpact(address net.IP, pd *pdState) {
	if address == nil {
		return
	}
	key := ipKey(address)
	record, ok := s.impactRecords[key]
	if !ok {
		record = &impactRecord{
			pds: make(map[uint64]*pdState),
		}
		s.impactRecords[key] = record
	}
	record.pds[pd.directive.ProbingDirectiveID] = pd
}

// removeImpact removes the given PD from the impact record of the specified
// address. Deletes the impact record entirely if no other PDs are impacting it.
func (s *Scheduler) removeImpact(address net.IP, pd *pdState) {
	if address == nil {
		return
	}
	key := ipKey(address)
	record, ok := s.impactRecords[key]
	if ok {
		delete(record.pds, pd.directive.ProbingDirectiveID)
		if len(record.pds) == 0 {
			delete(s.impactRecords, key)
		}
	}
}

// ipKey returns a normalized string key for a net.IP address, suitable for
// use as a map key. Returns an empty string for nil addresses.
func ipKey(ip net.IP) string {
	if ip == nil {
		return ""
	}
	return ip.To16().String()
}

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

// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/dioptra-io/retina-commons/model"
	wire "github.com/dioptra-io/retina-commons/wire/v2"
	"google.golang.org/protobuf/encoding/protojson"
)

// SchedulerConfig holds the configuration for the Scheduler.
// All fields are validated by Config.Validate() in orchestrator.go before
// NewScheduler is called.
type SchedulerConfig struct {
	Seed                       uint64
	IssuanceRate               float64
	ImpactThreshold            float64
	PDPathV4                   string
	PDPathV6                   string
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
	directive          *model.ProbingDirective
}

// unusedPD is a lightweight representation of a ProbingDirective in the unused
// pool. It omits scheduling state (addresses, probability, miss count) that is
// only meaningful for active directives, reducing memory usage at scale.
type unusedPD struct {
	evictionCount int
	directive     *model.ProbingDirective
}

// promote converts an unusedPD to a pdState ready for insertion into the active set.
func (u *unusedPD) promote() *pdState {
	return &pdState{
		issuanceProb:  1.0,
		evictionCount: u.evictionCount,
		directive:     u.directive,
	}
}

// ipIdx returns 0 for IPv4 and 1 for IPv6, used to index the per-protocol
// unused pool slices.
func ipIdx(ipVersion wire.IPVersion) int {
	if ipVersion == wire.IPVersion_IP_VERSION_IPV6 {
		return 1
	}
	return 0
}

// ipVersionLabel returns a compact label for the IP version, used in metrics.
func ipVersionLabel(ipVersion wire.IPVersion) string {
	if ipVersion == wire.IPVersion_IP_VERSION_IPV6 {
		return "6"
	}
	return "4"
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
	// unusedByAgent maps each agent ID to a 2-element array of unused PD pools,
	// indexed by IP version (0=IPv4, 1=IPv6). Replacement is protocol-matched
	// to maintain a stable IPv4/IPv6 distribution in the active set.
	unusedByAgent map[string][2][]*unusedPD

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

// loadPDsIntoPool fills the active set and unused pool from a slice of PDs.
// The first maxActive PDs go into pdMap and indices (active set); the rest go
// into the per-agent unused pool. The slice should be shuffled before calling
// to avoid IP range bias in the active set.
// seen tracks all PD IDs across both V4 and V6 calls to detect duplicates.
func loadPDsIntoPool(
	pds []*model.ProbingDirective,
	maxActive int,
	pdMap map[uint64]*pdState,
	indices *[]uint64,
	unusedByAgent map[string][2][]*unusedPD,
	seen map[uint64]struct{},
) error {
	for i, pd := range pds {
		if _, exists := seen[pd.ProbingDirectiveID]; exists {
			return fmt.Errorf("duplicate PD ID %d", pd.ProbingDirectiveID)
		}
		seen[pd.ProbingDirectiveID] = struct{}{}
		if i < maxActive {
			pdMap[pd.ProbingDirectiveID] = &pdState{
				directive:    pd,
				issuanceProb: 1.0,
			}
			*indices = append(*indices, pd.ProbingDirectiveID)
		} else {
			pools := unusedByAgent[pd.AgentID]
			ipVersion := ipIdx(pd.IPVersion)
			pools[ipVersion] = append(pools[ipVersion], &unusedPD{
				directive: pd,
			})
			unusedByAgent[pd.AgentID] = pools
		}
	}
	return nil
}

// loadAllPDs reads the V4 and V6 PD files from config. Either path may be empty,
// but at least one file must produce non-zero PDs.
func loadAllPDs(config *SchedulerConfig) ([]*model.ProbingDirective, []*model.ProbingDirective, error) {
	var (
		err          error
		v4pds, v6pds []*model.ProbingDirective
	)
	if config.PDPathV4 != "" {
		v4pds, err = readPDs(config.PDPathV4)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot read IPv4 PD file: %w", err)
		}
	}
	if config.PDPathV6 != "" {
		v6pds, err = readPDs(config.PDPathV6)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot read IPv6 PD file: %w", err)
		}
	}
	if len(v4pds) == 0 && len(v6pds) == 0 {
		return nil, nil, fmt.Errorf("invalid arguments: both PD files are empty")
	}
	return v4pds, v6pds, nil
}

// NewScheduler creates a new Scheduler from the given configuration.
// Returns an error if the configuration is invalid or the PD files cannot be read.
func NewScheduler(config *SchedulerConfig, logger *slog.Logger, metrics *Metrics) (*Scheduler, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if config.IssuanceRate <= 0 {
		return nil, fmt.Errorf("IssuanceRate must be greater than zero: got %f", config.IssuanceRate)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	// Defensive copy so the caller can't mutate config out from under
	// concurrent reads after construction.
	configCopy := *config
	config = &configCopy

	v4pds, v6pds, err := loadAllPDs(config)
	if err != nil {
		return nil, err
	}

	logger.Info("Scheduler loaded directives",
		slog.Int("v4_count", len(v4pds)),
		slog.String("v4_file", config.PDPathV4),
		slog.Int("v6_count", len(v6pds)),
		slog.String("v6_file", config.PDPathV6))

	pdMap := make(map[uint64]*pdState, config.ActiveSetSize)
	indices := make([]uint64, 0, config.ActiveSetSize)
	unusedByAgent := make(map[string][2][]*unusedPD)

	halfActive := config.ActiveSetSize / 2

	// When only one protocol is present, give all active set slots to that protocol.
	v4Active, v6Active := halfActive, halfActive
	if len(v4pds) == 0 {
		v6Active = config.ActiveSetSize
	}
	if len(v6pds) == 0 {
		v4Active = config.ActiveSetSize
	}

	seen := make(map[uint64]struct{}, len(v4pds)+len(v6pds))
	if err := loadPDsIntoPool(v4pds, v4Active, pdMap, &indices, unusedByAgent, seen); err != nil {
		return nil, fmt.Errorf("IPv4 PD file: %w", err)
	}
	if err := loadPDsIntoPool(v6pds, v6Active, pdMap, &indices, unusedByAgent, seen); err != nil {
		return nil, fmt.Errorf("IPv6 PD file: %w", err)
	}

	randomizer, err := newRandomizer(config.Seed, indices)
	if err != nil {
		return nil, fmt.Errorf("cannot create randomizer: %w", err)
	}

	totalPDs := len(v4pds) + len(v6pds)
	metrics.PDsTotal.Set(float64(totalPDs))
	metrics.PDsActiveTotal.Set(float64(len(pdMap)))
	for _, ipVer := range []wire.IPVersion{wire.IPVersion_IP_VERSION_IPV4, wire.IPVersion_IP_VERSION_IPV6} {
		total := 0
		for _, pools := range unusedByAgent {
			total += len(pools[ipIdx(ipVer)])
		}
		if total > 0 {
			metrics.PDsUnusedTotal.WithLabelValues(ipVersionLabel(ipVer)).Set(float64(total))
		}
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
// same agent and protocol. Returns nil if the active set is empty, the
// unused pool is exhausted, or ctx is canceled while waiting.
func (s *Scheduler) NextPD(ctx context.Context) *model.ProbingDirective {
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

	if !s.waitUntil(ctx, nextTime) {
		return nil
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

	// pd may have been replaced by a concurrent UpdateFromFIE call since
	// selection — re-check it's still active before replacing it again.
	s.mutex.Lock()
	current, stillActive := s.pdMap[pd.directive.ProbingDirectiveID]
	if !stillActive || current != pd {
		s.mutex.Unlock()
		return nil
	}
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

// waitUntil blocks until nextTime or ctx is canceled, whichever comes
// first. Returns false if ctx was canceled before nextTime was reached.
// Split out of NextPD to keep that function's cyclomatic complexity down —
// this logic is self-contained (only reads s.issuancePeriod) and doesn't
// need to be inlined.
func (s *Scheduler) waitUntil(ctx context.Context, nextTime time.Time) bool {
	if s.issuancePeriod >= 10*time.Millisecond {
		timer := time.NewTimer(time.Until(nextTime))
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
			return true
		}
	}

	// Busy-wait for sub-10ms periods: time.Sleep's effective resolution on
	// Linux (timer slack, scheduler granularity, C-states) can be several
	// milliseconds under load, which would distort high issuance rates.
	// Yield/sleep near the end instead of spinning the whole window —
	// same precision, less CPU pinned. Checks ctx each iteration so
	// cancellation can interrupt the wait rather than spinning past it.
	for {
		if ctx.Err() != nil {
			return false
		}
		remaining := time.Until(nextTime)
		if remaining <= 0 {
			return true
		}
		if remaining > 100*time.Microsecond {
			time.Sleep(remaining - 50*time.Microsecond)
		} else {
			runtime.Gosched()
		}
	}
}

// recycleOrEvict returns the PD to the unused pool or permanently evicts it
// if MaxEvictions has been reached. Must be called with s.mutex held.
func (s *Scheduler) recycleOrEvict(pd *pdState) {
	agentID := pd.directive.AgentID
	ipVersion := ipIdx(pd.directive.IPVersion)
	if pd.evictionCount < s.config.MaxEvictions {
		pools := s.unusedByAgent[agentID]
		pools[ipVersion] = append(pools[ipVersion], &unusedPD{
			evictionCount: pd.evictionCount + 1,
			directive:     pd.directive,
		})
		s.unusedByAgent[agentID] = pools
		s.metrics.PDsUnusedTotal.WithLabelValues(ipVersionLabel(pd.directive.IPVersion)).Inc()
	} else {
		s.metrics.PDsEvictedTotal.WithLabelValues(agentID).Inc()
		s.logger.Debug("PD permanently evicted",
			slog.Uint64("pd_id", pd.directive.ProbingDirectiveID),
			slog.String("agent_id", agentID),
			slog.String("ip_version", ipVersionLabel(pd.directive.IPVersion)))
	}
}

// replacePD replaces the given PD in the active set with a random draw from
// the unused pool for the same agent and protocol. Must be called with s.mutex held.
func (s *Scheduler) replacePD(pd *pdState) *pdState {
	// Clean up impact records
	s.removeImpact(pd.lastHitNearAddress, pd)
	s.removeImpact(pd.lastHitFarAddress, pd)

	agentID := pd.directive.AgentID
	ipVersion := ipIdx(pd.directive.IPVersion)

	// Remove from active set
	delete(s.pdMap, pd.directive.ProbingDirectiveID)
	s.metrics.PDsActiveTotal.Dec()

	// Draw replacement before returning pd to the unused pool — if pd were added
	// first, it could be randomly redrawn as its own replacement.
	pools := s.unusedByAgent[agentID]
	unused := pools[ipVersion]
	if len(unused) == 0 {
		// Active set permanently shrinks here — no fallback policy
		// (draw from another agent/protocol, reset and reinsert) is
		// implemented; left as a product decision, not guessed.
		s.logger.Warn("Unused pool exhausted for agent and protocol",
			slog.String("agent_id", agentID),
			slog.String("ip_version", ipVersionLabel(pd.directive.IPVersion)))
		s.recycleOrEvict(pd)
		return nil
	}

	drawIdx := s.random.IntN(len(unused))
	rawReplacement := unused[drawIdx]
	// Swap and shrink unused pool — O(1) removal without preserving order.
	unused[drawIdx] = unused[len(unused)-1]
	unused[len(unused)-1] = nil // avoid memory leak
	pools[ipVersion] = unused[:len(unused)-1]
	s.unusedByAgent[agentID] = pools
	s.metrics.PDsUnusedTotal.WithLabelValues(ipVersionLabel(pd.directive.IPVersion)).Dec()
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
func (s *Scheduler) UpdateFromFIE(fie *model.ForwardingInfoElement) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	pd, ok := s.pdMap[fie.ProbingDirectiveID]
	if !ok {
		// The PD may have been replaced since the probe was issued; stale FIEs are expected and ignored.
		s.logger.Debug("FIE for replaced PD ignored",
			slog.Uint64("pd_id", fie.ProbingDirectiveID))
		return nil
	}

	oldNearAddress, oldFarAddress := pd.lastHitNearAddress, pd.lastHitFarAddress

	// A missing near or far reply — whether due to timeout or packet loss — counts as a miss.
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
	if maxImpacts <= 1 {
		pd.issuanceProb = 1.0
	} else {
		// Actual active-set size, not the configured target — it's an
		// upper bound and can shrink (see replacePD).
		cycleDuration := float64(len(s.pdMap)) / s.config.IssuanceRate
		pd.issuanceProb = min(1.0, s.config.ImpactThreshold*cycleDuration/float64(maxImpacts))
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

// ipKey returns a normalized string key for a net.IP address. Returns ""
// for a nil address or one To16() can't normalize.
func ipKey(ip net.IP) string {
	if ip == nil {
		return ""
	}
	normalized := ip.To16()
	if normalized == nil {
		return ""
	}
	return normalized.String()
}

// readPDs reads a PD file and returns the parsed directives. Each line is
// a protojson-encoded wire.ProbingDirective — protojson is used rather
// than encoding/json because it understands oneofs (next_header) and
// accepts numeric enum values, both confirmed against a real file sample.
func readPDs(filepath string) ([]*model.ProbingDirective, error) {
	f, err := os.Open(filepath) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer func() {
		_ = f.Close()
	}()

	var results []*model.ProbingDirective
	scanner := bufio.NewScanner(f)
	// 4MiB max (default ~64KiB), in case a directive line is unusually large.
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue // skip blank or whitespace-only lines
		}

		var wirePD wire.ProbingDirective
		if err := protojson.Unmarshal(line, &wirePD); err != nil {
			return nil, fmt.Errorf("cannot unmarshal line %d: %w", lineNum, err)
		}

		pd, err := model.ProbingDirectiveFromProto(&wirePD)
		if err != nil {
			return nil, fmt.Errorf("invalid PD on line %d: %w", lineNum, err)
		}
		results = append(results, &pd)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scanner error: %w", err)
	}

	return results, nil
}

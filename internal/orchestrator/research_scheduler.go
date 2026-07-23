// Package orchestrator — ResearchScheduler implements the Retina Research
// Instance Scheduler as specified in DSD v1.2 (2026-06-26, frozen).
//
// Section references in comments (§x.y) refer to the DSD.
//
// Integration notes (adjust to the surrounding package before merging):
//   - The skeleton used `package main`; this file assumes it lives next to
//     the existing scheduler code in `package orchestrator`.
//   - AgentConnected / AgentDisconnected (§5.5) are NOT emitted here: the
//     Scheduler has no notion of agent liveness. They should be emitted on
//     the same bus by the component that owns agent state.
//   - The `Scheduler` interface below replaces the previous
//     NextPD/UpdateFromFIE interface (§5.1). Remove the old definition on
//     cutover.
package orchestrator

import (
	"container/heap"
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v1"
)

// ---------------------------------------------------------------------------
// Configuration (§6)
// ---------------------------------------------------------------------------

// ResearchSchedulerConfig holds the configuration parameters of §6, plus a
// few implementation-level knobs (seed, channel sizes). All parameters are
// fixed at startup.
type ResearchSchedulerConfig struct {
	// Seed for the internal RNG; kept explicit for reproducible experiments.
	Seed uint64

	// LearningRate is α, the multiplicative step of the period learning
	// rule (§3.4). Must be in (0, 1). Default: 0.3.
	LearningRate float64

	// SamplingWidth is β, the half-width of the uniform inter-issuance
	// sampling interval (§4.1). Must be in (0, 1). Default: 0.1.
	SamplingWidth float64

	// ImpactThreshold is Λ, the maximum permitted impact rate for any PD,
	// in impacts per second (§3.2, §4.2.1). Default: 1.0.
	ImpactThreshold float64

	// FIEHistoryCapacity is m, the number of FIEs retained per PD for the
	// staleness rule (§3.3, §4.2.2). Default: 6.
	FIEHistoryCapacity int

	// MinIssuancePeriod is μmin (§3.4). Default: 500ms.
	MinIssuancePeriod time.Duration

	// MaxIssuancePeriod is μmax (§3.4). Default: 12h.
	MaxIssuancePeriod time.Duration

	// AdmissionRate is r₀, the rate at which newly inserted PDs are
	// admitted into the schedule, in PDs per second (§4.3). Default: 1000.
	AdmissionRate float64

	// StartingIssuancePeriod is Μ, the period assigned at admission (§4.3).
	// Zero means "use MinIssuancePeriod" (the DSD default Μ = μmin).
	StartingIssuancePeriod time.Duration

	// StatusInterval is Tstatus, the CurrentStatus emission interval
	// (§5.5). Default: 1 minute.
	StatusInterval time.Duration

	// InsertChannelSize and FIEChannelSize size the internal channels
	// (§5.1). Insert and Update block when the corresponding channel is
	// full; this backpressure is accepted. Defaults: 1024 each.
	InsertChannelSize int
	UpdateChannelSize int

	// LatenessTolerance is the slack below which an issuance is not considered
	// late; it absorbs clock-read granularity around the busy-wait exit.
	LatenessTolerance time.Duration

	// BusyTolerance is Tbusy, governing the hybrid sleep strategy (§5.1.1).
	// Affects only timing precision, not scheduling semantics.
	// Default: 100micros.
	BusyTolerance time.Duration

	WaitTolerance time.Duration

	// InitialQueueSize is the default size of the queue variable.
	InitialQueueSize int

	// MaxUpdateDrainPerIssuance is the number of FIE updates we can do per
	// issuance call.
	MaxUpdateDrainPerIssuance int

	// MaxInsertDrainPerIssuance is the number of FIE updates we can do per
	// issuance call.
	MaxInsertDrainPerIssuance int

	// DefaultImpactDelay is the default delay estimated for a PD's issuance and
	// it's impact on the address.
	DefaultImpactDelay time.Duration
}

// initialize populates the zero value fields of ResearchSchedulerConfig with
// default values specified in the DSD section §6.
func (c *ResearchSchedulerConfig) initialize() { //nolint:gocyclo
	if c.LearningRate == 0 {
		c.LearningRate = 0.5
	}
	if c.SamplingWidth == 0 {
		c.SamplingWidth = 0.1
	}
	if c.ImpactThreshold == 0 {
		c.ImpactThreshold = 1.0
	}
	if c.FIEHistoryCapacity == 0 {
		c.FIEHistoryCapacity = 6
	}
	if c.MinIssuancePeriod == 0 {
		c.MinIssuancePeriod = 500 * time.Millisecond
	}
	if c.MaxIssuancePeriod == 0 {
		c.MaxIssuancePeriod = 12 * time.Hour
	}
	if c.AdmissionRate == 0 {
		c.AdmissionRate = 1000
	}
	if c.StartingIssuancePeriod == 0 {
		c.StartingIssuancePeriod = c.MinIssuancePeriod // Μ = μmin
	}
	if c.BusyTolerance == 0 {
		c.BusyTolerance = 500 * time.Microsecond
	}
	if c.StatusInterval == 0 {
		c.StatusInterval = time.Minute / 30
	}
	if c.InsertChannelSize == 0 {
		c.InsertChannelSize = 1024
	}
	if c.UpdateChannelSize == 0 {
		c.UpdateChannelSize = 1024
	}
	if c.LatenessTolerance == 0 {
		c.LatenessTolerance = time.Millisecond
	}
	if c.InitialQueueSize == 0 {
		c.InitialQueueSize = 100_000
	}
	if c.MaxUpdateDrainPerIssuance == 0 {
		c.MaxUpdateDrainPerIssuance = 5
	}
	if c.MaxInsertDrainPerIssuance == 0 {
		c.MaxInsertDrainPerIssuance = 5
	}
	if c.WaitTolerance == 0 {
		c.WaitTolerance = time.Millisecond
	}
	if c.DefaultImpactDelay == 0 {
		c.DefaultImpactDelay = time.Second
	}
}

// validate checks the configuration and returns an error describing the
// first invalid parameter found.
func (c *ResearchSchedulerConfig) validate() error { //nolint:gocyclo
	if c.LearningRate <= 0 || c.LearningRate >= 1 {
		return fmt.Errorf("learning rate (α) must be in (0, 1): %v", c.LearningRate)
	}
	if c.SamplingWidth <= 0 || c.SamplingWidth >= 1 {
		return fmt.Errorf("sampling width (β) must be in (0, 1): %v", c.SamplingWidth)
	}
	if c.ImpactThreshold <= 0 {
		return fmt.Errorf("impact threshold (Λ) must be positive: %v", c.ImpactThreshold)
	}
	if c.FIEHistoryCapacity < 2 {
		return fmt.Errorf("FIE history capacity (m) must be at least 2: %v", c.FIEHistoryCapacity)
	}
	if c.MinIssuancePeriod <= 0 {
		return fmt.Errorf("minimum issuance period (μmin) must be positive: %v", c.MinIssuancePeriod)
	}
	if c.MaxIssuancePeriod < c.MinIssuancePeriod {
		return fmt.Errorf("maximum issuance period (μmax = %v) cannot be smaller than μmin (%v)",
			c.MaxIssuancePeriod, c.MinIssuancePeriod)
	}
	if c.StartingIssuancePeriod < c.MinIssuancePeriod || c.StartingIssuancePeriod > c.MaxIssuancePeriod {
		return fmt.Errorf("starting issuance period (Μ = %v) must be within [μmin, μmax] = [%v, %v]",
			c.StartingIssuancePeriod, c.MinIssuancePeriod, c.MaxIssuancePeriod)
	}
	if c.AdmissionRate <= 0 {
		return fmt.Errorf("admission rate (r₀) must be positive: %v", c.AdmissionRate)
	}
	if c.BusyTolerance <= 0 {
		return fmt.Errorf("maximum busy-wait duration (Tbusy) must be positive: %v", c.BusyTolerance)
	}
	if c.StatusInterval <= 0 {
		return fmt.Errorf("status interval (Tstatus) must be positive: %v", c.StatusInterval)
	}
	if c.LatenessTolerance <= 0 {
		return fmt.Errorf("lateness tolerance must be positive: %v", c.LatenessTolerance)
	}
	if c.InitialQueueSize <= 0 {
		return fmt.Errorf("initial queue size must be positive: %v", c.InitialQueueSize)
	}
	if c.MaxUpdateDrainPerIssuance <= 0 {
		return fmt.Errorf("max update drain per issuance must be positive: %v", c.MaxUpdateDrainPerIssuance)
	}
	if c.MaxInsertDrainPerIssuance <= 0 {
		return fmt.Errorf("max insert drain per issuance must be positive: %v", c.MaxInsertDrainPerIssuance)
	}
	if c.DefaultImpactDelay <= time.Millisecond*200 { // this is a rule of thumb
		return fmt.Errorf("default impact delay must be greater than 200 ms: %v", c.DefaultImpactDelay)
	}
	return nil
}

// ---------------------------------------------------------------------------
// PD record and FIE history buffer (§5.4)
// ---------------------------------------------------------------------------

// addrKey is the canonical, comparable representation of an IP address used
// as a key in the address impact history (§5.3). Both IPv4 and IPv6
// addresses are stored in their 16-byte form, so the same address always
// maps to the same key regardless of which byte-length net.IP produced it.
type addrKey [16]byte

// toAddrKey converts addr to its canonical key. ok is false if addr is nil
// or not a valid IP (in which case it is treated as a null address).
func toAddrKey(addr net.IP) (key addrKey, ok bool) {
	b := addr.To16()
	if b == nil {
		return addrKey{}, false
	}
	copy(key[:], b)
	return key, true
}

// fieObservation stores the near and far addresses of one FIE — the only
// information the equivalence check requires (§5.4). A nil IP represents a
// null address, which is a legitimate observation (§4.2.2).
type fieObservation struct {
	near net.IP
	far  net.IP
}

// equivalent implements FIE equivalence (§4.2.2): near addresses equal and
// far addresses equal, with null treated as a value.
func (o fieObservation) equivalent(other fieObservation) bool {
	return ipEqual(o.near, other.near) && ipEqual(o.far, other.far)
}

// ipEqual compares two possibly-nil addresses; two nulls are equal, a null
// is unequal to any non-null address.
func ipEqual(a, b net.IP) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(b)
}

// pdRecord holds the scheduling state of a single PD.
type pdRecord struct {
	pdid uint64
	pd   *api.ProbingDirective

	// issuancePeriod is μᵢ in seconds (§3.1). It is the requested period;
	// the realized period μ̂ᵢ may deviate (§4.1, §7.2).
	issuancePeriod float64

	// nextIssuance is the scheduled issuance time; the heap is ordered by
	// this field.
	nextIssuance time.Time

	// lastIssuedAt is the time of the previous issuance; zero if the PD has
	// never been issued. Used to compute Bᵢ(t) (§3.2).
	lastIssuedAt time.Time

	// history is the FIE history 𝑭ᵢ as a fixed-capacity ring buffer of m
	// entries (§5.4), allocated once at admission.
	history   []fieObservation
	histWrite int
	histFill  int

	// lastNear/lastFar are the addresses impacted by the most recently
	// observed execution, as recorded in the address impact history (§5.3).
	// nil means null (no impact on that side).
	lastNear net.IP
	lastFar  net.IP

	// impactDelay is the last impact delay known in seconds.
	impactDelay float64
}

// appendFIE appends one observation to the ring buffer, evicting the oldest
// implicitly once full (§5.4).
func (r *pdRecord) appendFIE(o fieObservation) {
	r.history[r.histWrite] = o
	r.histWrite = (r.histWrite + 1) % len(r.history)
	if r.histFill < len(r.history) {
		r.histFill++
	}
}

// historyStable reports whether all m entries are pairwise equivalent.
// Equivalence is transitive, so comparing entries 2..m against entry 1
// suffices (§5.4). Must only be called when the history is full.
func (r *pdRecord) historyStable() bool {
	ref := r.history[0]
	for i := 1; i < len(r.history); i++ {
		if !r.history[i].equivalent(ref) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Priority queue (§5.2)
// ---------------------------------------------------------------------------

// pdHeap is a binary min-heap of PD records ordered by scheduled issuance
// time; the root is always the next due PD. Ties are broken arbitrarily.
type pdHeap []*pdRecord

func (h pdHeap) Len() int           { return len(h) }
func (h pdHeap) Less(i, j int) bool { return h[i].nextIssuance.Before(h[j].nextIssuance) }
func (h pdHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pdHeap) Push(x any)        { *h = append(*h, x.(*pdRecord)) }
func (h *pdHeap) Pop() any {
	old := *h
	n := len(old)
	rec := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return rec
}

// ---------------------------------------------------------------------------
// ResearchScheduler
// ---------------------------------------------------------------------------

// ResearchScheduler is the implementation of the Scheduler interface as
// specified in the Retina Research Instance Scheduler DSD v1.2.
//
// All state mutation happens inside Next, preserving the single-threaded
// execution model (§5.1). Insert and Update only push onto channels.
type ResearchScheduler struct {
	cfg    *ResearchSchedulerConfig
	logger *slog.Logger
	rand   *rand.Rand
	ebus   *EventBus

	// records maps PD identifier to its record; queue is the priority
	// queue of §5.2 over the same records.
	records map[uint64]*pdRecord
	queue   pdHeap

	// §4.2.1 revised: theoretical arrival time per address
	addressTAT map[addrKey]time.Time

	nextID atomic.Uint64

	// insertCh and fieCh are the internal channels of §5.1.
	insertCh     chan *api.ProbingDirective
	updateCh     chan *api.ForwardingInfoElement
	statusTicker *time.Ticker
	ctx          context.Context
	cancel       context.CancelFunc

	// bucketNext is the token bucket state for admission pacing (§5.6): the
	// earliest time the next admitted PD may be first-issued.
	bucketNext time.Time

	// Counters for CurrentStatus (§5.5).
	totalInsertions       uint64
	totalIssuances        uint64
	totalLate             uint64
	issuancesAtLastStatus uint64
	lastStatusEmission    time.Time
	windowMinPeriod       float64
	windowMaxPeriod       float64
	sumRate               float64
	pdsClampedAtMin       int
	pdsClampedAtMax       int
	pdsWithFullHistory    int
}

var _ Scheduler = (*ResearchScheduler)(nil)

// NewResearchScheduler constructs a scheduler with the given configuration.
// The scheduler starts empty (§4.3); PDs are admitted at runtime via Insert.
// Loading an initial PD set is the caller's responsibility: read the file
// and Insert in a loop — startup is just a burst of insertions.
func NewResearchScheduler(cfg *ResearchSchedulerConfig, logger *slog.Logger, ebus *EventBus) (*ResearchScheduler, error) {
	if logger == nil {
		logger = slog.Default()
	}
	cfg.initialize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	s := &ResearchScheduler{
		cfg:                cfg,
		logger:             logger,
		rand:               rand.New(rand.NewSource(int64(cfg.Seed))), //nolint:gosec // G404: not used for security
		records:            make(map[uint64]*pdRecord),
		queue:              make(pdHeap, 0, cfg.InitialQueueSize),
		addressTAT:         make(map[addrKey]time.Time),
		insertCh:           make(chan *api.ProbingDirective, cfg.InsertChannelSize),
		updateCh:           make(chan *api.ForwardingInfoElement, cfg.UpdateChannelSize),
		statusTicker:       time.NewTicker(cfg.StatusInterval),
		ctx:                ctx,
		cancel:             cancel,
		lastStatusEmission: time.Now(),
		ebus:               ebus,
		windowMinPeriod:    math.Inf(1),
		windowMaxPeriod:    math.Inf(-1),
	}

	s.logger.Info("Research scheduler initialized",
		slog.Float64("alpha", cfg.LearningRate),
		slog.Float64("beta", cfg.SamplingWidth),
		slog.Float64("lambda", cfg.ImpactThreshold),
		slog.Int("m", cfg.FIEHistoryCapacity),
		slog.Duration("mu_min", cfg.MinIssuancePeriod),
		slog.Duration("mu_max", cfg.MaxIssuancePeriod),
		slog.Float64("r0", cfg.AdmissionRate))
	return s, nil
}

// ---------------------------------------------------------------------------
// Public interface
// ---------------------------------------------------------------------------

func (s *ResearchScheduler) Insert(req *api.ProbingDirective) (uint64, error) {
	// ID starts from 0.
	id := s.nextID.Add(1) - 1
	req.ProbingDirectiveID = id

	select {
	case s.insertCh <- req:
		return id, nil
	case <-s.ctx.Done():
		return 0, s.ctx.Err()
	}
}

func (s *ResearchScheduler) Next() (*api.ProbingDirective, error) {
	// Drain the channels and ensure there are at least one element in the
	// queue.
	if err := s.drain(); err != nil {
		return nil, err
	}

	for {
		root := s.queue[0] // safe: drain guarantees non-empty on nil return
		remaining := time.Until(root.nextIssuance)

		// Far from due: interruptible wait for most of the gap, then loop to
		// re-drain and re-check (a newly admitted PD may now be sooner).
		if remaining > s.cfg.BusyTolerance {
			if err := s.wait(root.nextIssuance.Add(-s.cfg.BusyTolerance)); err != nil {
				return nil, err
			}

			// If a new PD is scheduled before our root, go back.
			if root != s.queue[0] {
				continue
			}
		}

		rec := heap.Pop(&s.queue).(*pdRecord)
		target := rec.nextIssuance // capture BEFORE compute reschedules it
		now := time.Now()

		if now.Sub(rec.nextIssuance) > s.cfg.LatenessTolerance {
			s.totalLate++
			s.ebus.Emit(&SchedulerLateEvent{
				ProbingDirectiveID: rec.pdid,
				ScheduledTime:      rec.nextIssuance,
				ActualTime:         now,
			})
		}

		s.compute(rec, now)
		heap.Push(&s.queue, rec)

		for time.Now().Before(target) {
		}

		return rec.pd, nil
	}
}

func (s *ResearchScheduler) Update(fie *api.ForwardingInfoElement) error {
	select {
	case s.updateCh <- fie:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *ResearchScheduler) Close() error {
	s.statusTicker.Stop()
	s.cancel()
	return nil
}

// ---------------------------------------------------------------------------
// Private interfaces
// ---------------------------------------------------------------------------

// drain applies pending inserts and FIEs, emits status, and honors shutdown
// before the scheduler picks the next PD to issue (§5.1). Returns nil only
// with a non-empty queue (so the caller can read s.queue[0]); returns an error
// only on shutdown.
func (s *ResearchScheduler) drain() error { //nolint:gocyclo
	// Per-issuance quotas: bound how many inserts and FIEs are applied per
	// call so neither can starve issuance. A spent quota drops its channel
	// from the select; unapplied items stay queued for the next pass.
	remInserts := s.cfg.MaxInsertDrainPerIssuance
	remUpdates := s.cfg.MaxUpdateDrainPerIssuance

	for {
		if s.queue.Len() == 0 {
			// Nothing to issue: block for work, ignoring quotas (there is no
			// issuance to starve). FIEs shouldn't occur here yet.
			select {
			case pd := <-s.insertCh:
				s.insert(pd)
			case fie := <-s.updateCh:
				s.update(fie)
			case <-s.statusTicker.C:
				s.emitStatus()
			case <-s.ctx.Done():
				return s.ctx.Err()
			}
			continue
		}

		// Non-empty queue: drain within quota. Build the select from whichever
		// channels still have budget.
		switch {
		case remInserts != 0 && remUpdates != 0:
			select {
			case pd := <-s.insertCh:
				s.insert(pd)
				remInserts--
			case fie := <-s.updateCh:
				s.update(fie)
				remUpdates--
			case <-s.statusTicker.C:
				s.emitStatus()
			case <-s.ctx.Done():
				return s.ctx.Err()
			default:
				return nil
			}
		case remInserts != 0: // FIE quota spent
			select {
			case pd := <-s.insertCh:
				s.insert(pd)
				remInserts--
			case <-s.statusTicker.C:
				s.emitStatus()
			case <-s.ctx.Done():
				return s.ctx.Err()
			default:
				return nil
			}
		case remUpdates != 0: // insert quota spent
			select {
			case fie := <-s.updateCh:
				s.update(fie)
				remUpdates--
			case <-s.statusTicker.C:
				s.emitStatus()
			case <-s.ctx.Done():
				return s.ctx.Err()
			default:
				return nil
			}
		default: // both spent
			return nil
		}
	}
}

// wait blocks until d, keeping the scheduler responsive to arrivals. Outside
// the WaitTolerance window it sleeps with time.After (interruptible by
// inserts, FIEs, and status ticks); within WaitTolerance it busy-waits, since
// time.After can over-sleep past d on a shared core and miss the issuance
// instant. Returns when d is reached, when an insert makes another PD the head
// (preemption), or on shutdown.
func (s *ResearchScheduler) wait(d time.Time) error { //nolint:gocyclo
	oldRoot := s.queue[0]
	tolerance := s.cfg.WaitTolerance // e.g. 1ms on GCP shared cores

	for {
		remaining := time.Until(d)

		// Within the tolerance window: busy-wait, no time.After. Still poll the
		// channels non-blockingly so arrivals are applied and preemption is
		// detected, but never sleep here.
		if remaining <= tolerance {
			for time.Now().Before(d) {
				select {
				case pd := <-s.insertCh:
					s.insert(pd)
					if s.queue[0] != oldRoot {
						return nil
					}
				case fie := <-s.updateCh:
					s.update(fie)
				case <-s.ctx.Done():
					return s.ctx.Err()
				default:
					// spin: nothing pending, keep checking the clock
				}
			}
			return nil // reached d, root still head
		}

		// Outside the tolerance window: sleep interruptibly until either an
		// arrival or (remaining - tolerance) elapses, then loop to re-decide.
		select {
		case pd := <-s.insertCh:
			s.insert(pd)
			if s.queue[0] != oldRoot {
				return nil
			}
			if time.Now().After(d) {
				return nil
			}
		case fie := <-s.updateCh:
			s.update(fie)
			if time.Now().After(d) {
				return nil
			}
		case <-s.statusTicker.C:
			s.emitStatus()
			if time.Now().After(d) {
				return nil
			}
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(remaining - tolerance):
			// woke ~tolerance before d; loop, next pass enters the busy-wait
			if time.Now().After(d) {
				return nil
			}
		}
	}
}

// insert admits one PD into the schedule. Called only from the scheduler
// goroutine during the channel drain. The PD's identifier was already
// assigned by the public Insert method; here we build its record, pace its
// first issuance via the token bucket, and push it onto the queue.
//
// Duplicate detection is unnecessary: identifiers are assigned by an atomic
// counter in Insert, so every admitted PD is unique by construction (this
// diverges from DSD §4.3's duplicate check / PDRejected event; see errata).
func (s *ResearchScheduler) insert(pd *api.ProbingDirective) {
	now := time.Now()

	// Token bucket with rate r₀ and capacity one token (§5.6): the first
	// issuance time is the later of now and the bucket's next-available
	// time, which then advances by 1/r₀.
	first := s.bucketNext
	if now.After(first) {
		first = now
	}
	s.bucketNext = first.Add(time.Duration(float64(time.Second) / s.cfg.AdmissionRate))

	rec := &pdRecord{
		pdid:           pd.ProbingDirectiveID,
		pd:             pd,
		issuancePeriod: s.cfg.StartingIssuancePeriod.Seconds(), // μᵢ = Μ (§4.3)
		nextIssuance:   first,
		history:        make([]fieObservation, s.cfg.FIEHistoryCapacity), // Fixed-capacity FIE ring buffer, allocated once at admission (§5.4).
		impactDelay:    s.cfg.DefaultImpactDelay.Seconds(),
	}
	s.records[pd.ProbingDirectiveID] = rec
	heap.Push(&s.queue, rec)
	s.totalInsertions++

	rec.issuancePeriod = s.cfg.StartingIssuancePeriod.Seconds()
	s.sumRate += 1 / rec.issuancePeriod
	if rec.issuancePeriod == s.cfg.MinIssuancePeriod.Seconds() {
		s.pdsClampedAtMin++
	}
	if rec.issuancePeriod == s.cfg.MaxIssuancePeriod.Seconds() {
		s.pdsClampedAtMax++
	}

	s.ebus.Emit(&PDInsertedEvent{
		ProbingDirectiveID: pd.ProbingDirectiveID,
		FirstIssuanceTime:  first,
		CurrentPDCount:     len(s.records),
	})
}

// update applies one FIE to its PD's record: the address impact history is
// updated first, then the observation is appended to the FIE history (§5.1).
// Called only from the scheduler goroutine during the channel drain.
//
// An FIE for an unknown PD id is ignored (no panic): stray or late FIEs can
// arrive around startup or cutover, and the drain's empty-queue branch relies
// on this being safe.
func (s *ResearchScheduler) update(fie *api.ForwardingInfoElement) {
	rec, ok := s.records[fie.ProbingDirectiveID]
	if !ok {
		return
	}

	var near, far net.IP
	if fie.NearInfo != nil {
		near = fie.NearInfo.ReplyAddress
	}
	if fie.FarInfo != nil {
		far = fie.FarInfo.ReplyAddress
	}
	rec.lastNear = near
	rec.lastFar = far

	wasFullBefore := rec.histFill == len(rec.history)
	rec.appendFIE(fieObservation{near: near, far: far})
	if !wasFullBefore && rec.histFill == len(rec.history) {
		s.pdsWithFullHistory++
	}

	// TODO: If we are getting the timestamps in seconds granularity, then this
	// would be 0. Then the impact deflay would be zero ???
	if fie.NearInfo != nil {
		mid := fie.NearInfo.SentTimestamp.Add(fie.NearInfo.ReceivedTimestamp.Sub(fie.NearInfo.SentTimestamp) / 2)
		rec.impactDelay = mid.Sub(rec.lastIssuedAt).Seconds()
	}
	if fie.FarInfo != nil {
		mid := fie.FarInfo.SentTimestamp.Add(fie.FarInfo.ReceivedTimestamp.Sub(fie.FarInfo.SentTimestamp) / 2)
		rec.impactDelay = mid.Sub(rec.lastIssuedAt).Seconds()
	}
}

// reserveAndFloor advances addr's theoretical arrival time (TAT) to account
// for this issuance's projected impact, landing at now+impactDelay, and
// returns the minimum period the NEXT issuance must respect to keep this
// address's combined impact rate at or below Λ (§4.2.1, revised: GCRA-style
// rate limiting per address, shared across every PD that touches it).
func (s *ResearchScheduler) reserveAndFloor(addr net.IP, now time.Time, impactDelay float64) float64 {
	key, ok := toAddrKey(addr)
	if !ok {
		return 0 // null/invalid address: no shared resource, no floor
	}

	landing := now.Add(secondsToDuration(impactDelay))
	tat := s.addressTAT[key] // zero value if never reserved: before `landing`
	if tat.Before(landing) {
		tat = landing
	}
	tat = tat.Add(secondsToDuration(1 / s.cfg.ImpactThreshold))
	s.addressTAT[key] = tat // reserve: this issuance's slot is consumed

	floor := tat.Sub(landing).Seconds()
	if floor < 0 {
		return 0
	}
	return floor
}

// compute applies the learning rules to the just-popped PD and reschedules
// it. It runs just before issuance, on the record being issued at time t.
//
// Rule order (§4.2): the responsible-probing floor is computed first, the
// staleness rule is applied, and the floor is enforced last so that the
// hard impact constraint holds at the end of the step regardless of what
// staleness wanted (this is the precedence fix; see errata on §4.2). A
// single step emits at most one PeriodAdjusted, attributed to the binding
// rule. Finally a new inter-issuance time is sampled and nextIssuance and
// lastIssuedAt are set.
func (s *ResearchScheduler) compute(rec *pdRecord, t time.Time) { //nolint:gocyclo
	fmt.Printf("rec.issuancePeriod: %v\n", rec.issuancePeriod)
	old := rec.issuancePeriod
	candidate := old
	rule := PeriodAdjustmentRuleNone

	// --- Responsible probing rpFloorPeriod (§4.2.1, revised) ---
	// Reserve this issuance's projected impact on both addresses (assumed
	// same as the previous issuance's, per §4.2.1) and take the tighter of
	// the two resulting floors for the next issuance.
	rpFloorPeriod := math.Max(
		s.reserveAndFloor(rec.lastNear, t, rec.impactDelay),
		s.reserveAndFloor(rec.lastFar, t, rec.impactDelay),
	)
	// rpFloor = 0.0 // disable RP

	// --- Staleness (§4.2.2) ---
	// Applied only once the FIE history is full; then every issuance either
	// slows down (stable) or speeds up (unstable).
	if rec.histFill == len(rec.history) {
		if rec.historyStable() {
			candidate = old * (1 + s.cfg.LearningRate)
			rule = PeriodAdjustmentRuleStalenessSlowDown
		} else {
			candidate = old / (1 + s.cfg.LearningRate)
			rule = PeriodAdjustmentRuleStalenessSpeedUp
		}
	}

	// --- Enforce the responsible-probing floor last (§4.2.1 precedence) ---
	if candidate < rpFloorPeriod {
		candidate = rpFloorPeriod
		rule = PeriodAdjustmentRuleResponsibleProbing
	}

	// --- Clamp to [μmin, μmax] (§3.4) ---
	if min := s.cfg.MinIssuancePeriod.Seconds(); candidate < min {
		candidate = min
		rule = PeriodAdjustmentRuleClamp
	}
	if max := s.cfg.MaxIssuancePeriod.Seconds(); candidate > max {
		candidate = max
		rule = PeriodAdjustmentRuleClamp
	}

	// Emit only on an actual change.
	if candidate != old {
		// Rate sum: decomposable, update by delta.
		s.sumRate += 1/candidate - 1/old

		// Clamp membership counts: independent per-PD, adjust in/out.
		minB, maxB := s.cfg.MinIssuancePeriod.Seconds(), s.cfg.MaxIssuancePeriod.Seconds()
		if old == minB {
			s.pdsClampedAtMin--
		}
		if old == maxB {
			s.pdsClampedAtMax--
		}
		if candidate == minB {
			s.pdsClampedAtMin++
		}
		if candidate == maxB {
			s.pdsClampedAtMax++
		}

		rec.issuancePeriod = candidate
		s.ebus.Emit(&PeriodAdjustedEvent{
			ProbingDirectiveID: rec.pdid,
			PreviousPeriod:     secondsToDuration(old),
			NewPeriod:          secondsToDuration(candidate),
			Rule:               rule,
		})
	}

	// --- Sample X and reschedule (§4.1) ---
	x := s.sampleInterIssuance(rec.issuancePeriod)
	rec.lastIssuedAt = t
	rec.nextIssuance = t.Add(x)
	s.totalIssuances++

	// after rec.issuancePeriod is finalized:
	if rec.issuancePeriod < s.windowMinPeriod {
		s.windowMinPeriod = rec.issuancePeriod
	}
	if rec.issuancePeriod > s.windowMaxPeriod {
		s.windowMaxPeriod = rec.issuancePeriod
	}
}

// sampleInterIssuance samples X ~ Uniform((1 − β)·μ, (1 + β)·μ) (§4.1). The
// bounded support guarantees a minimum spacing of (1 − β)·μ between issuances.
func (s *ResearchScheduler) sampleInterIssuance(periodSeconds float64) time.Duration {
	beta := s.cfg.SamplingWidth
	x := periodSeconds * (1 - beta + 2*beta*s.rand.Float64())
	return secondsToDuration(x)
}

// emitStatus computes and emits the CurrentStatus aggregate snapshot (§5.5).
func (s *ResearchScheduler) emitStatus() {
	now := time.Now()

	interval := now.Sub(s.lastStatusEmission).Seconds()
	var realized float64
	if interval > 0 {
		realized = float64(s.totalIssuances-s.issuancesAtLastStatus) / interval
	}

	var minP, maxP time.Duration
	if !math.IsInf(s.windowMinPeriod, 1) { // window saw at least one issuance
		minP = secondsToDuration(s.windowMinPeriod)
		maxP = secondsToDuration(s.windowMaxPeriod)
	}

	s.ebus.Emit(&CurrentStatusEvent{
		CurrentPDCount:            len(s.records),
		CumulativeInsertions:      s.totalInsertions,
		CumulativeIssuances:       s.totalIssuances,
		AggregateRequestedRate:    s.sumRate,
		AggregateRequestedPeriod:  1 / s.sumRate, // possible division by zero.
		RealizedRate:              realized,
		DistinctImpactedAddrs:     len(s.addressTAT),
		PeriodMin:                 minP.Seconds(),
		PeriodMax:                 maxP.Seconds(),
		PDsClampedAtMin:           s.pdsClampedAtMin,
		PDsClampedAtMax:           s.pdsClampedAtMax,
		PDsWithFullHistory:        s.pdsWithFullHistory,
		UpdateChannelOccupancy:    len(s.updateCh),
		InsertChannelOccupancy:    len(s.insertCh),
		CumulativeLateOccurrences: s.totalLate,
	})

	// reset window
	s.windowMinPeriod = math.Inf(1)
	s.windowMaxPeriod = math.Inf(-1)
	s.issuancesAtLastStatus = s.totalIssuances
	s.lastStatusEmission = now
}

func secondsToDuration(sec float64) time.Duration {
	return time.Duration(sec * float64(time.Second))
}

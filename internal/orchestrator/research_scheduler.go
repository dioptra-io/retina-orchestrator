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
	"fmt"
	"log/slog"
	"math/rand"
	"net"
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
	// rule (§3.4). Must be in (0, 1). Default: 0.1.
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

	// MaxBusyWait is Tbusy, governing the hybrid sleep strategy (§5.1.1).
	// Affects only timing precision, not scheduling semantics.
	// Default: 100ms.
	MaxBusyWait time.Duration

	// StatusInterval is Tstatus, the CurrentStatus emission interval
	// (§5.5). Default: 1 minute.
	StatusInterval time.Duration

	// InsertChannelSize and FIEChannelSize size the internal channels
	// (§5.1). Insert and Update block when the corresponding channel is
	// full; this backpressure is accepted. Defaults: 1024 each.
	InsertChannelSize int
	UpdateChannelSize int
}

// withDefaults returns a copy of the config with zero values replaced by the
// DSD §6 defaults.
func (c ResearchSchedulerConfig) withDefaults() ResearchSchedulerConfig {
	if c.LearningRate == 0 {
		c.LearningRate = 0.1
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
	if c.MaxBusyWait == 0 {
		c.MaxBusyWait = 100 * time.Millisecond
	}
	if c.StatusInterval == 0 {
		c.StatusInterval = time.Minute
	}
	if c.InsertChannelSize == 0 {
		c.InsertChannelSize = 1024
	}
	if c.UpdateChannelSize == 0 {
		c.UpdateChannelSize = 1024
	}
	return c
}

// validate checks the configuration and returns an error describing the
// first invalid parameter found.
func (c ResearchSchedulerConfig) validate() error {
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
	if c.MaxBusyWait <= 0 {
		return fmt.Errorf("maximum busy-wait duration (Tbusy) must be positive: %v", c.MaxBusyWait)
	}
	if c.StatusInterval <= 0 {
		return fmt.Errorf("status interval (Tstatus) must be positive: %v", c.StatusInterval)
	}
	return nil
}

// ---------------------------------------------------------------------------
// PD record and FIE history buffer (§5.4)
// ---------------------------------------------------------------------------

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
	histWrite int // write index
	histFill  int // fill count n

	// lastNear/lastFar are the addresses impacted by the most recently
	// observed execution, as recorded in the address impact history (§5.3).
	// nil means null (no impact on that side).
	lastNear net.IP
	lastFar  net.IP
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

// latenessTolerance is the slack below which an issuance is not considered
// late; it absorbs clock-read granularity around the busy-wait exit.
const latenessTolerance = time.Millisecond

// ResearchScheduler is the implementation of the Scheduler interface as
// specified in the Retina Research Instance Scheduler DSD v1.2.
//
// All state mutation happens inside Next, preserving the single-threaded
// execution model (§5.1). Insert and Update only push onto channels.
type ResearchScheduler struct {
	cfg    ResearchSchedulerConfig
	logger *slog.Logger
	rand   *rand.Rand
	ebus   *EventBus

	// records maps PD identifier to its record; queue is the priority
	// queue of §5.2 over the same records.
	records map[uint64]*pdRecord
	queue   pdHeap

	// impacts is the address impact history (§5.3): a map from IP address
	// (string form) to the set of PD identifiers that impacted that address
	// on their most recent issuance.
	impacts map[string]map[uint64]struct{}

	// insertCh and fieCh are the internal channels of §5.1.
	insertCh chan *api.ProbingDirective
	updateCh chan *api.ForwardingInfoElement

	// bucketNext is the token bucket state for admission pacing (§5.6): the
	// earliest time the next admitted PD may be first-issued.
	bucketNext time.Time

	statusTicker *time.Ticker

	// Counters for CurrentStatus (§5.5).
	totalInsertions       uint64
	totalIssuances        uint64
	totalLate             uint64
	issuancesAtLastStatus uint64
	lastStatusEmission    time.Time
}

var _ Scheduler = (*ResearchScheduler)(nil)

// NewResearchScheduler constructs a scheduler with the given configuration.
// The scheduler starts empty (§4.3); PDs are admitted at runtime via Insert.
// Loading an initial PD set is the caller's responsibility: read the file
// and Insert in a loop — startup is just a burst of insertions.
func NewResearchScheduler(cfg ResearchSchedulerConfig, logger *slog.Logger, ebus *EventBus) (*ResearchScheduler, error) {
	if logger == nil {
		logger = slog.Default()
	}
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	s := &ResearchScheduler{
		cfg:                cfg,
		logger:             logger,
		rand:               rand.New(rand.NewSource(int64(cfg.Seed))), //nolint:gosec // G404: not used for security
		records:            make(map[uint64]*pdRecord),
		queue:              make(pdHeap, 0),
		impacts:            make(map[string]map[uint64]struct{}),
		insertCh:           make(chan *api.ProbingDirective, cfg.InsertChannelSize),
		updateCh:           make(chan *api.ForwardingInfoElement, cfg.UpdateChannelSize),
		statusTicker:       time.NewTicker(cfg.StatusInterval),
		lastStatusEmission: time.Now(),
	}

	// s.emit(SSEEventSchedulerStarted, &SchedulerStartedData{Config: cfg})

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
	return 0, nil
}

func (s *ResearchScheduler) Next() (*api.ProbingDirective, error) {
	return nil, nil
}

func (s *ResearchScheduler) Update(fie *api.ForwardingInfoElement) error {
	return nil
}

func (s *ResearchScheduler) Close() error {
	s.statusTicker.Stop()
	return nil
}

// ---------------------------------------------------------------------------
// Private interface
// ---------------------------------------------------------------------------

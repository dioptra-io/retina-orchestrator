// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
)

// ---------------------------------------------------------------------------
// Event infrastructure
// ---------------------------------------------------------------------------

// RetinaBaseEvent is the base of every event. Embed it by value in each
// concrete event type. It is never emitted on its own.
type RetinaBaseEvent struct {
	// Type is the wire discriminator, set by Emit to the concrete Go type
	// name (e.g. "PeriodAdjusted"). Consumers switch on this field.
	Type string `json:"type"`
	// Timestamp is the emission time, set by Emit.
	Timestamp time.Time `json:"timestamp"`
}

// stamp fills the metadata common to every event. It is written once here and
// inherited by every embedder, so no concrete event type needs to implement
// it. Being unexported, it also seals the Event interface to this package:
// only types defined here (which embed RetinaEvent) can satisfy Event.
func (e *RetinaBaseEvent) stamp(typ string) {
	e.Type = typ
	e.Timestamp = time.Now()
}

// RetinaEvent is anything embedding RetinaEvent. The sole (unexported) method
// makes the interface unimplementable from outside this package, so the set of
// event types is closed and known to consumers.
type RetinaEvent interface {
	stamp(string)
}

// ---------------------------------------------------------------------------
// Section 5 events — Scheduler decisions (DSD §5.5)
// ---------------------------------------------------------------------------

// Every event embeds RetinaEvent and follows the naming convention of ending
// in "Event". Payloads follow §5.5; per the DSD's note, exact field names and
// types are not normative and the code is the source of truth.
//
// AgentConnected / AgentDisconnected (§5.5) are intentionally not defined
// here: the Scheduler has no notion of agent liveness, so those events belong
// to whichever component owns agent state and are emitted on the same bus.
// PDRejected (§4.3) is intentionally omitted: identifiers are assigned by an
// atomic counter in Insert, so duplicates are impossible and there is no
// rejection path (see errata).

// SchedulerStartedEvent is emitted once at initialization. Payload: the
// configuration parameters in effect (§5.5).
type SchedulerStartedEvent struct {
	RetinaEvent
	Config ResearchSchedulerConfig `json:"config"`
}

// PDInsertedEvent is emitted when an insertion is applied and the PD is
// admitted into the schedule (§4.3).
type PDInsertedEvent struct {
	RetinaEvent
	ProbingDirectiveID uint64    `json:"probing_directive_id"`
	FirstIssuanceTime  time.Time `json:"first_issuance_time"`
	CurrentPDCount     int       `json:"current_pd_count"`
}

// PeriodAdjustmentRule identifies which rule produced a period change (§4.2,
// §3.4). PeriodAdjustmentRuleNone is the zero value, used internally when no
// rule changed the period (no event is emitted in that case).
type PeriodAdjustmentRule string

const (
	PeriodAdjustmentRuleNone               PeriodAdjustmentRule = ""
	PeriodAdjustmentRuleStalenessSlowDown  PeriodAdjustmentRule = "staleness_slow_down" // §4.2.2
	PeriodAdjustmentRuleStalenessSpeedUp   PeriodAdjustmentRule = "staleness_speed_up"  // §4.2.2
	PeriodAdjustmentRuleResponsibleProbing PeriodAdjustmentRule = "responsible_probing" // §4.2.1
	PeriodAdjustmentRuleClamp              PeriodAdjustmentRule = "clamp"               // §3.4
)

// PeriodAdjustedEvent is emitted on every change to a PD's issuance period,
// from any rule: staleness slow-down or speed-up (§4.2.2), the responsible
// probing floor (§4.2.1), or the μ_min/μ_max clamp (§3.4). A single learning
// step emits at most one such event, attributed to the binding rule.
type PeriodAdjustedEvent struct {
	RetinaEvent
	ProbingDirectiveID uint64               `json:"probing_directive_id"`
	PreviousPeriod     time.Duration        `json:"previous_period"`
	NewPeriod          time.Duration        `json:"new_period"`
	Rule               PeriodAdjustmentRule `json:"rule"`
}

// SchedulerLateEvent is emitted when an overdue PD is issued past its
// scheduled time (§4.1). The Scheduler does not recover the schedule; overdue
// PDs are issued immediately in queue order and this event surfaces the
// condition.
type SchedulerLateEvent struct {
	RetinaEvent
	ProbingDirectiveID uint64    `json:"probing_directive_id"`
	ScheduledTime      time.Time `json:"scheduled_time"`
	ActualTime         time.Time `json:"actual_time"`
}

// CurrentStatusEvent is the periodic aggregate snapshot emitted every
// Tstatus (§5.5), for monitoring and coarse-grained analysis without
// reconstructing state from the per-PD event stream.
type CurrentStatusEvent struct {
	RetinaEvent
	CurrentPDCount            int           `json:"current_pd_count"`
	CumulativeInsertions      uint64        `json:"cumulative_insertions"`
	CumulativeIssuances       uint64        `json:"cumulative_issuances"`
	AggregateRequestedRate    float64       `json:"aggregate_requested_rate"` // Σ rᵢ = Σ 1/μᵢ, per second
	RealizedRate              float64       `json:"realized_rate"`            // issuances over the last interval, per second
	DistinctImpactedAddrs     int           `json:"distinct_impacted_addrs"`
	PeriodMin                 time.Duration `json:"period_min"`
	PeriodMax                 time.Duration `json:"period_max"`
	PeriodMean                time.Duration `json:"period_mean"`
	PDsClampedAtMin           int           `json:"pds_clamped_at_min"`
	PDsClampedAtMax           int           `json:"pds_clamped_at_max"`
	PDsWithFullHistory        int           `json:"pds_with_full_history"`
	UpdateChannelOccupancy    int           `json:"update_channel_occupancy"`
	InsertChannelOccupancy    int           `json:"insert_channel_occupancy"`
	CumulativeLateOccurrences uint64        `json:"cumulative_late_occurrences"`
}

// ---------------------------------------------------------------------------
// Event bus
// ---------------------------------------------------------------------------

type envelope struct{ RetinaEvent }

// EventBus is the emitter through which the Scheduler exposes its decisions
// (§5.5). It wraps the same ring-buffer mechanism used for client FIE
// streaming (outside this document's scope). Emission is non-blocking: a slow
// or absent subscriber is lapped rather than stalling the scheduling loop.
type EventBus struct {
	ring *structures.RingBuffer[envelope]
}

// Emit stamps the event with its type name and emission time, then publishes
// it to the ring. It is non-blocking with best-effort delivery.
//
// The event is passed as &e (a *Event) because the underlying RingBuffer
// stores *T and uses nil for empty slots; here T is the Event interface, so
// the stored element type is *Event. &e is the address of Emit's own
// parameter, distinct per call, so no aliasing occurs across emissions.
func (b *EventBus) Emit(e RetinaEvent) {
	e.stamp(typeName(e))
	b.ring.Push(&envelope{e})
}

// NewConsumer creates a new consumer for the ringbuffer. It has it's own
// methods for synchronization etc.
func (b *EventBus) NewConsumer() *structures.RingConsumer[envelope] {
	return b.ring.NewConsumer()
}

// eventTypeNames caches the reflect.Type -> wire-name mapping so the
// reflection lookup in typeName runs at most once per concrete event type.
var eventTypeNames sync.Map // reflect.Type -> string

// typeName returns the wire discriminator for an event: its concrete Go type
// name, verbatim (e.g. *PeriodAdjusted -> "PeriodAdjusted"). Because the name
// is derived from the type, it cannot drift out of sync with the struct, but
// renaming a struct renames its wire event accordingly.
func typeName(e any) string {
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if v, ok := eventTypeNames.Load(t); ok {
		return v.(string)
	}
	name := t.Name()
	eventTypeNames.Store(t, name)
	return name
}

// NewEventBus creates an EventBus backed by a ring buffer of the given
// capacity. Capacity bounds how far a slow subscriber may fall behind before
// it is lapped and starts missing events (§5.5); it must be positive.
func NewEventBus(capacity int) (*EventBus, error) {
	ring, err := structures.NewRingBuffer[envelope](capacity)
	if err != nil {
		return nil, fmt.Errorf("cannot create event bus: %w", err)
	}
	return &EventBus{ring: ring}, nil
}

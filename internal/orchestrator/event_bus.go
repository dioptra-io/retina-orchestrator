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

// PeriodAdjustedEvent is emitted on every change to a PD's issuance period, from
// any rule: the staleness slow-down or speed-up (§4.2.2), the responsible
// probing clamp (§4.2.1), or the μ_min/μ_max clamp (§3.4). A single learning
// step emits at most one PeriodAdjustedEvent, attributed to the rule that
// determined the final period.
type PeriodAdjustedEvent struct {
	RetinaBaseEvent
	ProbingDirectiveID uint64        `json:"probing_directive_id"`
	PreviousPeriod     time.Duration `json:"previous_period"`
	NewPeriod          time.Duration `json:"new_period"`
	Rule               string        `json:"rule"`
}

// SchedulerLateEvent is emitted when an overdue PD is issued past its scheduled
// time (§4.1). The Scheduler does not attempt to recover the schedule;
// overdue PDs are issued immediately in queue order, and this event is the
// safety valve that surfaces the condition.
type SchedulerLateEvent struct {
	RetinaBaseEvent
	ProbingDirectiveID uint64    `json:"probing_directive_id"`
	ScheduledTime      time.Time `json:"scheduled_time"`
	ActualTime         time.Time `json:"actual_time"`
}

type CurrentStatusEvent struct {
	RetinaBaseEvent
	// TODO
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

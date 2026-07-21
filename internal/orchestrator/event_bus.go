// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"reflect"
	"sync"
	"time"

	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
)

// RetinaEvent is the base of every event. Embed it by value in each
// concrete event type.
type RetinaEvent struct {
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

// stamp fills the metadata. Written once, inherited by every embedder;
// being unexported, it also seals Event to this package.
func (e *RetinaEvent) stamp(typ string) {
	e.Type = typ
	e.Timestamp = time.Now()
}

// Event is anything embedding RetinaEvent.
type Event interface{ stamp(string) }

type PeriodAdjusted struct {
	RetinaEvent
	ProbingDirectiveID uint64        `json:"probing_directive_id"`
	PreviousPeriod     time.Duration `json:"previous_period"`
	NewPeriod          time.Duration `json:"new_period"`
	Rule               string        `json:"rule"`
}

type SchedulerLate struct {
	RetinaEvent
	ProbingDirectiveID uint64    `json:"probing_directive_id"`
	ScheduledTime      time.Time `json:"scheduled_time"`
	ActualTime         time.Time `json:"actual_time"`
}

type EventBus struct {
	ring *structures.RingBuffer[Event]
}

func (b *EventBus) Push(e Event) {
	e.stamp(typeName(e))
	// b.ring.Push(e)
}

var typeNames sync.Map // reflect.Type -> string

func typeName(e any) string {
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if v, ok := typeNames.Load(t); ok {
		return v.(string)
	}
	name := t.Name()
	typeNames.Store(t, name)
	return name
}

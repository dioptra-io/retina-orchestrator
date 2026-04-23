// Copyright (c) 2026 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestNewMetrics_NilRegistry(t *testing.T) {
	t.Parallel()
	m := NewMetrics(nil)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
}

func TestNewMetrics_WithRegistry(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}

	// NOTE: If a new metric is added to Metrics, add a corresponding nil check here.
	if m.AgentsConnected == nil {
		t.Error("expected AgentsConnected to be non-nil")
	}
	if m.AuthFailuresTotal == nil {
		t.Error("expected AuthFailuresTotal to be non-nil")
	}
	if m.AgentDisconnectionsTotal == nil {
		t.Error("expected AgentDisconnectionsTotal to be non-nil")
	}
	if m.PDsSentTotal == nil {
		t.Error("expected PDsSentTotal to be non-nil")
	}
	if m.FIEsReceivedTotal == nil {
		t.Error("expected FIEsReceivedTotal to be non-nil")
	}
	if m.AgentQueueSize == nil {
		t.Error("expected AgentQueueSize to be non-nil")
	}
	if m.PDsTotal == nil {
		t.Error("expected PDsTotal to be non-nil")
	}
	if m.CycleDurationSeconds == nil {
		t.Error("expected CycleDurationSeconds to be non-nil")
	}
	if m.CyclesTotal == nil {
		t.Error("expected CyclesTotal to be non-nil")
	}
	if m.PDsSkippedTotal == nil {
		t.Error("expected PDsSkippedTotal to be non-nil")
	}
	if m.StreamClientsConnected == nil {
		t.Error("expected StreamClientsConnected to be non-nil")
	}
	if m.StreamConnectionsTotal == nil {
		t.Error("expected StreamConnectionsTotal to be non-nil")
	}
	if m.StreamDisconnectionsTotal == nil {
		t.Error("expected StreamDisconnectionsTotal to be non-nil")
	}
	if m.FIEsStreamedTotal == nil {
		t.Error("expected FIEsStreamedTotal to be non-nil")
	}
	if m.StreamLagSeconds == nil {
		t.Error("expected StreamLagSeconds to be non-nil")
	}
}

func TestNewMetrics_DefaultRegistry(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: registers global metrics")
	}
	m := NewMetrics(prometheus.DefaultRegisterer)
	if m == nil {
		t.Fatal("expected non-nil Metrics")
	}
}

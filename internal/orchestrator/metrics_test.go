// Copyright (c) 2026 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestMetrics_CounterBehavior(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.AuthFailuresTotal.Inc()
	if testutil.ToFloat64(m.AuthFailuresTotal) != 1 {
		t.Error("expected AuthFailuresTotal to be 1")
	}

	m.CyclesTotal.Inc()
	if testutil.ToFloat64(m.CyclesTotal) != 1 {
		t.Error("expected CyclesTotal to be 1")
	}

	m.PDsSkippedTotal.Inc()
	if testutil.ToFloat64(m.PDsSkippedTotal) != 1 {
		t.Error("expected PDsSkippedTotal to be 1")
	}

	m.StreamConnectionsTotal.Inc()
	if testutil.ToFloat64(m.StreamConnectionsTotal) != 1 {
		t.Error("expected StreamConnectionsTotal to be 1")
	}

	m.FIEsStreamedTotal.Inc()
	if testutil.ToFloat64(m.FIEsStreamedTotal) != 1 {
		t.Error("expected FIEsStreamedTotal to be 1")
	}
}

func TestMetrics_GaugeBehavior(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.AgentsConnected.Set(2)
	if testutil.ToFloat64(m.AgentsConnected) != 2 {
		t.Error("expected AgentsConnected to be 2")
	}

	m.StreamClientsConnected.Set(3)
	if testutil.ToFloat64(m.StreamClientsConnected) != 3 {
		t.Error("expected StreamClientsConnected to be 3")
	}

	m.PDsTotal.Set(100)
	if testutil.ToFloat64(m.PDsTotal) != 100 {
		t.Error("expected PDsTotal to be 100")
	}
}

func TestMetrics_VecCounters(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.AgentDisconnectionsTotal.WithLabelValues("agent-1").Inc()
	m.PDsSentTotal.WithLabelValues("agent-1").Inc()
	m.FIEsReceivedTotal.WithLabelValues("agent-1").Inc()
	m.AgentQueueSize.WithLabelValues("agent-1").Set(5)

	if testutil.ToFloat64(m.AgentDisconnectionsTotal.WithLabelValues("agent-1")) != 1 {
		t.Error("expected AgentDisconnectionsTotal to be 1")
	}
	if testutil.ToFloat64(m.PDsSentTotal.WithLabelValues("agent-1")) != 1 {
		t.Error("expected PDsSentTotal to be 1")
	}
	if testutil.ToFloat64(m.FIEsReceivedTotal.WithLabelValues("agent-1")) != 1 {
		t.Error("expected FIEsReceivedTotal to be 1")
	}
	if testutil.ToFloat64(m.AgentQueueSize.WithLabelValues("agent-1")) != 5 {
		t.Error("expected AgentQueueSize to be 5")
	}
}

func TestMetrics_StreamDisconnections(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.StreamDisconnectionsTotal.WithLabelValues("shutdown_or_disconnect").Inc()
	m.StreamDisconnectionsTotal.WithLabelValues("internal_error").Inc()

	if testutil.ToFloat64(m.StreamDisconnectionsTotal.WithLabelValues("shutdown_or_disconnect")) != 1 {
		t.Error("expected shutdown_or_disconnect to be 1")
	}
	if testutil.ToFloat64(m.StreamDisconnectionsTotal.WithLabelValues("internal_error")) != 1 {
		t.Error("expected internal_error to be 1")
	}
}

func TestMetrics_Histogram(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)

	m.CycleDurationSeconds.Observe(0.5)
	m.StreamLagSeconds.Observe(0.1)
}

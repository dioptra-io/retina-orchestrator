// Copyright (c) 2026 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the orchestrator.
// It is created once and passed to the orchestrator components via constructors.
type Metrics struct {
	// Agent connections
	AgentsConnected          prometheus.Gauge
	AuthFailuresTotal        prometheus.Counter
	AgentDisconnectionsTotal *prometheus.CounterVec
	PDsSentTotal             *prometheus.CounterVec
	FIEsReceivedTotal        *prometheus.CounterVec
	AgentQueueSize           *prometheus.GaugeVec

	// PD cycling
	PDsTotal             prometheus.Gauge
	CycleDurationSeconds prometheus.Histogram
	CyclesTotal          prometheus.Counter

	// PD replacement — labeled by agent_id
	PDsReplacedBernoulliTotal *prometheus.CounterVec
	PDsReplacedMissTotal      *prometheus.CounterVec
	PDsEvictedTotal           *prometheus.CounterVec
	PDsUnusedTotal            *prometheus.GaugeVec
	// PDsActiveTotal is not per-agent since the active set is shared across agents.
	PDsActiveTotal prometheus.Gauge

	// apiClient (push connection to retina-api)
	APIClientFIEsPushedTotal  prometheus.Counter
	APIClientFIEsDroppedTotal prometheus.Counter
	APIClientConnectionUp     prometheus.Gauge
}

// NewMetrics creates and registers all orchestrator metrics with the given registry.
//
//nolint:funlen // metric registration is necessarily verbose
func NewMetrics(registry prometheus.Registerer) *Metrics {
	factory := promauto.With(registry)

	return &Metrics{
		// Agent connections
		AgentsConnected: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_orchestrator_agents_connected",
			Help: "Number of currently connected agents.",
		}),
		AuthFailuresTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_auth_failures_total",
			Help: "Total number of rejected authentication attempts.",
		}),
		AgentDisconnectionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_agent_disconnections_total",
			Help: "Total number of agent disconnections, labeled by agent ID.",
		}, []string{"agent_id"}),
		PDsSentTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_pds_sent_total",
			Help: "Total number of probing directives dispatched, labeled by agent ID.",
		}, []string{"agent_id"}),
		FIEsReceivedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_fies_received_total",
			Help: "Total number of forwarding info elements received, labeled by agent ID.",
		}, []string{"agent_id"}),
		AgentQueueSize: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "retina_orchestrator_agent_queue_size",
			Help: "Current number of probing directives queued for the agent.",
		}, []string{"agent_id"}),

		// PD cycling
		PDsTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_orchestrator_pds_total",
			Help: "Total number of probing directives loaded at startup.",
		}),
		CycleDurationSeconds: factory.NewHistogram(prometheus.HistogramOpts{ // TODO: tune buckets once we have real cycle duration data.
			Name:    "retina_orchestrator_cycle_duration_seconds",
			Help:    "Duration of a full PD cycle in seconds.",
			Buckets: prometheus.DefBuckets,
		}),
		CyclesTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_cycles_total",
			Help: "Total number of completed PD cycles.",
		}),

		// PD replacement
		PDsReplacedBernoulliTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_pds_replaced_bernoulli_total",
			Help: "Total number of probing directives replaced due to failed Bernoulli experiment (responsible probing), labeled by agent ID.",
		}, []string{"agent_id"}),
		PDsReplacedMissTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_pds_replaced_miss_total",
			Help: "Total number of probing directives replaced due to consecutive misses threshold, labeled by agent ID.",
		}, []string{"agent_id"}),
		PDsEvictedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "retina_orchestrator_pds_evicted_total",
			Help: "Total number of probing directives permanently evicted from the pool, labeled by agent ID.",
		}, []string{"agent_id"}),
		PDsUnusedTotal: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "retina_orchestrator_pds_unused_total",
			Help: "Current number of probing directives in the unused pool, labeled by IP version (4 or 6).",
		}, []string{"ip_version"}),
		PDsActiveTotal: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_orchestrator_pds_active_total",
			Help: "Current number of probing directives in the active set.",
		}),

		// Streaming to retina-api
		APIClientFIEsPushedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_api_client_fies_pushed_total",
			Help: "Total number of FIEs pushed to retina-api.",
		}),
		APIClientFIEsDroppedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "retina_orchestrator_api_client_fies_dropped_total",
			Help: "Total number of FIEs dropped because the outbound buffer to retina-api was full.",
		}),
		APIClientConnectionUp: factory.NewGauge(prometheus.GaugeOpts{
			Name: "retina_orchestrator_api_client_connection_up",
			Help: "1 if the push connection to retina-api is currently established, 0 otherwise.",
		}),
	}
}

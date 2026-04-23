// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator"
)

const defaultAgentBufferLength = 8192

func main() {
	if err := run(); err != nil {
		slog.Error("Orchestrator error", "err", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		apiAddr              = flag.String("api-addr", envOrDefault("RETINA_API_ADDR", "localhost:8080"), "Listening address for the HTTP API server")
		agentAddr            = flag.String("agent-addr", envOrDefault("RETINA_AGENT_ADDR", "localhost:50050"), "Listening address for agent connections")
		pdQueueSize          = flag.Int("pd-queue-size", envOrDefaultInt("RETINA_PD_QUEUE_SIZE", 100), "The size of the agent queue")
		pdPath               = flag.String("pd-path", envOrDefault("RETINA_PD_PATH", ""), "Path to the probing directives file")
		issuanceRate         = flag.Float64("issuance-rate", envOrDefaultFloat64("RETINA_ISSUANCE_RATE", 1.0), "Target global issuance rate of probing directives (PDs per second, approximate)")
		impactThreshold      = flag.Float64("impact-threshold", envOrDefaultFloat64("RETINA_IMPACT_THRESHOLD", 1.0), "Maximum impact threshold per address for the responsible probing algorithm")
		seed                 = flag.Uint64("seed", envOrDefaultUInt64("RETINA_SEED", 42), "Seed for the randomizer")
		apiReadHeaderTimeout = flag.Duration("api-read-header-timeout", envOrDefaultDuration("RETINA_API_READ_HEADER_TIMEOUT", 5*time.Second), "Timeout for reading HTTP request headers")
		logLevel             = flag.String("log-level", envOrDefault("RETINA_LOG_LEVEL", "info"), "Log level (debug, info, warn, error)")
		metricsAddr          = flag.String("metrics-addr", envOrDefault("RETINA_METRICS_ADDR", ":9312"), "Address to expose Prometheus metrics on")
	)
	flag.Parse()

	logger := newLogger(*logLevel)

	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	metrics := orchestrator.NewMetrics(registry)
	metricsSrv, err := startMetricsServer(logger, registry, *metricsAddr)
	if err != nil {
		return err
	}

	secret := os.Getenv("RETINA_SECRET")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	orch, err := orchestrator.NewOrch(&orchestrator.Config{
		AgentAddress:         *agentAddr,
		PDQueueSize:          *pdQueueSize,
		RingBufferSize:       *ringBufferSize,
		AgentBufferLength:    defaultAgentBufferLength,
		APIAddress:           *apiAddr,
		APIReadHeaderTimeout: *apiReadHeaderTimeout,
		PDPath:               *pdPath,
		IssuanceRate:         *issuanceRate,
		Seed:                 *seed,
		ImpactThreshold:      *impactThreshold,
		FIEFilterPolicy:      *fieFilterPolicy,
		Secret:               secret,
	}, logger, metrics)
	if err != nil {
		return err
	}

	logger.Info("Starting orchestrator",
		"api_addr", *apiAddr,
		"agent_addr", *agentAddr,
		"pd_path", *pdPath,
		"issuance_rate", *issuanceRate,
		"impact_threshold", *impactThreshold,
		"log_level", *logLevel,
		"metrics_addr", *metricsAddr,
	)

	if err := orch.Run(ctx); !errors.Is(err, ctx.Err()) {
		return err
	}

	shutdown(logger, metricsSrv)
	return nil
}

// startMetricsServer starts an HTTP server exposing Prometheus metrics at /metrics.
// It binds eagerly so that a port conflict is detected before the orchestrator starts.
func startMetricsServer(logger *slog.Logger, registry *prometheus.Registry, addr string) (*http.Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics server: %w", err)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	//nolint:gosec // G112: metrics endpoint is internal-only; timeout omitted intentionally
	srv := &http.Server{Handler: mux}

	go func() {
		logger.Info("Starting metrics server", slog.String("addr", addr))
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("Metrics server failed", slog.Any("err", err))
		}
	}()

	return srv, nil
}

// newLogger creates a JSON logger writing to stdout at the given level.
// Falls back to info if the level string is unrecognised.
func newLogger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: l,
	}))
}

func shutdown(logger *slog.Logger, metricsSrv *http.Server) {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Metrics server shutdown failed", slog.Any("err", err))
	}
	logger.Info("Shutting down orchestrator")
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrDefaultUInt64(key string, def uint64) uint64 {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return i
	}
	return def
}

func envOrDefaultInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return int(i)
	}
	return def
}

func envOrDefaultFloat64(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.ParseFloat(v, 64)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return i
	}
	return def
}

func envOrDefaultDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			slog.Error("Invalid environment variable", slog.String("key", key), slog.String("value", v)) //nolint:gosec // G706: value is from env var, rejected as invalid, slog.String sanitizes output
			os.Exit(1)
		}
		return d
	}
	return def
}

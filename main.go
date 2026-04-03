// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

// Command retina-orchestrator runs the Retina orchestrator, which schedules
// Probing Directives (PDs) to connected agents and streams the resulting
// ForwardingInfoElements (FIEs) to clients.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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
		apiAddr              = flag.String("api-addr", "localhost:8080", "Listening address for the HTTP API server")
		agentAddr            = flag.String("agent-addr", "localhost:50050", "Listening address for agent connections")
		pdPath               = flag.String("pd-path", "", "Path to the probing directives file")
		issuanceRate         = flag.Float64("issuance-rate", 1.0, "Target global issuance rate of probing directives (PDs per second, approximate)")
		impactThreshold      = flag.Float64("impact-threshold", 1.0, "Maximum impact threshold per address for the responsible probing algorithm")
		seed                 = flag.Uint64("seed", 42, "Seed for the randomizer")
		apiReadHeaderTimeout = flag.Duration("api-read-header-timeout", 5*time.Second, "Timeout for reading HTTP request headers")
		logLevel             = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
	)
	flag.Parse()

	var level slog.Level
	invalidLevel := level.UnmarshalText([]byte(*logLevel)) != nil
	if invalidLevel {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	if invalidLevel {
		logger.Warn("Unknown log level, defaulting to info", slog.String("log_level", *logLevel))
	}

	secret := os.Getenv("RETINA_SECRET")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	orch, err := orchestrator.NewOrch(&orchestrator.Config{
		AgentAddress:         *agentAddr,
		AgentBufferLength:    defaultAgentBufferLength,
		APIAddress:           *apiAddr,
		APIReadHeaderTimeout: *apiReadHeaderTimeout,
		PDPath:               *pdPath,
		IssuanceRate:         *issuanceRate,
		Seed:                 *seed,
		ImpactThreshold:      *impactThreshold,
		Secret:               secret,
		Logger:               logger,
	})

	if err != nil {
		return err
	}
	logger.Info("Starting orchestrator",
		"api_addr", *apiAddr,
		"agent_addr", *agentAddr,
		"pd_path", *pdPath,
		"issuance_rate", *issuanceRate,
		"impact_threshold", *impactThreshold,
		"log_level", level,
	)

	if err := orch.Run(ctx); !errors.Is(err, ctx.Err()) {
		return err
	}

	logger.Info("Shutting down orchestrator")
	return nil
}

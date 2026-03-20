// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
//
// Command retina-orchestrator runs the Retina orchestrator, which schedules
// Probing Directives (PDs) to connected agents and streams the resulting
// ForwardingInfoElements (FIEs) to clients.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/dioptra-io/retina-orchestrator/internal/retina"
)

const defaultAgentBufferLength = 8192

func main() {
	if err := run(); err != nil {
		log.Fatalf("%v", err)
	}
}

func run() error {
	var (
		apiAddr         = flag.String("api-addr", "localhost:8080", "Listening address for the HTTP API server")
		agentAddr       = flag.String("agent-addr", "localhost:50050", "Listening address for agent connections")
		pdPath          = flag.String("pd-path", "", "Path to the probing directives file")
		issuanceRate    = flag.Float64("issuance-rate", 1.0, "Target global issuance rate of probing directives (PDs per second, approximate)")
		impactThreshold = flag.Float64("impact-threshold", 1.0, "Maximum impact threshold per address for the responsible probing algorithm")
		seed            = flag.Uint64("seed", 42, "Seed for the randomizer")
		secret          = flag.String("secret", "", "Shared secret for agent authentication")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	orch, err := retina.NewOrch(&retina.Config{
		AgentAddress:      *agentAddr,
		AgentBufferLength: defaultAgentBufferLength,
		AgentTimeout:      time.Hour,
		APIAddress:        *apiAddr,
		APITimeout:        time.Hour,
		PDPath:            *pdPath,
		IssuanceRate:      *issuanceRate,
		Seed:              *seed,
		ImpactThreshold:   *impactThreshold,
		Secret:            *secret,
	})
	if err != nil {
		return err
	}

	if err := orch.Run(ctx); !errors.Is(err, ctx.Err()) {
		return err
	}

	log.Println("Shutting down orchestrator.")
	return nil
}

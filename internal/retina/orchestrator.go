// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

// Package retina implements the Retina orchestrator, which schedules
// ProbingDirectives to connected agents and streams the resulting
// ForwardingInfoElements to HTTP clients.
// TODO: rename package retina → orchestrator when internal/retina/ is
// restructured to internal/orchestrator/.
// TODO: flatten internal/retina/issuance, internal/retina/servers, and
// internal/retina/structures into this package to improve testability and
// remove unnecessary package boundaries. Defer until after first release.
package retina

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/dioptra-io/retina-orchestrator/internal/retina/issuance"
	"github.com/dioptra-io/retina-orchestrator/internal/retina/servers"
	"github.com/dioptra-io/retina-orchestrator/internal/retina/structures"
	"golang.org/x/sync/errgroup"
)

// Config is the main configuration struct used in the orchestrator.
type Config struct {
	// AgentAddress is the TCP listening address for agent connections, in the form "host:port".
	AgentAddress      string
	AgentBufferLength int

	// APIAddress is the TCP listening address for the HTTP API server, in the form "host:port".
	APIAddress string
	// APIReadHeaderTimeout defaults to 5 seconds if zero.
	APIReadHeaderTimeout time.Duration

	PDPath string
	Seed   uint64
	// IssuanceRate is the target global issuance rate of probing directives
	// (PDs per second, approximate).
	IssuanceRate float64
	// ImpactThreshold is the maximum number of concurrent directives allowed
	// to impact a single address in the responsible probing algorithm.
	ImpactThreshold float64
	// Secret is the shared secret for agent authentication.
	// This is an MVS feature and will be removed soon.
	Secret string
}

// Validate checks all configuration fields and applies defaults where appropriate.
// Returns an error if any required field is missing or invalid.
func (c *Config) Validate() error {
	if c.AgentAddress == "" {
		return fmt.Errorf("AgentAddress cannot be empty")
	}
	if c.AgentBufferLength < 8192 {
		return fmt.Errorf("AgentBufferLength is too small: got %d, minimum 8192", c.AgentBufferLength)
	}
	if c.APIAddress == "" {
		return fmt.Errorf("APIAddress cannot be empty")
	}
	if c.PDPath == "" {
		return fmt.Errorf("PDPath cannot be empty")
	}
	if c.IssuanceRate <= 0 {
		return fmt.Errorf("IssuanceRate must be greater than zero: got %f", c.IssuanceRate)
	}
	if c.ImpactThreshold <= 0 {
		return fmt.Errorf("ImpactThreshold must be greater than zero: got %f", c.ImpactThreshold)
	}
	if c.APIReadHeaderTimeout == 0 {
		c.APIReadHeaderTimeout = 5 * time.Second
	}
	return nil
}

type orch struct {
	config      *Config
	scheduler   *issuance.Scheduler
	apiServer   *servers.APIServer
	agentServer *servers.AgentServer
	pdQueue     *structures.Queue[api.ProbingDirective]
	ringBuffer  *structures.RingBuffer[api.ForwardingInfoElement]
}

// NewOrch creates a new orchestrator from the given configuration. Returns an
// error if the configuration is invalid or any component creation fails.
func NewOrch(config *Config) (*orch, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	o := &orch{config: config}

	scheduler, err := issuance.NewScheduler(config.Seed, config.IssuanceRate, config.PDPath)
	if err != nil {
		return nil, fmt.Errorf("error on creating scheduler: %w", err)
	}
	o.scheduler = scheduler

	apiServer, err := servers.NewAPIServer(&servers.APIServerConfig{
		Address:           config.APIAddress,
		ReadHeaderTimeout: config.APIReadHeaderTimeout,
		FIEHandler:        o.fieStreamHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating API server: %w", err)
	}
	o.apiServer = apiServer

	agentServer, err := servers.NewAgentServer(&servers.AgentServerConfig{
		BufferLength:     config.AgentBufferLength,
		HandshakeTimeout: 5 * time.Second,
		Address:          config.AgentAddress,
		AgentHandler:     o.agentHandler,
		AuthHandler:      o.agentAuthHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating agent server: %w", err)
	}
	o.agentServer = agentServer

	pdQueue, err := structures.NewQueue[api.ProbingDirective](100)
	if err != nil {
		return nil, fmt.Errorf("error on creating pd queue: %w", err)
	}
	o.pdQueue = pdQueue

	ringBuffer, err := structures.NewRingBuffer[api.ForwardingInfoElement](100)
	if err != nil {
		return nil, fmt.Errorf("error on creating ring buffer: %w", err)
	}
	o.ringBuffer = ringBuffer

	return o, nil
}

func (o *orch) Run(parentCtx context.Context) error {
	group, ctx := errgroup.WithContext(parentCtx)
	group.Go(func() error {
		return o.runAPIServer(ctx)
	})
	group.Go(func() error {
		return o.runAgentServer(ctx)
	})
	group.Go(func() error {
		return o.runScheduler(ctx)
	})

	return group.Wait()
}

func (o *orch) runScheduler(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pd := o.scheduler.NextPD()
		if pd == nil {
			continue
		}

		if err := o.pdQueue.Push(ctx, pd.AgentID, pd); err != nil {
			// TODO: downgrade to DEBUG once slog is added.
			// PD drops before agent connection is expected.
			log.Printf("Scheduler: no queue for agent %q, directive dropped", pd.AgentID)
		}
	}
}

func (o *orch) runAPIServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.apiServer.ListenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.apiServer.Shutdown(3 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}
	return nil
}

func (o *orch) runAgentServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.agentServer.ListenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.agentServer.Shutdown(3 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}
	return nil
}

func (o *orch) fieStreamHandler(s *servers.FIEClient) {
	consumer := o.ringBuffer.NewConsumer()
	defer consumer.Close()

	for {
		fie, seq, err := consumer.Pop(s.Context())
		if err != nil {
			return
		}
		seqFIE := &servers.SequencedFIE{
			ForwardingInfoElement: *fie,
			SequenceNumber:        seq,
		}

		// TODO: log sent FIE at DEBUG level once slog is added.
		if err = s.SendFIE(seqFIE); err != nil {
			return
		}
	}
}

func (o *orch) agentHandler(status *servers.AgentAuthStatus, s *servers.AgentStream) {
	consumer, err := o.pdQueue.NewConsumer(status.AgentID)
	if err != nil {
		log.Printf("Agent %q is already connected, rejecting", status.AgentID)
		return
	}
	defer consumer.Close()

	log.Printf("Agent %q connected", status.AgentID)

	group, ctx := errgroup.WithContext(s.Context())

	group.Go(func() error {
		for {
			fie, err := s.ReceiveFIE()
			if err != nil {
				return err
			}

			// TODO: log received FIE at DEBUG level once slog is added.
			if err := o.scheduler.UpdateFromFIE(fie); err != nil {
				log.Printf("Scheduler: failed to update from FIE: %v", err)
			}

			// Only push complete FIEs to the ring buffer for streaming.
			if fie.NearInfo == nil || fie.FarInfo == nil {
				continue
			}

			_ = o.ringBuffer.Push(fie)
		}
	})

	group.Go(func() error {
		for {
			pd, err := consumer.Pop(ctx)
			if err != nil {
				return err
			}

			// TODO: log sent PD at DEBUG level once slog is added.
			if err = s.SendPD(pd); err != nil {
				return err
			}
		}
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		log.Printf("Agent %q stream failed: %v", status.AgentID, err)
	}
	log.Printf("Agent %q disconnected", status.AgentID)
}

func (o *orch) agentAuthHandler(auth api.AuthRequest) api.AuthResponse {
	if auth.Secret == o.config.Secret {
		return api.AuthResponse{
			Authenticated: true,
			Message:       "authenticated",
		}
	}
	return api.AuthResponse{
		Authenticated: false,
		Message:       "secret is not correct",
	}
}

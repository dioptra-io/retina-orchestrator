// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

// Package orchestrator implements the Retina orchestrator, which schedules
// ProbingDirectives (PDs) to connected agents and streams the resulting
// ForwardingInfoElements to HTTP clients.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
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
	// Logger is the structured logger for the orchestrator.
	// Defaults to discarding all output if nil.
	Logger *slog.Logger
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
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return nil
}

type orch struct {
	config      *Config
	logger      *slog.Logger
	scheduler   *Scheduler
	agentServer *agentServer
	apiServer   *apiServer
	pdQueue     *structures.Queue[api.ProbingDirective]
	ringBuffer  *structures.RingBuffer[api.ForwardingInfoElement]
}

// NewOrch creates a new orchestrator from the given configuration. Returns an
// error if the configuration is invalid or any component creation fails.
func NewOrch(config *Config) (*orch, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	o := &orch{
		config: config,
		logger: config.Logger,
	}

	scheduler, err := NewScheduler(config.Seed, config.IssuanceRate, config.PDPath,
		config.Logger.With("component", "scheduler"))
	if err != nil {
		return nil, fmt.Errorf("error on creating scheduler: %w", err)
	}
	o.scheduler = scheduler

	apiServer, err := newAPIServer(&apiServerConfig{
		address:           config.APIAddress,
		readHeaderTimeout: config.APIReadHeaderTimeout,
		fieHandler:        o.fieStreamHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating API server: %w", err)
	}
	o.apiServer = apiServer

	agentServer, err := newAgentServer(&agentServerConfig{
		bufferLength:     config.AgentBufferLength,
		handshakeTimeout: 5 * time.Second,
		address:          config.AgentAddress,
		agentHandler:     o.agentHandler,
		authHandler:      o.agentAuthHandler,
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
			// PD drops before agent connection are expected during startup.
			o.logger.Debug("PD dropped: no queue for agent",
				slog.String("agent_id", pd.AgentID),
				slog.Uint64("pd_id", pd.ProbingDirectiveID))
		}
	}
}

func (o *orch) runAPIServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.apiServer.listenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.apiServer.close(3 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}
	return nil
}

func (o *orch) runAgentServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.agentServer.listenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.agentServer.close(3 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) && !errors.Is(err, ErrServerShutdown) {
		return err
	}
	return nil
}

func (o *orch) fieStreamHandler(s *fieClient) {
	consumer := o.ringBuffer.NewConsumer()
	defer consumer.Close()

	for {
		fie, seq, err := consumer.Pop(s.context())
		if err != nil {
			return
		}
		seqFIE := &SequencedFIE{
			ForwardingInfoElement: *fie,
			SequenceNumber:        seq,
		}

		o.logger.Debug("Sending FIE to client",
			slog.Uint64("seq", seq),
			slog.Uint64("pd_id", fie.ProbingDirectiveID))
		if err = s.sendFIE(seqFIE); err != nil {
			return
		}
	}
}

func (o *orch) agentHandler(status *agentAuthStatus, s *agentStream) {
	consumer, err := o.pdQueue.NewConsumer(status.agentID)
	if err != nil {
		o.logger.Warn("Agent already connected, rejecting", "agent_id", status.agentID)
		return
	}
	defer consumer.Close()

	o.logger.Info("Agent connected", "agent_id", status.agentID)

	group, ctx := errgroup.WithContext(s.context())

	group.Go(func() error {
		for {
			fie, err := s.receiveFIE()
			if err != nil {
				return err
			}

			o.logger.Debug("FIE received",
				slog.String("agent_id", status.agentID),
				slog.Uint64("pd_id", fie.ProbingDirectiveID),
				slog.Bool("complete", fie.NearInfo != nil && fie.FarInfo != nil))
			if err := o.scheduler.UpdateFromFIE(fie); err != nil {
				o.logger.Error("Failed to update scheduler from FIE", "agent_id", status.agentID, "err", err)
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

			o.logger.Debug("Sending PD to agent",
				slog.String("agent_id", status.agentID),
				slog.Uint64("pd_id", pd.ProbingDirectiveID),
				slog.String("dest", pd.DestinationAddress.String()))
			if err = s.sendPD(pd); err != nil {
				return err
			}
		}
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		o.logger.Error("Agent stream failed", "agent_id", status.agentID, "err", err)
	}
	o.logger.Info("Agent disconnected", "agent_id", status.agentID)
}

func (o *orch) agentAuthHandler(auth api.AuthRequest) api.AuthResponse {
	if auth.Secret == o.config.Secret {
		return api.AuthResponse{
			Authenticated: true,
			Message:       "authenticated",
		}
	}
	o.logger.Warn("Agent authentication failed")
	return api.AuthResponse{
		Authenticated: false,
		Message:       "secret is not correct",
	}
}

// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

// Package orchestrator implements the Retina orchestrator, which schedules
// ProbingDirectives (PDs) to connected agents and streams the resulting
// ForwardingInfoElements to HTTP clients.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	"github.com/dioptra-io/retina-commons/model"
	wire "github.com/dioptra-io/retina-commons/wire/v2"
	"github.com/dioptra-io/retina-orchestrator/internal/orchestrator/structures"
	"golang.org/x/sync/errgroup"
)

// Config is the main configuration struct used in the orchestrator.
// All CLI flags are defined here; sub-components are configured from this struct.
type Config struct {
	// AgentAddress is the TCP listening address for agent connections, in the form "host:port".
	AgentAddress      string
	AgentBufferLength int

	// PDQueueSize is the number of PDs that can be queued per agent.
	// Increase this value if agents are slow to consume directives.
	PDQueueSize    int
	RingBufferSize int

	// APIAddress is the TCP listening address for the HTTP API server, in the form "host:port".
	APIAddress string
	// APIReadHeaderTimeout defaults to 5 seconds if zero.
	APIReadHeaderTimeout time.Duration

	FIEFilterPolicy string
	Seed            uint64
	// Secret is the shared secret for agent authentication.
	// This is an MVS feature and will be removed soon.
	Secret string

	// Scheduler parameters
	PDPathV4     string
	PDPathV6     string
	IssuanceRate float64
	// ImpactThreshold is the maximum allowed probe rate (probes/second) on any
	// single address in the responsible probing algorithm.
	ImpactThreshold            float64
	ActiveSetSize              int
	ConsecutiveMissesThreshold int
	MaxEvictions               int
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
	if c.PDQueueSize <= 0 {
		return fmt.Errorf("PDQueueSize must be greater than zero: got %d", c.PDQueueSize)
	}
	if c.RingBufferSize <= 0 {
		return fmt.Errorf("RingBufferSize must be greater than zero: got %d", c.RingBufferSize)
	}
	if c.APIAddress == "" {
		return fmt.Errorf("APIAddress cannot be empty")
	}
	if c.APIReadHeaderTimeout == 0 {
		c.APIReadHeaderTimeout = 5 * time.Second
	}
	if c.FIEFilterPolicy == "" {
		c.FIEFilterPolicy = "both"
	}
	if !slices.Contains([]string{"any", "one", "both"}, c.FIEFilterPolicy) {
		return fmt.Errorf("supported FIE filtering policies are 'any', 'one', or 'both' got %s", c.FIEFilterPolicy)
	}
	return c.validateSchedulerConfig()
}

// validateSchedulerConfig checks scheduler-specific configuration fields.
func (c *Config) validateSchedulerConfig() error {
	if c.PDPathV4 == "" && c.PDPathV6 == "" {
		return fmt.Errorf("at least one of PDPathV4 or PDPathV6 must be provided")
	}
	if c.IssuanceRate <= 0 {
		return fmt.Errorf("IssuanceRate must be greater than zero: got %f", c.IssuanceRate)
	}
	if c.ImpactThreshold <= 0 {
		return fmt.Errorf("ImpactThreshold must be greater than zero: got %f", c.ImpactThreshold)
	}
	if c.ActiveSetSize <= 0 {
		return fmt.Errorf("ActiveSetSize must be greater than zero: got %d", c.ActiveSetSize)
	}
	if c.ConsecutiveMissesThreshold <= 0 {
		return fmt.Errorf("ConsecutiveMissesThreshold must be greater than zero: got %d", c.ConsecutiveMissesThreshold)
	}
	if c.MaxEvictions <= 0 {
		return fmt.Errorf("MaxEvictions must be greater than zero: got %d", c.MaxEvictions)
	}
	return nil
}

type orch struct {
	config      *Config
	logger      *slog.Logger
	metrics     *Metrics
	scheduler   *Scheduler
	agentServer *agentServer
	apiServer   *apiServer
	pdQueue     *structures.Queue[model.ProbingDirective]
	ringBuffer  *structures.RingBuffer[model.ForwardingInfoElement]
}

// NewOrch creates a new orchestrator from the given configuration. Returns an
// error if the configuration is invalid or any component creation fails.
func NewOrch(config *Config, logger *slog.Logger, metrics *Metrics) (*orch, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	// Copy after Validate() (which applies defaults by mutating config) so
	// the orchestrator's own reads can't race with the caller mutating the
	// original afterward.
	configCopy := *config
	config = &configCopy

	o := &orch{
		config:  config,
		logger:  logger,
		metrics: metrics,
	}

	scheduler, err := NewScheduler(&SchedulerConfig{
		Seed:                       config.Seed,
		IssuanceRate:               config.IssuanceRate,
		ImpactThreshold:            config.ImpactThreshold,
		PDPathV4:                   config.PDPathV4,
		PDPathV6:                   config.PDPathV6,
		ActiveSetSize:              config.ActiveSetSize,
		ConsecutiveMissesThreshold: config.ConsecutiveMissesThreshold,
		MaxEvictions:               config.MaxEvictions,
	}, logger.With("component", "scheduler"), metrics)
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
	}, logger, metrics)
	if err != nil {
		return nil, fmt.Errorf("error on creating agent server: %w", err)
	}
	o.agentServer = agentServer

	pdQueue, err := structures.NewQueue[model.ProbingDirective](config.PDQueueSize)
	if err != nil {
		return nil, fmt.Errorf("error on creating pd queue: %w", err)
	}
	o.pdQueue = pdQueue

	ringBuffer, err := structures.NewRingBuffer[model.ForwardingInfoElement](config.RingBufferSize)
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
		pd := o.scheduler.NextPD(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if pd == nil {
			continue
		}

		if err := o.pdQueue.TryPush(pd.AgentID, pd); err != nil {
			o.logger.Debug("PD dropped: no queue for agent",
				slog.String("agent_id", pd.AgentID),
				slog.Uint64("pd_id", pd.ProbingDirectiveID))
		} else {
			o.metrics.AgentQueueSize.WithLabelValues(pd.AgentID).Inc()
		}
	}
}

func (o *orch) runAPIServer(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer cancel() // wake the shutdown goroutine even if listenAndServe returns nil
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

func (o *orch) runAgentServer(parentCtx context.Context) error {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		defer cancel() // wake the shutdown goroutine even if listenAndServe returns nil
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

// modelFIEToAPIv1 converts a model.ForwardingInfoElement to the legacy
// api/v1 type api_server.go's SequencedFIE still embeds, preserving the
// existing HTTP/JSON wire format for clients until api_server.go's own
// migration. Fallible: wire.IPVersion/wire.Protocol are int32-based
// (protobuf enums) but api.IPVersion/api.Protocol are uint8-based
// (confirmed against api/v1's real source) — a blind numeric cast would
// be a genuine narrowing conversion, even though both enums' legitimate
// values (0, 4, 6 for IPVersion; 0-58 for Protocol) fit comfortably in a
// uint8 in practice. Explicit range checks here, rather than a bare
// cast, so a corrupted/unexpected value errors instead of silently
// wrapping — same reasoning as this codebase's other narrowing
// conversions (e.g. model's TTL narrowing).
func modelFIEToAPIv1(fie *model.ForwardingInfoElement) (api.ForwardingInfoElement, error) {
	if fie.IPVersion < 0 || fie.IPVersion > 255 {
		return api.ForwardingInfoElement{}, fmt.Errorf("ip_version %d exceeds uint8 range", fie.IPVersion)
	}
	if fie.Protocol < 0 || fie.Protocol > 255 {
		return api.ForwardingInfoElement{}, fmt.Errorf("protocol %d exceeds uint8 range", fie.Protocol)
	}

	out := api.ForwardingInfoElement{
		Agent:               api.Agent{AgentID: fie.Agent.ID},
		ProbingDirectiveID:  fie.ProbingDirectiveID,
		IPVersion:           api.IPVersion(fie.IPVersion), //nolint:gosec // range-checked above
		Protocol:            api.Protocol(fie.Protocol),   //nolint:gosec // range-checked above
		SourceAddress:       fie.SourceAddress,
		DestinationAddress:  fie.DestinationAddress,
		ProductionTimestamp: fie.ProductionTimestamp,
	}
	if fie.NearInfo != nil {
		out.NearInfo = &api.Info{
			ProbeTTL:          fie.NearInfo.ProbeTTL,
			ReplyAddress:      fie.NearInfo.ReplyAddress,
			SentTimestamp:     fie.NearInfo.SentTimestamp,
			ReceivedTimestamp: fie.NearInfo.ReceivedTimestamp,
		}
	}
	if fie.FarInfo != nil {
		out.FarInfo = &api.Info{
			ProbeTTL:          fie.FarInfo.ProbeTTL,
			ReplyAddress:      fie.FarInfo.ReplyAddress,
			SentTimestamp:     fie.FarInfo.SentTimestamp,
			ReceivedTimestamp: fie.FarInfo.ReceivedTimestamp,
		}
	}
	return out, nil
}

func (o *orch) fieStreamHandler(s *fieClient) {
	var closeReason string
	consumer := o.ringBuffer.NewConsumer()
	o.metrics.StreamClientsConnected.Inc()
	o.metrics.StreamConnectionsTotal.Inc()
	defer func() {
		consumer.Close()
		o.metrics.StreamClientsConnected.Dec()
		o.metrics.StreamDisconnectionsTotal.WithLabelValues(closeReason).Inc()
		o.logger.Debug("FIE stream closed", slog.String("reason", closeReason))
	}()

	for {
		fie, seq, err := consumer.Pop(s.context())
		if err != nil {
			closeReason = "internal_error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeReason = "shutdown_or_disconnect"
			}
			return
		}
		apiFIE, err := modelFIEToAPIv1(fie)
		if err != nil {
			closeReason = "internal_error"
			o.logger.Error("Failed to convert FIE for HTTP client", slog.Any("err", err))
			return
		}
		seqFIE := &SequencedFIE{
			ForwardingInfoElement: apiFIE,
			SequenceNumber:        seq,
		}

		o.logger.Debug("Sending FIE to client",
			slog.Uint64("seq", seq),
			slog.Uint64("pd_id", fie.ProbingDirectiveID))
		if err = s.sendFIE(seqFIE); err != nil {
			closeReason = "internal_error"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				closeReason = "shutdown_or_disconnect"
			}
			return
		}
		o.metrics.FIEsStreamedTotal.Inc()
		o.metrics.StreamLagSeconds.Observe(time.Since(seqFIE.ProductionTimestamp).Seconds())
	}
}

//nolint:funlen
func (o *orch) agentHandler(status *agentAuthStatus, s *agentStream) {
	consumer, err := o.pdQueue.NewConsumer(status.agentID)
	if err != nil {
		o.logger.Warn("Agent already connected, rejecting", "agent_id", status.agentID)
		return
	}
	defer consumer.Close()

	o.logger.Info("Agent connected", "agent_id", status.agentID)
	o.metrics.AgentQueueSize.WithLabelValues(status.agentID).Set(0)
	defer func() {
		o.logger.Info("Agent disconnected", "agent_id", status.agentID)
		o.metrics.AgentQueueSize.DeleteLabelValues(status.agentID)
	}()

	group, ctx := errgroup.WithContext(s.context())

	group.Go(func() error {
		for {
			fie, err := s.receiveFIE()
			if err != nil {
				return err
			}
			o.metrics.FIEsReceivedTotal.WithLabelValues(status.agentID).Inc()

			o.logger.Debug("FIE received",
				slog.String("agent_id", status.agentID),
				slog.Uint64("pd_id", fie.ProbingDirectiveID),
				slog.Bool("complete", fie.NearInfo != nil && fie.FarInfo != nil))

			if err := o.scheduler.UpdateFromFIE(fie); err != nil {
				o.logger.Error("Failed to update scheduler from FIE", "agent_id", status.agentID, "err", err)
			}

			allow := o.filterFIE(fie)
			if !allow {
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
			o.metrics.PDsSentTotal.WithLabelValues(status.agentID).Inc()
			o.metrics.AgentQueueSize.WithLabelValues(status.agentID).Dec()
		}
	})

	group.Go(func() error {
		<-ctx.Done()
		_ = s.conn.Close()
		return nil
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		o.logger.Error("Agent stream failed", "agent_id", status.agentID, "err", err)
	}
}

// agentAuthHandler takes/returns *wire.AuthRequest/AuthResponse — pointers,
// not values, since these generated proto messages embed a sync.Mutex
// (via protoimpl.MessageState) that copylocks correctly flags if copied
// by value. See authHandleFunc's doc comment in agent_server.go.
func (o *orch) agentAuthHandler(auth *wire.AuthRequest) *wire.AuthResponse {
	if auth.Secret == o.config.Secret {
		return &wire.AuthResponse{
			Authenticated: true,
			Message:       "authenticated",
		}
	}
	o.logger.Warn("Agent authentication failed")
	return &wire.AuthResponse{
		Authenticated: false,
		Message:       "secret is not correct",
	}
}

// filterFIE reports whether a FIE should be streamed based on the policy.
// No longer returns an error: the policy is validated once in
// Config.Validate() against an immutable config copy, so the default
// case below is unreachable in practice.
func (o *orch) filterFIE(fie *model.ForwardingInfoElement) bool {
	switch o.config.FIEFilterPolicy {
	case "any": // allow all FIEs
		return true
	case "one": // allow FIEs with at least one non-nil response address
		return fie.NearInfo != nil || fie.FarInfo != nil
	default: // "both", already the only other value Config.Validate() allows
		return fie.NearInfo != nil && fie.FarInfo != nil
	}
}

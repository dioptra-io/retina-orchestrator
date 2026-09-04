// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

// Package orchestrator implements the Retina orchestrator, which schedules
// ProbingDirectives (PDs) to connected agents and pushes the resulting
// ForwardingInfoElements to retina-api for external streaming.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
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
	PDQueueSize int

	// APIURL is retina-api's ingest endpoint, e.g. "https://retina0.lip6.fr:8090/api/v1/ingest".
	APIURL string
	// APIBufferSize is the outbound FIE buffer capacity. Defaults to 10,000 if zero.
	APIBufferSize int
	// APIReconnectDelay is the wait before retrying a dropped connection. Defaults to 5s if zero.
	APIReconnectDelay time.Duration

	FIEFilterPolicy string
	Seed            uint64
	// Secret is the shared secret for agent authentication.
	// This is an MVS feature and will be removed soon.
	Secret string

	// Scheduler parameters
	PDPathV4     string
	PDPathV6     string
	IssuanceRate float64
	// PDDiffPath is the path to a PD diff file (insert/remove ops). If set,
	// the orchestrator reloads it on SIGHUP and applies the diff to the
	// running scheduler without a restart. Optional — hot-reload is
	// disabled if empty.
	PDDiffPath string
	// ImpactThreshold is the maximum allowed probe rate (probes/second) on any
	// single address in the responsible probing algorithm.
	ImpactThreshold            float64
	ActiveSetSize              int
	ConsecutiveMissesThreshold int
	MaxEvictions               int
}

// applyDefaults fills in zero-valued optional fields with their defaults.
// It never rejects anything — call Validate afterward to check the result.
// NewOrch calls both, in this order.
func (c *Config) applyDefaults() {
	if c.APIBufferSize == 0 {
		c.APIBufferSize = 10_000 // smooths a brief reconnect blip, not a sustained outage
	}
	if c.APIReconnectDelay == 0 {
		c.APIReconnectDelay = 5 * time.Second
	}
	if c.FIEFilterPolicy == "" {
		c.FIEFilterPolicy = "both"
	}
}

// Validate checks all configuration fields and returns an error if any
// required field is missing or invalid. It does not mutate c — call
// applyDefaults first if zero-valued optional fields should be treated as
// "use the default" rather than checked as-is.
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
	if err := c.validateAPIConfig(); err != nil {
		return err
	}
	if !slices.Contains([]string{"any", "one", "both"}, c.FIEFilterPolicy) {
		return fmt.Errorf("supported FIE filtering policies are 'any', 'one', or 'both' got %s", c.FIEFilterPolicy)
	}
	return c.validateSchedulerConfig()
}

// validateAPIConfig checks the fields governing the connection to
// retina-api: APIURL, APIBufferSize, APIReconnectDelay.
func (c *Config) validateAPIConfig() error {
	if c.APIURL == "" {
		return fmt.Errorf("APIURL cannot be empty")
	}
	parsedURL, err := url.Parse(c.APIURL)
	if err != nil {
		return fmt.Errorf("APIURL is not a valid URL: %w", err)
	}
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return fmt.Errorf("APIURL must use http or https, got %q", parsedURL.Scheme)
	}
	if parsedURL.Host == "" {
		return fmt.Errorf("APIURL must include a host")
	}
	if parsedURL.Path == "" || parsedURL.Path == "/" {
		return fmt.Errorf("APIURL must include a non-root path")
	}
	if parsedURL.Fragment != "" {
		return fmt.Errorf("APIURL must not include a fragment")
	}
	if c.APIBufferSize < 0 {
		return fmt.Errorf("APIBufferSize cannot be negative: got %d", c.APIBufferSize)
	}
	if c.APIBufferSize > 5_000_000 {
		return fmt.Errorf("APIBufferSize is implausibly large (%d): likely a config error", c.APIBufferSize)
	}
	if c.APIReconnectDelay < 0 {
		return fmt.Errorf("APIReconnectDelay cannot be negative: got %s", c.APIReconnectDelay)
	}
	return nil
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
	// PDDiffPath is intentionally unvalidated here: it's optional, and an
	// empty value simply disables hot-reload (see watchPDDiffReload).
	return nil
}

type orch struct {
	config      *Config
	logger      *slog.Logger
	metrics     *Metrics
	scheduler   *Scheduler
	agentServer *agentServer
	apiClient   *apiClient
	pdQueue     *structures.Queue[model.ProbingDirective]
}

// NewOrch creates a new orchestrator from the given configuration. Returns an
// error if the configuration is invalid or any component creation fails.
func NewOrch(config *Config, logger *slog.Logger, metrics *Metrics) (*orch, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	config.applyDefaults()
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}

	// Copy after applyDefaults() (which mutates config) so the
	// orchestrator's own reads can't race with the caller mutating the
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
		PDDiffPath:                 config.PDDiffPath,
		ActiveSetSize:              config.ActiveSetSize,
		ConsecutiveMissesThreshold: config.ConsecutiveMissesThreshold,
		MaxEvictions:               config.MaxEvictions,
	}, logger.With("component", "scheduler"), metrics)
	if err != nil {
		return nil, fmt.Errorf("error on creating scheduler: %w", err)
	}
	o.scheduler = scheduler

	ac, err := newAPIClient(&apiClientConfig{
		url:            config.APIURL,
		bufferSize:     config.APIBufferSize,
		reconnectDelay: config.APIReconnectDelay,
		logger:         logger.With("component", "api_client"),
		metrics:        metrics,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating api client: %w", err)
	}
	o.apiClient = ac

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

	return o, nil
}

// Run starts every orchestrator component and blocks until one fails or
// ctx is canceled. apiClient.run always returns nil — a failed or dropped
// connection to retina-api reconnects on its own and never brings down the
// rest of the orchestrator (see api_client.go). runAgentServer/
// runScheduler/runPDDiffReload return nil on clean shutdown and non-nil
// only for failures that should stop the orchestrator via errgroup's
// cancellation.
func (o *orch) Run(parentCtx context.Context) error {
	group, ctx := errgroup.WithContext(parentCtx)
	group.Go(func() error {
		o.apiClient.run(ctx)
		return nil
	})
	group.Go(func() error {
		return o.runAgentServer(ctx)
	})
	group.Go(func() error {
		return o.runScheduler(ctx)
	})
	group.Go(func() error {
		return o.runPDDiffReload(ctx)
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

// runPDDiffReload listens for SIGHUP and applies PD diff files to the
// running scheduler without restarting the orchestrator. A no-op (blocks
// until ctx is done) if config.PDDiffPath is empty. See watchPDDiffReload
// in reload.go for the mechanism.
func (o *orch) runPDDiffReload(ctx context.Context) error {
	return watchPDDiffReload(ctx, o.scheduler, o.config.PDDiffPath, o.logger.With("component", "pd_diff_reload"))
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

// modelFIEToAPIv1 converts a model.ForwardingInfoElement to the api/v1 wire
// type pushed to retina-api. Fallible: wire.IPVersion/wire.Protocol are
// int32-based (protobuf enums) but api.IPVersion/api.Protocol are
// uint8-based (confirmed against api/v1's real source) — a blind numeric
// cast would be a genuine narrowing conversion, even though both enums'
// legitimate values (0, 4, 6 for IPVersion; 0-58 for Protocol) fit
// comfortably in a uint8 in practice. Explicit range checks here, rather
// than a bare cast, so a corrupted/unexpected value errors instead of
// silently wrapping — same reasoning as this codebase's other narrowing
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

			apiFIE, err := modelFIEToAPIv1(fie)
			if err != nil {
				o.logger.Error("Failed to convert FIE for retina-api", slog.Any("err", err))
				continue
			}
			o.apiClient.push(&apiFIE)
			o.metrics.APIClientFIEsPushedTotal.Inc()
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

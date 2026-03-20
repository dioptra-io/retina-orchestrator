// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT

package retina

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
	apiOrch "github.com/dioptra-io/retina-orchestrator/internal/retina/api"
	"github.com/dioptra-io/retina-orchestrator/internal/retina/issuance"
	"github.com/dioptra-io/retina-orchestrator/internal/retina/servers"
	"github.com/dioptra-io/retina-orchestrator/internal/retina/structures"
	"golang.org/x/sync/errgroup"
)

// Config is the main configuration struct used in the orchestrator.
type Config struct {
	// AgentAddress is the listening address of the JSONL server between
	// retina-orchestrator and retina-agent.
	AgentAddress string
	// AgentBufferLength is the allocated send and receive buffer length for
	// the agent server.
	AgentBufferLength int
	// AgentTimeout is the timeout value for the agent server.
	AgentTimeout time.Duration

	// APIAddress is the listening address of the HTTP API server.
	APIAddress string
	// APITimeout is the timeout value for the HTTP API server.
	APITimeout time.Duration

	// PDPath is the path to the file containing the probing directives.
	PDPath string
	// Seed is the seed used in the randomizer.
	Seed uint64
	// IssuanceRate is the target global issuance rate of probing directives
	// (PDs per second, approximate).
	IssuanceRate float64
	// ImpactThreshold is the maximum impact threshold per address for the
	// responsible probing algorithm.
	ImpactThreshold float64
	// Secret is the secret shared with the agents for authentication.
	// This is an MVS feature and will be removed soon.
	Secret string
}

type orch struct {
	config *Config
	// scheduler schedules the ProbingDirectives and updates by the responses
	// from ForwardingInfoElements and implements respoinsible probing.
	scheduler *issuance.Scheduler
	// apiServer serves the HTTP API endpoint.
	apiServer *servers.HTTPServer
	// agentServer is the JSONL server used to communicate PDs and FIEs
	// with connected agents.
	agentServer *servers.JSONLServer
	// pdQueue is the queue for generated probing directives.
	pdQueue *structures.Queue[api.ProbingDirective]
	// ringbuffer is used to stream FIEs to connected clients.
	ringBuffer *structures.RingBuffer[api.ForwardingInfoElement]
}

// NewOrch creates a new orchestrator from the given configuration. Returns the
// error if any of the component creation fails.
func NewOrch(config *Config) (*orch, error) {
	o := &orch{config: config}

	// Create the Scheduler.
	scheduler, err := issuance.NewScheduler(config.Seed, config.IssuanceRate, config.PDPath)
	if err != nil {
		return nil, fmt.Errorf("error on creating scheduler: %w", err)
	}
	o.scheduler = scheduler

	// Create the API server.
	apiServer, err := servers.NewHTTPServer(&servers.HTTPServerConfig{
		Address:       config.APIAddress,
		StreamHandler: o.httpStreamHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating API server: %w", err)
	}
	o.apiServer = apiServer

	// Create the agent server.
	agentServer, err := servers.NewJSONLServer(&servers.JSONLServerConfig{
		TCPBufferLength: config.AgentBufferLength,
		TCPTimeout:      config.AgentTimeout,
		Address:         config.AgentAddress,
		StreamHandler:   o.jsonlStreamHandler,
		AuthHandler:     o.jsonlAuthHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating agent server: %w", err)
	}
	o.agentServer = agentServer

	// Create pd queue
	pdQueue, _ := structures.NewQueue[api.ProbingDirective](100)
	o.pdQueue = pdQueue

	// Create ringBuffer
	ringBuffer, err := structures.NewRingBuffer[api.ForwardingInfoElement](100)
	if err != nil {
		return nil, err
	}
	o.ringBuffer = ringBuffer

	return o, nil
}

func (o *orch) Run(parentCtx context.Context) error {
	// Create a errgroup and run each components individually. If the context is
	// cancelled then the whole system is cancelled.
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

// runScheduler starts the scheduler loop for issuing new ProbingDirectives
// using the respoinsible probing algorithm.
func (o *orch) runScheduler(ctx context.Context) error {
	var pd *api.ProbingDirective

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pd = o.scheduler.NextPD()
		if pd == nil {
			log.Println("Bernoulli experiment failed, skipping PD.")
			continue
		}

		if err := o.pdQueue.Push(ctx, pd.AgentID, pd); err != nil {
			log.Printf("Cannot find the queue for agent id %v", pd.AgentID)
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

// httpStreamHandler is the http handler for the http server.
func (o *orch) httpStreamHandler(info *servers.FIEStreamerInfo, s *servers.FIEStreamer) {
	ringBufferConsumer := o.ringBuffer.NewConsumer()
	defer ringBufferConsumer.Close()

	for {
		fie, seq, err := ringBufferConsumer.Pop(s.Context())
		if err != nil {
			return
		}

		// Set the sequence number to the sequence number of the ring buffer.
		seqFIE := &apiOrch.SequencedForwardingInfoElement{
			ForwardingInfoElement: *fie,
			SequenceNumber:        seq,
		}

		err = s.Send(seqFIE)
		if err != nil {
			return
		}
	}
}

// jsonlStreamHandler is the handler for streaming.
func (o *orch) jsonlStreamHandler(status *servers.JSONLAuthStatus, s *servers.JSONLStream) {
	pdQueueConsumer, err := o.pdQueue.NewConsumer(status.AgentID)
	if err != nil {
		log.Printf("Agent with agent id %q is already connected.", status.AgentID)
	}
	defer pdQueueConsumer.Close()

	group, ctx := errgroup.WithContext(s.Context())

	group.Go(func() error {
		for {
			fie, err := s.Receive()
			if err != nil {
				return err
			}

			// Make a completeness check.
			if fie.FarInfo.ReplyAddress == nil || fie.NearInfo.ReplyAddress == nil {
				continue
			}

			// don't care about the slow consumers
			_ = o.ringBuffer.Push(fie)
		}
	})
	group.Go(func() error {
		for {
			pd, err := pdQueueConsumer.Pop(ctx)
			if err != nil {
				return err
			}

			log.Printf("Sent PD: %v", pd)

			err = s.Send(pd)
			if err != nil {
				return err
			}
		}
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		log.Printf("jsonl stream handler failed: %v\n", err)
	}
	log.Println("jsonl stream handler exited")
}

// jsonlAuthHandler is the auth handler for the jsonl server.
func (o *orch) jsonlAuthHandler(auth api.AuthRequest) api.AuthResponse {
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

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
	// JSONLServerAddress is the listening address of the JSONL server between
	// retina-orchestrator and retina-agent.
	JSONLServerAddress string
	// JSONLBufferLength is the allocated send and receive buffer length for JSONL
	// server.
	JSONLBufferLength int
	// JSONLTimeout is the timeout value for JSONL server.
	JSONLTimeout time.Duration

	// HTTPServerAddress is the address of the HTTP server used to add
	// directives and stream data.
	HTTPServerAddress string
	// HTTPTimeout is the timeout value for the http server.
	HTTPTimeout time.Duration

	// PDPath is the filepath of the file that stores the ProbingDirectives.
	PDPath string
	// Seed is the seed used in the randomizer.
	Seed uint64
	// IssueRate denotes the number of PDs issued per second.
	IssueRate float64
	// ImpactCap is desired maximum impact capacity per address for the system.
	ImpactCap float64
	// SecretString is the provided secret via command lines. This is an MVS
	// feature and will be removed soon.
	SecretString string
}

type orch struct {
	config *Config
	// scheduler schedules the ProbingDirectives and updates by the responses
	// from ForwardingInfoElements and implements respoinsible probing.
	scheduler *issuance.Scheduler
	// httpServer is used to provide an server to explore orchestrator.
	httpServer *servers.HTTPServer
	// jsonlServer is the jsonl server implementation used to communicate PDs
	// and FIEs.
	jsonlServer *servers.JSONLServer
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
	scheduler, err := issuance.NewScheduler(config.Seed, config.IssueRate, config.PDPath)
	if err != nil {
		return nil, fmt.Errorf("error on creating scheduler: %w", err)
	}
	o.scheduler = scheduler

	// Create the http server.
	httpServer, err := servers.NewHTTPServer(&servers.HTTPServerConfig{
		Address:       config.HTTPServerAddress,
		StreamHandler: o.httpStreamHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating http server: %w", err)
	}
	o.httpServer = httpServer

	// Create the jsonl server.
	jsonlServer, err := servers.NewJSONLServer(&servers.JSONLServerConfig{
		TCPBufferLength: config.JSONLBufferLength,
		TCPTimeout:      config.JSONLTimeout,
		Address:         config.JSONLServerAddress,
		StreamHandler:   o.jsonlStreamHandler,
		AuthHandler:     o.jsonlAuthHandler,
	})
	if err != nil {
		return nil, fmt.Errorf("error on creating jsonl server: %w", err)
	}
	o.jsonlServer = jsonlServer

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
		return o.runHTTPServer(ctx)
	})
	group.Go(func() error {
		return o.runJSONLServer(ctx)
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

		pd = o.scheduler.Issue()
		if pd == nil {
			log.Println("Bernoulli experiment failed, skipping PD.")
			continue
		}

		if err := o.pdQueue.Push(ctx, pd.AgentID, pd); err != nil {
			log.Printf("Cannot find the queue for agent id %v", pd.AgentID)
		}
	}
}

func (o *orch) runHTTPServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.httpServer.ListenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.httpServer.Shutdown(3 * time.Second)
	})
	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return err
	}
	return nil
}

func (o *orch) runJSONLServer(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return o.jsonlServer.ListenAndServe()
	})
	group.Go(func() error {
		<-ctx.Done()
		return o.jsonlServer.Shutdown(3 * time.Second)
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
	if auth.Secret == o.config.SecretString {
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

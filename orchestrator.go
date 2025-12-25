package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"golang.org/x/sync/errgroup"
)

// Config contains the configuration parameters for orchestrator.
type Config struct {
	// MaxAgentProbingRate is the maximum probing rate per agent the orchestrator enforces. It is in number of probes
	// per second.
	MaxAgentProbingRate uint

	// GeneratorReader is the stream that the SS structs are encoded to.
	GeneratorReader io.Reader

	// GeneratorWriter is the stream that the generated PDs are decoded from.
	GeneratorWriter io.Writer
}

// state represents the internal state of the orchestrator.
type state struct {
	// disallowedDestinations is a set of IP addresses that system is not allowed to generate as destinationAddress.
	disallowedDestinations map[[16]byte]struct{}

	// activeAgentIDs is a set of agentIDs that are active.
	activeAgentIDs []string
}

type orchestrator struct {
	// state is the internal orchestrator state, it contains values from config and orchestrator.
	// Expected to mutate thus protected by the mutex.
	state state

	// config holds orchestrator-specific parameters.
	// Expected to not mutate.
	config Config

	// mutex protects access to state.
	mutex sync.Mutex

	// All the channels:

	// generatorSSChan is the channel to hold SS structs.
	generatorSSChan chan *api.SystemStatus

	// generatorPDChan is the channel to hold PD structs.
	generatorPDChan chan *api.ProbingDirective

	// agentPDChans is the map of channels to hold PD structs.
	agentPDChans map[string]chan *api.ProbingDirective
	// agentPDChansMutex protects agentPDChans.
	agentPDChansMutex sync.Mutex

	// agentFIEChan is the channel to hold FIE structs.
	agentFIEChan chan *api.ForwardingInfoElement

	// clientFIEChans is the map of channels to hold FIE structs.
	clientFIEChans map[string]chan *api.ForwardingInfoElement
	// clientFIEChansMutex protects clientFIEChans.
	clientFIEChansMutex sync.Mutex
}

func RunOrchestrator(parentCtx context.Context, config Config) error {
	orchestrator := &orchestrator{
		state: state{
			disallowedDestinations: make(map[[16]byte]struct{}),
			activeAgentIDs:         []string{},
		},
		config: config,
	}

	group, ctx := errgroup.WithContext(parentCtx)

	// Goroutine: goroutine that sends SS from generatorSSChan to generator.
	group.Go(func() error {
		return dequeueAndWriteJSON(ctx,
			orchestrator.config.GeneratorWriter,
			orchestrator.generatorSSChan)
	})

	// Goroutine: goroutine that sends PD from generator to generatorPDChan.
	group.Go(func() error {
		return readJSONAndEnqueue(ctx,
			orchestrator.config.GeneratorReader,
			orchestrator.generatorPDChan)
	})

	// Goroutine: goroutine that sends PD from generatorPDChan to agentFIEChans[agentID].
	group.Go(func() error {
		return readAssignAndSend(ctx,
			orchestrator.generatorPDChan,
			orchestrator.agentPDChans,
			&orchestrator.agentPDChansMutex,
			func(pd *api.ProbingDirective) string {
				return pd.AgentID
			})
	})

	// Wait until both loops end.
	if err := group.Wait(); err != nil && errors.Is(err, ctx.Err()) {
		return fmt.Errorf("generator failed: %w", err)
	}

	log.Println("Generator stopped.")

	return nil
}

func readAssignAndSend[T any](ctx context.Context, inChan chan *T, outChan map[string]chan *T, outMutex *sync.Mutex, selectFn func(t *T) string) error {
	var (
		obj   *T
		objCh chan *T
		ok    bool
		err   error
	)

	for {
		if obj, err = dequeue(ctx, inChan); err != nil {
			return err
		}

		outMutex.Lock()
		objCh, ok = outChan[selectFn(obj)]
		outMutex.Unlock()

		// The channel does not exist, drop the struct.
		if !ok {
			continue
		}

		if err = enqueue(ctx, objCh, obj); err != nil {
			return err
		}
	}

}

func readJSONAndEnqueue[T any](ctx context.Context, reader io.Reader, ch chan *T) error {
	var (
		decoder = json.NewDecoder(reader)
		obj     *T
		err     error
	)

	for {
		if err = decoder.Decode(obj); err != nil {
			return err
		}
		if err = enqueue(ctx, ch, obj); err != nil {
			return err
		}
	}
}

func dequeueAndWriteJSON[T any](ctx context.Context, w io.Writer, ch chan *T) error {
	var (
		encoder = json.NewEncoder(w)
		ss      *T
		err     error
	)

	for {
		if ss, err = dequeue(ctx, ch); err != nil {
			return err
		}
		if err = encoder.Encode(ss); err != nil {
			return err
		}
	}
}

func enqueue[T any](ctx context.Context, ch chan<- *T, v *T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	case ch <- v:
		return nil
	}
}

func dequeue[T any](ctx context.Context, ch <-chan *T) (*T, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case v := <-ch:
		return v, nil
	}
}

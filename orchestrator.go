package main

import (
	"context"
	"crypto/rand"
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

	// MessengerBuffer is the number of elements allocated in the messenger buffers.
	MessengerBuffer uint
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

	// All the messengers:

	// generatorSSMessenger is the messenger used to send SSes to the generator.
	// We assume there is only one generator who is registered with ID "default".
	generatorSSMessenger *Messenger[api.SystemStatus]

	// agentPDMessenger is the messenger used to receive PDs from the generator and send it to an assigned agent.
	// We assume each agent is registered with their agentID.
	agentPDMessenger *Messenger[api.ProbingDirective]

	// clientFIEMessenger is the messenger used to receive FIEs from the agents and send it to all clients.
	// We assume each client is registered with a randomly generated unique ID.
	clientFIEMessenger *Messenger[api.ForwardingInfoElement]
}

func RunOrchestrator(parentCtx context.Context, config Config) error {
	orchestrator := &orchestrator{
		state: state{
			disallowedDestinations: make(map[[16]byte]struct{}),
			activeAgentIDs:         []string{},
		},
		config:               config,
		generatorSSMessenger: NewMessenger[api.SystemStatus](config.MessengerBuffer),
		agentPDMessenger:     NewMessenger[api.ProbingDirective](config.MessengerBuffer),
		clientFIEMessenger:   NewMessenger[api.ForwardingInfoElement](config.MessengerBuffer),
	}

	log.Println("Orchestrator stopped.")

	return nil
}

func (o *orchestrator) handleGenerator(parentCtx context.Context, rw io.ReadWriter) error {
	// Try to register itself to the messenger with the name "default".
	// This ensures there can be only one generator.
	if err := o.agentPDMessenger.RegisterAs("default"); err != nil {
		return err
	}
	defer o.agentPDMessenger.UnregisterAs("default")

	group, ctx := errgroup.WithContext(parentCtx)

	// Goroutine: decode PD, send to assigned agent.
	// Lifetime is limited to this method.
	group.Go(func() error {
		var (
			decoder = json.NewDecoder(rw)
			pd      *api.ProbingDirective
			err     error
		)

		for {
			if err = decoder.Decode(pd); err != nil {
				return err
			}
			if err = o.agentPDMessenger.SendTo(ctx, pd.AgentID, pd); err != nil {
				log.Printf("Cannot find the agentID %q from the PD. Skipping.", pd.AgentID)
				continue
			}
		}
	})

	// Goroutine: get SS from default generator, encode SS.
	// Lifetime is limited to this method.
	group.Go(func() error {
		var (
			encoder = json.NewEncoder(rw)
			ss      *api.SystemStatus
			err     error
		)

		for {
			// We have only one generator and it has the ID: "default".
			if ss, err = o.generatorSSMessenger.GetAs(ctx, "default"); err != nil {
				return err
			}
			if err = encoder.Encode(ss); err != nil {
				return err
			}
		}
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return fmt.Errorf("handle generator failed: %w", err)
	}

	return nil
}

func (o *orchestrator) handleAgent(parentCtx context.Context, agentID string, rw io.ReadWriter) error {
	// Try to register itself to the messenger.
	if err := o.agentPDMessenger.RegisterAs(agentID); err != nil {
		return err
	}
	defer o.agentPDMessenger.UnregisterAs(agentID)

	group, ctx := errgroup.WithContext(parentCtx)

	// Goroutine: decode FIE, send to all clients.
	// Lifetime is limited to this method.
	group.Go(func() error {
		var (
			decoder = json.NewDecoder(rw)
			fie     *api.ForwardingInfoElement
			err     error
		)

		for {
			if err = decoder.Decode(fie); err != nil {
				return err
			}
			if err = o.clientFIEMessenger.SendAll(ctx, fie); err != nil && err == ErrDroppedAtLeastOneMessage {
				log.Println("At least one client disconnected, cannot send FIE. Skipping.")
				continue
			} else {
				return err
			}
		}
	})

	// Goroutine: get PD from agent with the agentID, encode PD.
	// Lifetime is limited to this method.
	group.Go(func() error {
		var (
			encoder = json.NewEncoder(rw)
			pd      *api.ProbingDirective
			err     error
		)

		for {
			if pd, err = o.agentPDMessenger.GetAs(ctx, agentID); err != nil {
				return err
			}
			if err = encoder.Encode(pd); err != nil {
				return err
			}
		}
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return fmt.Errorf("handle agent failed: %w", err)
	}

	return nil
}

func (o *orchestrator) handleClient(parentCtx context.Context, w io.Writer) error {
	clientID := newUUID()
	// Try to register itself to the messenger.
	if err := o.clientFIEMessenger.RegisterAs(clientID); err != nil {
		return err
	}
	defer o.clientFIEMessenger.UnregisterAs(clientID)

	group, ctx := errgroup.WithContext(parentCtx)

	// Goroutine: get FIE from agents, encode FIE.
	// Lifetime is limited to this method.
	group.Go(func() error {
		var (
			encoder = json.NewEncoder(w)
			fie     *api.ForwardingInfoElement
			err     error
		)

		for {
			if fie, err = o.clientFIEMessenger.GetAs(ctx, clientID); err != nil {
				return err
			}
			if err = encoder.Encode(fie); err != nil {
				return err
			}
		}
	})

	if err := group.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		return fmt.Errorf("handle client failed: %w", err)
	}

	return nil
}

func newUUID() string {
	var b [16]byte
	// rand.Rand never returns an error.
	_, _ = rand.Read(b[:])

	// Set version (4) and variant (RFC 4122)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"%08x-%04x-%04x-%04x-%012x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:16],
	)
}

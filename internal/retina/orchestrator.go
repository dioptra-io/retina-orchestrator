package retina

import (
	"context"
	"fmt"
)

type Config struct {
	// JSONLServerAddress is the listening address of the JSONL server between
	// retina-orchestrator and retina-agent.
	JSONLServerAddress string
	// HTTPServerAddress is the address of the HTTP server used to add
	// directives and stream data.
	HTTPServerAddress string

	// PDPath is the filepath of the file that stores the ProbingDirectives. If
	// the string is empty, then this step is skipped.
	PDPath string
}

type orch struct {
	config *Config

	// scheduler schedules the ProbingDirectives and updates by the responses
	// from ForwardingInfoElements and implements respoinsible probing.
	scheduler *scheduler
}

func NewOrchFromConfig(config *Config) (*orch, error) {
	// TODO: Check config.
	return nil, nil
}

func (o *orch) Run(ctx context.Context) error {
	// 1. Read from the PDFile if it's not nil.

	// 2. Initialize the JSONL and HTTP server.

	// 3. Run the PDScheduler.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		pd, err := o.scheduler.Issue()
		if err != nil {
			if err == ErrExperimentFailed {
				continue
			}
			return err
		}

		// TODO: assign pd to an agent and send.
		fmt.Printf("pd.AgentID: %v\n", pd.AgentID)
	}
}

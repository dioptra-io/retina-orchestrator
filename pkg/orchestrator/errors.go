package orchestrator

import "errors"

var (
	// agent errors
	ErrAgentAlreadyExists = errors.New("agent already exists")
	ErrAgentDoesNotExist  = errors.New("agent does not exist")

	ErrStreamingNotSupported     = errors.New("streaming is not supported")
	ErrAgentIDMissing            = errors.New("agent id missing")
	ErrAlreadyConnected          = errors.New("already connected")
	ErrNotConnected              = errors.New("not connected")
	ErrGeneratorAlreadyConnected = errors.New("generator already connected")
	ErrBroadcasterClosed         = errors.New("broadcaster is closed")
	ErrQueueClosed               = errors.New("queue closed")

	// internal
	ErrAgentInPDDoesNotExist = errors.New("agent id in pd does not exist")
	ErrAgentsQueueFull       = errors.New("agent's queue full")
)

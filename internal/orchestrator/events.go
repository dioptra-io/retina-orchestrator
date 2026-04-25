// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

package orchestrator

import (
	"encoding/json"
	"time"
)

// SSEEventType represents the type of SSE event.
type SSEEventType string

const (
	SSEEventOperatorStarted   SSEEventType = "OperatorStarted"
	SSEEventOperatorStopped   SSEEventType = "OperatorStopped"
	SSEEventAgentConnected    SSEEventType = "AgentConnected"
	SSEEventAgentDisconnected SSEEventType = "AgentDisconnected"
	SSEEventCycleStarted      SSEEventType = "CycleStarted"
	SSEEventCycleFinished     SSEEventType = "CycleFinished"
)

// SSEEvent is a server-sent event with a type, data, and timestamp.
type SSEEvent struct {
	Type      SSEEventType `json:"type"`
	Timestamp time.Time    `json:"timestamp"`
	Data      any          `json:"data,omitempty"`
}

// OperatorStartedData is the payload for OperatorStarted events.
type OperatorStartedData struct{}

// OperatorStoppedData is the payload for OperatorStopped events.
type OperatorStoppedData struct{}

// AgentConnectedData is the payload for AgentConnected events.
type AgentConnectedData struct {
	AgentID string `json:"agent_id"`
}

// AgentDisconnectedData is the payload for AgentDisconnected events.
type AgentDisconnectedData struct {
	AgentID string `json:"agent_id"`
	Reason  string `json:"reason"`
}

// CycleStartedData is the payload for CycleStarted events.
type CycleStartedData struct {
	Cycle int `json:"cycle"`
}

// CycleFinishedData is the payload for CycleFinished events.
type CycleFinishedData struct {
	Cycle  int `json:"cycle"`
	Issued int `json:"issued"`
}

// MarshalJSON implements json.Marshaler for SSEEvent.
func (e *SSEEvent) MarshalJSON() ([]byte, error) {
	type rawSSEEvent struct {
		Type      SSEEventType `json:"type"`
		Timestamp time.Time    `json:"timestamp"`
		Data      any          `json:"data,omitempty"`
	}

	raw := rawSSEEvent{
		Type:      e.Type,
		Timestamp: e.Timestamp,
		Data:      e.Data,
	}

	// Serialize data separately to control the output
	if e.Data != nil {
		dataBytes, err := json.Marshal(e.Data)
		if err != nil {
			return nil, err
		}
		raw.Data = json.RawMessage(dataBytes)
	}

	return json.Marshal(raw)
}

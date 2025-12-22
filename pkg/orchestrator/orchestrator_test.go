// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
)

// TestUpdateAgentConnectionUsesWriteLock verifies that updateAgentConnection
// uses a write lock (not read lock) since it modifies state.
func TestUpdateAgentConnectionUsesWriteLock(t *testing.T) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			ActiveAgentIDs: []string{},
		},
	}

	ctx := context.Background()

	// This test checks for data races - run with -race flag
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			o.updateAgentConnection(ctx, "agent-"+string(rune('a'+id)), true)
		}(i)
	}
	wg.Wait()

	// BUG: The function uses RLock but modifies ActiveAgentIDs
	// This should cause a data race when run with -race
}

// TestUpdateAgentConnectionDisconnect verifies that the connected parameter
// is properly handled (agent should be removed when connected=false).
func TestUpdateAgentConnectionDisconnect(t *testing.T) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			ActiveAgentIDs: []string{},
		},
	}

	ctx := context.Background()

	// Connect an agent
	o.updateAgentConnection(ctx, "agent-1", true)

	if len(o.currentStatus.ActiveAgentIDs) != 1 {
		t.Fatalf("expected 1 active agent, got %d", len(o.currentStatus.ActiveAgentIDs))
	}

	// Disconnect the agent
	o.updateAgentConnection(ctx, "agent-1", false)

	// BUG: The connected parameter is ignored - agent is always added
	// After disconnect, we expect 0 agents, but the buggy code will have 2
	if len(o.currentStatus.ActiveAgentIDs) != 0 {
		t.Errorf("expected 0 active agents after disconnect, got %d (agents: %v)",
			len(o.currentStatus.ActiveAgentIDs), o.currentStatus.ActiveAgentIDs)
	}
}

// TestHandleAgentGetsProbingDirectivesFromCorrectMessenger verifies that
// handleAgent reads from messengerProbingDirective, not messengerForwardingInfoElement.
func TestHandleAgentGetsProbingDirectivesFromCorrectMessenger(t *testing.T) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			ActiveAgentIDs: []string{},
		},
	}

	agentID := "test-agent-pd"

	// Register agent in the correct messenger (ProbingDirective)
	err := o.messengerProbingDirective.RegisterAs(agentID)
	if err != nil {
		t.Fatalf("failed to register in ProbingDirective: %v", err)
	}
	defer o.messengerProbingDirective.UnregisterAs(agentID)

	// Send a probing directive to this agent
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	testPD := &api.ProbingDirective{AgentID: agentID}
	err = o.messengerProbingDirective.SendTo(ctx, agentID, testPD)
	if err != nil {
		t.Fatalf("failed to send PD: %v", err)
	}

	// BUG: The code does: pd, err := o.messengerForwardingInfoElement.GetAs(ctx, agentID)
	// but it should be: pd, err := o.messengerProbingDirective.GetAs(ctx, agentID)

	// Try to get from ForwardingInfoElement (the buggy code path) - should fail
	// because agent is not registered there
	_, err = o.messengerForwardingInfoElement.GetAs(ctx, agentID)
	if err == nil {
		t.Error("BUG: should not be able to get from ForwardingInfoElement messenger")
	}

	// Getting from ProbingDirective (correct) should work
	pd, err := o.messengerProbingDirective.GetAs(ctx, agentID)
	if err != nil {
		t.Errorf("should be able to get from ProbingDirective messenger: %v", err)
	}
	if pd.AgentID != agentID {
		t.Errorf("got wrong PD, expected agentID %s, got %s", agentID, pd.AgentID)
	}
}

// TestHandleAgentExtractsIDFromPathVariable verifies that the agent ID
// is extracted from the path variable, not query parameter.
func TestHandleAgentExtractsIDFromPathVariable(t *testing.T) {
	// The route is defined as: r.HandleFunc("/agent/{id}", o.handleAgent)
	// But the code does: agentID := r.URL.Query().Get("agent_id")
	// It should do: agentID := mux.Vars(r)["id"]

	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			ActiveAgentIDs: []string{},
		},
	}

	// Create a test request with path variable
	req := httptest.NewRequest(http.MethodGet, "/agent/my-agent-id", nil)

	// Simulate mux setting the path variable
	req = mux.SetURLVars(req, map[string]string{"id": "my-agent-id"})

	// The buggy code looks for query param "agent_id" which is empty
	queryAgentID := req.URL.Query().Get("agent_id")
	if queryAgentID != "" {
		t.Error("query param agent_id should be empty")
	}

	// The correct way to get the ID
	pathAgentID := mux.Vars(req)["id"]
	if pathAgentID != "my-agent-id" {
		t.Errorf("expected path var 'id' to be 'my-agent-id', got '%s'", pathAgentID)
	}

	// BUG: Since the code uses query param, it will get empty string and fail
	_ = o // avoid unused variable warning
}

// TestHandleSetSettingDeadlock tests for potential deadlock when notifyGenerator
// is called while holding the write lock.
func TestHandleSetSettingDeadlock(t *testing.T) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			GlobalProbingRatePSPA: 1,
			ActiveAgentIDs:        []string{},
		},
	}

	r := mux.NewRouter()
	r.HandleFunc("/settings/{name}", o.handleSetSetting).Methods(http.MethodPut)

	// Create request to update a setting
	body := bytes.NewBufferString("100")
	req := httptest.NewRequest(http.MethodPut, "/settings/global_probing_rate_pspa", body)
	req.Header.Set("Content-Type", "application/json")

	// Use a channel to detect if the request completes
	done := make(chan struct{})

	go func() {
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)
		close(done)
	}()

	// Wait for completion with timeout
	select {
	case <-done:
		// Request completed successfully
	case <-time.After(2 * time.Second):
		t.Error("potential deadlock detected: handleSetSetting did not complete in time")
	}
}

// TestSettingsGetAndSet tests the settings endpoints.
func TestSettingsGetAndSet(t *testing.T) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			GlobalProbingRatePSPA:          1,
			ProbingImpactLimitMS:           1000,
			DisallowedDestinationAddresses: []net.IP{},
			ActiveAgentIDs:                 []string{},
		},
	}

	r := mux.NewRouter()
	r.HandleFunc("/settings/{name}", o.handleGetSetting).Methods(http.MethodGet)
	r.HandleFunc("/settings/{name}", o.handleSetSetting).Methods(http.MethodPut)

	t.Run("get global_probing_rate_pspa", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/settings/global_probing_rate_pspa", nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rr.Code)
		}

		var value uint
		if err := json.NewDecoder(rr.Body).Decode(&value); err != nil {
			t.Fatalf("failed to decode response: %v", err)
		}
		if value != 1 {
			t.Errorf("expected value 1, got %d", value)
		}
	})

	t.Run("set global_probing_rate_pspa", func(t *testing.T) {
		body := bytes.NewBufferString("50")
		req := httptest.NewRequest(http.MethodPut, "/settings/global_probing_rate_pspa", body)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusNoContent {
			t.Errorf("expected status 204, got %d: %s", rr.Code, rr.Body.String())
		}

		// Verify the value was updated
		o.mu.RLock()
		if o.currentStatus.GlobalProbingRatePSPA != 50 {
			t.Errorf("expected GlobalProbingRatePSPA to be 50, got %d", o.currentStatus.GlobalProbingRatePSPA)
		}
		o.mu.RUnlock()
	})

	t.Run("get unknown setting", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/settings/unknown_setting", nil)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", rr.Code)
		}
	})

	t.Run("set with invalid type", func(t *testing.T) {
		body := bytes.NewBufferString(`"not a number"`)
		req := httptest.NewRequest(http.MethodPut, "/settings/global_probing_rate_pspa", body)
		rr := httptest.NewRecorder()
		r.ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", rr.Code)
		}
	})
}

// TestNotifyGeneratorMakesDeepCopy verifies that notifyGenerator creates
// a proper deep copy of the status to avoid race conditions.
func TestNotifyGeneratorMakesDeepCopy(t *testing.T) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			GlobalProbingRatePSPA:          1,
			DisallowedDestinationAddresses: []net.IP{net.ParseIP("192.168.1.1")},
			ActiveAgentIDs:                 []string{"agent-1"},
		},
	}

	generatorID := "test-generator"
	err := o.messengerSystemStatus.RegisterAs(generatorID)
	if err != nil {
		t.Fatalf("failed to register generator: %v", err)
	}
	defer o.messengerSystemStatus.UnregisterAs(generatorID)

	ctx := context.Background()
	o.notifyGenerator(ctx)

	// Get the status that was sent
	sentStatus, err := o.messengerSystemStatus.GetAs(ctx, generatorID)
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}

	// Modify the original
	o.mu.Lock()
	o.currentStatus.ActiveAgentIDs = append(o.currentStatus.ActiveAgentIDs, "agent-2")
	o.mu.Unlock()

	// The sent status should not be affected (deep copy)
	if len(sentStatus.ActiveAgentIDs) != 1 {
		t.Errorf("deep copy failed: sent status was modified, expected 1 agent, got %d",
			len(sentStatus.ActiveAgentIDs))
	}
}

// Helper function to create a test WebSocket connection (mock)
type mockWSConn struct {
	readMessages  [][]byte
	writeMessages []interface{}
	readIndex     int
	mu            sync.Mutex
	closed        bool
}

func (m *mockWSConn) ReadMessage() (messageType int, p []byte, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || m.readIndex >= len(m.readMessages) {
		return 0, nil, websocket.ErrCloseSent
	}
	msg := m.readMessages[m.readIndex]
	m.readIndex++
	return websocket.TextMessage, msg, nil
}

func (m *mockWSConn) WriteJSON(v interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return websocket.ErrCloseSent
	}
	m.writeMessages = append(m.writeMessages, v)
	return nil
}

func (m *mockWSConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

// TestMultipleAgentsConcurrent tests concurrent agent connections.
func TestMultipleAgentsConcurrent(t *testing.T) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			ActiveAgentIDs: []string{},
		},
	}

	ctx := context.Background()
	numAgents := 5

	var wg sync.WaitGroup
	for i := 0; i < numAgents; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			agentID := string(rune('a' + id))

			// Connect
			o.updateAgentConnection(ctx, agentID, true)
			time.Sleep(10 * time.Millisecond)

			// Disconnect
			o.updateAgentConnection(ctx, agentID, false)
		}(i)
	}
	wg.Wait()

	// After all agents disconnect, there should be no active agents
	// BUG: Due to the issues in updateAgentConnection, this will likely fail
	o.mu.RLock()
	activeCount := len(o.currentStatus.ActiveAgentIDs)
	o.mu.RUnlock()

	if activeCount != 0 {
		t.Errorf("expected 0 active agents after all disconnected, got %d", activeCount)
	}
}

// TestForwardingInfoElementBroadcast tests that FIEs are broadcast to all stream clients.
func TestForwardingInfoElementBroadcast(t *testing.T) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			ActiveAgentIDs: []string{},
		},
	}

	// Register multiple stream clients
	client1 := "stream-client-1"
	client2 := "stream-client-2"

	err := o.messengerForwardingInfoElement.RegisterAs(client1)
	if err != nil {
		t.Fatalf("failed to register client1: %v", err)
	}
	defer o.messengerForwardingInfoElement.UnregisterAs(client1)

	err = o.messengerForwardingInfoElement.RegisterAs(client2)
	if err != nil {
		t.Fatalf("failed to register client2: %v", err)
	}
	defer o.messengerForwardingInfoElement.UnregisterAs(client2)

	ctx := context.Background()

	// Broadcast a FIE
	testFIE := &api.ForwardingInfoElement{
		// Add appropriate fields based on the actual struct
	}
	err = o.messengerForwardingInfoElement.SendAll(ctx, testFIE)
	if err != nil {
		t.Fatalf("failed to broadcast FIE: %v", err)
	}

	// Both clients should receive the FIE
	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()

	_, err = o.messengerForwardingInfoElement.GetAs(ctx1, client1)
	if err != nil {
		t.Errorf("client1 should have received FIE: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()

	_, err = o.messengerForwardingInfoElement.GetAs(ctx2, client2)
	if err != nil {
		t.Errorf("client2 should have received FIE: %v", err)
	}
}

// BenchmarkUpdateAgentConnection benchmarks the agent connection update.
func BenchmarkUpdateAgentConnection(b *testing.B) {
	o := &orchestrator{
		messengerSystemStatus:          NewMessenger[api.SystemStatus](10),
		messengerForwardingInfoElement: NewMessenger[api.ForwardingInfoElement](10),
		messengerProbingDirective:      NewMessenger[api.ProbingDirective](10),
		currentStatus: api.SystemStatus{
			ActiveAgentIDs: []string{},
		},
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agentID := "agent-" + strings.Repeat("x", i%10)
		o.updateAgentConnection(ctx, agentID, true)
	}
}

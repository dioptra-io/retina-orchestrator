// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	api "github.com/dioptra-io/retina-commons/api/v1"
)

// Coverage gaps — unreachable without integration tests or production code changes:
//   - Run, runScheduler, runAPIServer, runAgentServer: require live servers
//   - fieStreamHandler, agentHandler: FIEClient and AgentStream have unexported
//     fields and no constructors accessible from this package
//   - NewOrch: NewQueue, NewRingBuffer, NewAPIServer, NewAgentServer error branches
//     are unreachable — all use hardcoded safe values or non-nil handlers

// -- helpers ------------------------------------------------------------------

func writePDFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "pds-*.jsonl")
	if err != nil {
		t.Fatalf("cannot create temp file: %v", err)
	}
	pd := api.ProbingDirective{ProbingDirectiveID: 1}
	b, _ := json.Marshal(pd)
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
	return f.Name()
}

func validConfig(t *testing.T) *Config {
	t.Helper()
	return &Config{
		AgentAddress:      "127.0.0.1:0",
		AgentBufferLength: 8192,
		APIAddress:        "127.0.0.1:0",
		PDPath:            writePDFile(t),
		Seed:              0,
		IssuanceRate:      1.0,
		ImpactThreshold:   1.0,
		Secret:            "secret",
	}
}

// -- Config.Validate ----------------------------------------------------------

func TestConfig_Validate_Valid(t *testing.T) {
	t.Parallel()
	if err := validConfig(t).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConfig_Validate_DefaultsAPIReadHeaderTimeout(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.APIReadHeaderTimeout = 0
	_ = c.Validate()
	if c.APIReadHeaderTimeout != 5*time.Second {
		t.Errorf("expected default 5s, got %v", c.APIReadHeaderTimeout)
	}
}

func TestConfig_Validate_Errors(t *testing.T) {
	t.Parallel()
	base := validConfig(t)
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"empty AgentAddress", func(c *Config) { c.AgentAddress = "" }},
		{"small AgentBufferLength", func(c *Config) { c.AgentBufferLength = 100 }},
		{"empty APIAddress", func(c *Config) { c.APIAddress = "" }},
		{"empty PDPath", func(c *Config) { c.PDPath = "" }},
		{"zero IssuanceRate", func(c *Config) { c.IssuanceRate = 0 }},
		{"negative IssuanceRate", func(c *Config) { c.IssuanceRate = -1 }},
		{"zero ImpactThreshold", func(c *Config) { c.ImpactThreshold = 0 }},
		{"negative ImpactThreshold", func(c *Config) { c.ImpactThreshold = -1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := *base
			tc.mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("expected error for %q, got nil", tc.name)
			}
		})
	}
}

// -- NewOrch ------------------------------------------------------------------

func TestNewOrch_Valid(t *testing.T) {
	t.Parallel()
	o, err := NewOrch(validConfig(t))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if o == nil {
		t.Fatal("expected non-nil orchestrator")
	}
}

func TestNewOrch_InvalidConfig(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.AgentAddress = ""
	if _, err := NewOrch(c); err == nil {
		t.Fatal("expected error for invalid config, got nil")
	}
}

func TestNewOrch_SchedulerError(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.PDPath = "/nonexistent/path.jsonl"
	if _, err := NewOrch(c); err == nil {
		t.Fatal("expected error for bad PDPath, got nil")
	}
}

// -- agentAuthHandler ---------------------------------------------------------

func TestAgentAuthHandler_ValidSecret(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.Secret = "mysecret"
	o, err := NewOrch(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp := o.agentAuthHandler(api.AuthRequest{Secret: "mysecret"})
	if !resp.Authenticated {
		t.Errorf("expected authenticated, got: %s", resp.Message)
	}
}

func TestAgentAuthHandler_InvalidSecret(t *testing.T) {
	t.Parallel()
	c := validConfig(t)
	c.Secret = "mysecret"
	o, err := NewOrch(c)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp := o.agentAuthHandler(api.AuthRequest{Secret: "wrong"})
	if resp.Authenticated {
		t.Fatal("expected not authenticated")
	}
}

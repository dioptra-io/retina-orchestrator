// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT

// Tests for the retina-orchestrator CLI.
//
// Intentionally uncovered:
//   - main(): thin wrapper around run() with os.Exit; standard practice.
//   - run(): requires full orchestrator startup; covered by integration tests.
//   - startMetricsServer(): goroutine error log after Serve failure —
//     requires closing the listener mid-serve, which is inherently racy.
//   - envOrDefault*: os.Exit(1) on invalid input is untestable without
//     subprocess testing.
//   - shutdown(): requires a live *http.Server; covered indirectly via
//     startMetricsServer tests.

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// -- envOrDefault* ------------------------------------------------------------

func TestEnvOrDefault(t *testing.T) {
	t.Setenv("RETINA_TEST_STR", "hello")
	if got := envOrDefault("RETINA_TEST_STR", "default"); got != "hello" {
		t.Errorf("got %q, want %q", got, "hello")
	}
	if got := envOrDefault("RETINA_TEST_STR_UNSET", "default"); got != "default" {
		t.Errorf("got %q, want %q", got, "default")
	}
}

func TestEnvOrDefaultInt(t *testing.T) {
	t.Setenv("RETINA_TEST_INT", "42")
	if got := envOrDefaultInt("RETINA_TEST_INT", 0); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
	if got := envOrDefaultInt("RETINA_TEST_INT_UNSET", 7); got != 7 {
		t.Errorf("got %d, want 7", got)
	}
}

func TestEnvOrDefaultFloat64(t *testing.T) {
	t.Setenv("RETINA_TEST_FLOAT", "3.14")
	if got := envOrDefaultFloat64("RETINA_TEST_FLOAT", 0); got != 3.14 {
		t.Errorf("got %f, want 3.14", got)
	}
	if got := envOrDefaultFloat64("RETINA_TEST_FLOAT_UNSET", 1.5); got != 1.5 {
		t.Errorf("got %f, want 1.5", got)
	}
}

func TestEnvOrDefaultUInt64(t *testing.T) {
	t.Setenv("RETINA_TEST_UINT", "99")
	if got := envOrDefaultUInt64("RETINA_TEST_UINT", 0); got != 99 {
		t.Errorf("got %d, want 99", got)
	}
	if got := envOrDefaultUInt64("RETINA_TEST_UINT_UNSET", 42); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
}

func TestEnvOrDefaultDuration(t *testing.T) {
	t.Setenv("RETINA_TEST_DUR", "30s")
	if got := envOrDefaultDuration("RETINA_TEST_DUR", 0); got != 30*time.Second {
		t.Errorf("got %v, want 30s", got)
	}
	if got := envOrDefaultDuration("RETINA_TEST_DUR_UNSET", 5*time.Second); got != 5*time.Second {
		t.Errorf("got %v, want 5s", got)
	}
}

// -- newLogger ----------------------------------------------------------------

func TestNewLogger_Levels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     string
		wantLevel slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"error", slog.LevelError},
		{"invalid", slog.LevelInfo}, // fallback to info
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()

			logger := newLogger(tt.input)
			if logger == nil {
				t.Fatal("newLogger returned nil")
			}
			if !logger.Enabled(context.Background(), tt.wantLevel) {
				t.Errorf("newLogger(%q): level %v should be enabled", tt.input, tt.wantLevel)
			}
			if tt.wantLevel > slog.LevelDebug {
				if logger.Enabled(context.Background(), tt.wantLevel-1) {
					t.Errorf("newLogger(%q): level below %v should not be enabled", tt.input, tt.wantLevel)
				}
			}
		})
	}
}

// -- startMetricsServer -------------------------------------------------------

func TestStartMetricsServer(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	registry := prometheus.NewRegistry()
	srv, err := startMetricsServer(testLogger(), registry, addr)
	if err != nil {
		t.Fatalf("startMetricsServer returned unexpected error: %v", err)
	}
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	resp, err := http.Get(fmt.Sprintf("http://%s/metrics", addr)) //nolint:noctx
	if err != nil {
		t.Fatalf("failed to reach metrics server: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("metrics server returned %d, want 200", resp.StatusCode)
	}
}

func TestStartMetricsServer_InvalidAddr(t *testing.T) {
	t.Parallel()

	_, err := startMetricsServer(testLogger(), prometheus.NewRegistry(), "invalid-addr")
	if err == nil {
		t.Error("expected error for invalid address, got nil")
	}
}

func TestShutdown(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	registry := prometheus.NewRegistry()
	srv, err := startMetricsServer(testLogger(), registry, addr)
	if err != nil {
		t.Fatalf("startMetricsServer failed: %v", err)
	}

	shutdown(testLogger(), srv)
}

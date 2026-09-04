// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

// apiClientConfig.url is retina-api's ingest endpoint, e.g.
// "https://retina0.lip6.fr:8090/api/v1/ingest".
type apiClientConfig struct {
	url            string
	bufferSize     int
	reconnectDelay time.Duration
	httpClient     *http.Client
	logger         *slog.Logger
	metrics        *Metrics
}

// apiClient pushes FIEs to retina-api over a single long-lived NDJSON POST
// connection, reconnecting on failure. No sequence numbers are assigned
// here — retina-api assigns those per subscriber on the way out.
type apiClient struct {
	config  *apiClientConfig
	fieChan chan *api.ForwardingInfoElement
}

// newAPIClient trusts url/bufferSize/reconnectDelay to already be valid —
// Config.Validate() is the single place those are checked, since NewOrch
// is the only real caller. What's left here is what Validate() can't cover:
// defaults for fields outside Config, and metrics as a required dependency.
func newAPIClient(config *apiClientConfig) (*apiClient, error) {
	if config.httpClient == nil {
		config.httpClient = &http.Client{}
	}
	if config.logger == nil {
		config.logger = slog.Default()
	}
	if config.metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}
	return &apiClient{
		config:  config,
		fieChan: make(chan *api.ForwardingInfoElement, config.bufferSize),
	}, nil
}

// push is non-blocking: a full buffer means the connection is down or slow,
// so the FIE is dropped and counted rather than stalling the caller. A nil
// FIE is also dropped and counted rather than being encoded as JSON null.
func (c *apiClient) push(fie *api.ForwardingInfoElement) {
	if fie == nil {
		c.config.metrics.APIClientFIEsDroppedTotal.Inc()
		return
	}
	select {
	case c.fieChan <- fie:
	default:
		c.config.metrics.APIClientFIEsDroppedTotal.Inc()
	}
}

// run drives the reconnect loop; start it in its own goroutine. Fixed
// retry delay, not backoff — not worth it for two orchestrators total.
func (c *apiClient) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.streamOnce(ctx); err != nil && ctx.Err() == nil {
			c.config.logger.Error("Lost connection to retina-api, retrying",
				slog.String("error", err.Error()),
				slog.Duration("retry_in", c.config.reconnectDelay))
		}
		select {
		case <-time.After(c.config.reconnectDelay):
		case <-ctx.Done():
			return
		}
	}
}

// streamOnce opens one POST connection and encodes FIEs into it as NDJSON
// as they arrive, until the connection drops or ctx is canceled.
//
// retina-api's ingest handler doesn't respond until the connection ends
// (it's a long-lived stream, not a request/response exchange), so Do()
// normally only returns once something has gone wrong — deliberately not
// inspecting a response here: a server replying while the request body is
// still being streamed is a fragile pattern in Go's http stack (an early
// response can race with the still-open body write in ways that are hard
// to reason about), so retina-api and this client both avoid it rather
// than trying to make it work.
func (c *apiClient) streamOnce(ctx context.Context) error {
	pr, pw := io.Pipe()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.config.url, pr)
	if err != nil {
		pw.Close()
		return fmt.Errorf("failed to build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	respCh := make(chan error, 1)
	go func() {
		resp, err := c.config.httpClient.Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		respCh <- err
	}()

	c.config.metrics.APIClientConnectionUp.Set(1)
	defer c.config.metrics.APIClientConnectionUp.Set(0)
	c.config.logger.Info("Connected to retina-api", slog.String("url", c.config.url))

	enc := json.NewEncoder(pw)
	for {
		select {
		case fie := <-c.fieChan:
			if err := enc.Encode(fie); err != nil {
				pw.CloseWithError(err)
				return fmt.Errorf("failed to encode FIE: %w", err)
			}
		case err := <-respCh:
			pw.Close()
			if err != nil {
				return fmt.Errorf("retina-api connection failed: %w", err)
			}
			return fmt.Errorf("retina-api connection closed unexpectedly")
		case <-ctx.Done():
			pw.Close()
			return nil
		}
	}
}

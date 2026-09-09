// Copyright (c) 2025 Sorbonne Université
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/dioptra-io/retina-commons/framing"
	"github.com/dioptra-io/retina-commons/model"
)

// apiClientKeepalivePeriod matches agentKeepalivePeriod (agent_server.go).
const apiClientKeepalivePeriod = 10 * time.Second

// apiClientConfig.address is retina-api's ingest listener address, e.g.
// "retina0.lip6.fr:8123".
type apiClientConfig struct {
	address        string
	bufferSize     int
	reconnectDelay time.Duration
	// sendTimeout is the deadline for sending one FIE. Defaults to 5s if zero.
	sendTimeout time.Duration
	logger      *slog.Logger
	metrics     *Metrics
}

// apiClient pushes FIEs to retina-api over one long-lived TCP connection,
// reconnecting on failure. Sequence numbers are assigned by retina-api,
// not here.
type apiClient struct {
	config  *apiClientConfig
	fieChan chan *model.ForwardingInfoElement
}

// newAPIClient trusts address/bufferSize/reconnectDelay as already valid
// (Config.Validate() owns that); it only defaults sendTimeout/logger and
// requires metrics.
func newAPIClient(config *apiClientConfig) (*apiClient, error) {
	if config.sendTimeout <= 0 {
		config.sendTimeout = 5 * time.Second
	}
	if config.logger == nil {
		config.logger = slog.Default()
	}
	if config.metrics == nil {
		return nil, fmt.Errorf("metrics cannot be nil")
	}
	return &apiClient{
		config:  config,
		fieChan: make(chan *model.ForwardingInfoElement, config.bufferSize),
	}, nil
}

// push is non-blocking: a full buffer means the connection is down or
// slow, so the FIE is dropped and counted rather than stalling the
// caller. A nil FIE is also dropped and counted.
func (c *apiClient) push(fie *model.ForwardingInfoElement) {
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

// run drives the reconnect loop; start it in its own goroutine.
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

// streamOnce dials retina-api and sends FIEs as length-prefixed protobuf
// (retina-commons/framing).
func (c *apiClient) streamOnce(ctx context.Context) error {
	dialer := net.Dialer{}
	rawConn, err := dialer.DialContext(ctx, "tcp", c.config.address)
	if err != nil {
		return fmt.Errorf("failed to connect to retina-api: %w", err)
	}
	conn, ok := rawConn.(*net.TCPConn)
	if !ok {
		rawConn.Close()
		return fmt.Errorf("expected TCP connection, got %T", rawConn)
	}
	defer conn.Close()

	if err := conn.SetKeepAlive(true); err != nil {
		return fmt.Errorf("failed to enable keepalive: %w", err)
	}
	if err := conn.SetKeepAlivePeriod(apiClientKeepalivePeriod); err != nil {
		return fmt.Errorf("failed to set keepalive period: %w", err)
	}

	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()

	c.config.metrics.APIClientConnectionUp.Set(1)
	defer c.config.metrics.APIClientConnectionUp.Set(0)
	c.config.logger.Info("Connected to retina-api", slog.String("address", c.config.address))

	for {
		select {
		case fie := <-c.fieChan:
			wireFIE, err := fie.ToProto()
			if err != nil {
				c.config.logger.Error("Dropping FIE: failed to convert to wire format",
					slog.String("error", err.Error()))
				c.config.metrics.APIClientFIEsDroppedTotal.Inc()
				continue
			}
			// AfterFunc (above) closes conn on cancellation, which
			// unblocks this Send — ctx.Err() then tells a clean
			// shutdown apart from a genuine failure.
			if err := framing.Send(conn, c.config.sendTimeout, wireFIE); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("failed to send FIE: %w", err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

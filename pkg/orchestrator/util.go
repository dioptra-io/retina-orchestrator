// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"errors"
	"net/http"

	"github.com/gorilla/websocket"
)

var (
	// ErrDisconnected indicates that a WebSocket connection was closed unexpectedly. This error is returned when read
	// or write operations fail due to connection loss.
	ErrDisconnected = errors.New("disconnected")

	// ErrStreamingNotSupported indicates that the client does not support. Server-Sent Events (SSE), typically because
	// the ResponseWriter does not implement http.Flusher.
	ErrStreamingNotSupported = errors.New("streaming not supported")
)

// upgradeToWS upgrades an incoming HTTP request to a WebSocket connection.
//
// It uses Gorilla's Upgrader and (currently) accepts all origins via CheckOrigin. That is convenient for dev, but
// unsafe for production—restrict origins there.
//
// Returns ErrStreamingNotSupported if the upgrade fails (e.g., client/server/proxy doesn't support WebSockets).
func upgradeToWS(w http.ResponseWriter, r *http.Request) (*websocket.Conn, error) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // tighten in prod
		},
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return nil, ErrStreamingNotSupported
	}
	return conn, nil
}

// upgradeToSSE prepares the ResponseWriter for Server-Sent Events (SSE).
//
// It sets the required streaming headers and returns an http.Flusher used to flush events to the client as they are
// written. If the ResponseWriter doesn't implement http.Flusher, streaming isn't possible and ErrStreamingNotSupported
// is returned.
func upgradeToSSE(w http.ResponseWriter, _ *http.Request) (http.Flusher, error) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, ErrStreamingNotSupported
	}
	return flusher, nil
}

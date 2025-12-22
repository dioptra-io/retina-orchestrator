// Copyright (c) 2025 Dioptra
// SPDX-License-Identifier: MIT
package orchestrator

import (
	"errors"
	"math"
	"net"
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

// castJSONValueToUInt is used my the set settings handler, it expects a decoded value from a json reader. Since the JSON decoder
// returns float64 for all number types this is the type we expects. Do not use this function somewhere else.
func castJSONValueToUInt(v any) (uint, bool) {
	f, ok := v.(float64)
	if !ok {
		return 0, false
	}

	// Must be non-negative and integral
	if f < 0 || f != math.Trunc(f) {
		return 0, false
	}

	// Protect against overflow on 32-bit systems
	if f > float64(^uint(0)) {
		return 0, false
	}

	return uint(f), true
}

// castJSONValueToIPArray casets the JSON decoded value to an array of IP addresses, it expects to receive a JSON array of
// strings, where each string is an IP address, IPv4, IPv4-mapped IPv6 or IPv6. Do not use this function somewhere else.
func castJSONValueToIPArray(v any) ([]net.IP, bool) {
	raw, ok := v.([]any)
	if !ok {
		return nil, false
	}

	addrs := make([]net.IP, 0, len(raw))
	for _, elem := range raw {
		s, ok := elem.(string)
		if !ok {
			return nil, false
		}

		ip := net.ParseIP(s)
		if ip == nil {
			return nil, false
		}

		addrs = append(addrs, ip)
	}

	return addrs, true
}

package orchestrator

import (
	"net/http"

	"github.com/gorilla/websocket"
)

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

func getAgentID(w http.ResponseWriter, r *http.Request) (string, error) {
	// agentID := r.Header.Get("agent_id")
	agentID := r.URL.Query().Get("agent_id")
	if agentID == "" {
		return "", ErrAgentIDMissing
	}

	return agentID, nil
}

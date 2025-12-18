package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"github.com/gorilla/websocket"
)

func main() {
	agentID := "agent-1"
	if len(os.Args) > 1 {
		agentID = os.Args[1]
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	url := "ws://localhost:50050/agent?agent_id=" + agentID
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	log.Printf("connected as %s", agentID)

	go func() {
		<-ctx.Done()
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Printf("read error: %v", err)
			return
		}

		var pd api.ProbingDirective
		if err := json.Unmarshal(msg, &pd); err != nil {
			log.Printf("unmarshal error: %v", err)
			continue
		}

		log.Printf("received directive: dst=%s ttl=%d", pd.DestinationAddress, pd.NearTTL)

		// Simulate probing and send back a result
		fie := &api.ForwardingInfoElement{
			Agent:              api.Agent{AgentID: agentID},
			IPVersion:          pd.IPVersion,
			Protocol:           pd.Protocol,
			SourceAddress:      net.ParseIP("192.168.1.1"),
			DestinationAddress: pd.DestinationAddress,
			NearInfo: api.Info{
				TTL:               pd.NearTTL,
				ReplyAddress:      net.ParseIP("10.0.0.1"),
				PayloadSize:       64,
				SentTimestamp:     time.Now(),
				ReceivedTimestamp: time.Now().Add(5 * time.Millisecond),
			},
			FarInfo: api.Info{
				TTL:               pd.NearTTL + 1,
				ReplyAddress:      pd.DestinationAddress,
				PayloadSize:       64,
				SentTimestamp:     time.Now(),
				ReceivedTimestamp: time.Now().Add(10 * time.Millisecond),
			},
			ProductionTimestamp: time.Now(),
		}

		if err := conn.WriteJSON(fie); err != nil {
			log.Printf("write error: %v", err)
			return
		}

		log.Printf("sent result for dst=%s", pd.DestinationAddress)
	}
}

package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"github.com/gorilla/websocket"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	url := "ws://localhost:50050/generator"
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	log.Println("connected as generator")

	// Read settings updates
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				log.Printf("read error: %v", err)
				return
			}
			log.Printf("received settings: %s", string(msg))
		}
	}()

	go func() {
		<-ctx.Done()
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}()

	// Send probing directives periodically
	targets := []string{"8.8.8.8", "1.1.1.1", "9.9.9.9"}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pd := &api.ProbingDirective{
				IPVersion:          api.TypeIPv4,
				Protocol:           api.ICMP,
				AgentID:            "agent-1",
				DestinationAddress: net.ParseIP(targets[i%len(targets)]),
				NearTTL:            uint8(10 + i%20),
				NextHeader: api.NextHeader{
					ICMPNextHeader: &api.ICMPNextHeader{
						FirstHalfWord:  8,
						SecondHalfWord: 0,
					},
				},
			}

			if err := conn.WriteJSON(pd); err != nil {
				log.Printf("write error: %v", err)
				return
			}

			log.Printf("sent directive: agent=%s dst=%s ttl=%d",
				pd.AgentID, pd.DestinationAddress, pd.NearTTL)
			i++
		}
	}
}

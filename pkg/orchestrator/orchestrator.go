package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/dioptra-io/retina-commons/pkg/api/v1"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

type settings struct {
	ProbingRate uint `json:"probing_rate"`
}

type orchestrator struct {
	connectedAgents     *set[string]
	connectedClients    *set[string]
	connectedGenerators *set[string]
	agentQueue          *queue[*api.ProbingDirective]
	clientBroadcaster   *broadcaster[*api.ForwardingInfoElement]
	settingsBroadcaster *broadcaster[*settings]

	httpServer *http.Server
}

func Run(parent context.Context, address string) error {
	g, ctx := errgroup.WithContext(parent)

	mux := http.NewServeMux()
	o := &orchestrator{
		connectedAgents:     newSet[string](),
		connectedClients:    newSet[string](),
		connectedGenerators: newSet[string](),
		agentQueue:          newQueue[*api.ProbingDirective](ctx, 1024),
		clientBroadcaster:   newBroadcaster[*api.ForwardingInfoElement](ctx, true),
		settingsBroadcaster: newBroadcaster[*settings](ctx, true),
		httpServer: &http.Server{
			Addr:    address,
			Handler: mux,
			BaseContext: func(_ net.Listener) context.Context {
				return ctx
			},
		},
	}

	mux.HandleFunc("/generator", o.handleGenerator)
	mux.HandleFunc("/agent", o.handleAgent)
	mux.HandleFunc("/stream", o.handleStream)

	g.Go(func() error {
		log.Printf("orchestrator listening on %s", o.httpServer.Addr)

		if err := o.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			return err
		}
		return nil
	})

	g.Go(func() error {
		<-ctx.Done()

		log.Println("orchestrator shutting down")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		return o.httpServer.Shutdown(shutdownCtx)
	})

	return g.Wait()
}

func (o *orchestrator) handleGenerator(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWS(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	generatorID := uuid.New().String()
	o.connectedGenerators.Add(generatorID)
	defer o.connectedGenerators.Remove(generatorID)
	log.Printf("generator %s connected", generatorID)

	g, ctx := errgroup.WithContext(r.Context())

	g.Go(func() error {
		settingsChan, err := o.settingsBroadcaster.Subscribe(ctx, 1024)
		if err != nil {
			return err
		}

		for {
			select {
			case <-ctx.Done():
				return nil

			case settings, ok := <-settingsChan:
				if !ok {
					return nil
				}

				if err := conn.WriteJSON(settings); err != nil {
					return err
				}
			}
		}
	})

	g.Go(func() error {
		for {
			_, pdJSONString, err := conn.ReadMessage()
			if err != nil {
				return err
			}

			pd := new(api.ProbingDirective)
			if err := json.Unmarshal(pdJSONString, pd); err != nil {
				return err
			}

			if !o.connectedAgents.Contains(pd.AgentID) {
				log.Printf("ignoring the probing directive with an unexisting agent id: %s", pd.AgentID)
				continue
			}

			if err := o.agentQueue.Push(ctx, pd.AgentID, pd); err != nil {
				return err
			}
		}
	})

	if err := g.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		log.Printf("generator %s disconnected with error: %v", generatorID, err)
		return
	}

	log.Printf("generator %s disconnected", generatorID)
}

func (o *orchestrator) handleAgent(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWS(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	agentID, err := getAgentID(w, r)
	if err != nil {
		return
	}

	if ok := o.connectedAgents.Add(agentID); !ok {
		return
	}
	defer o.connectedAgents.Remove(agentID)
	log.Printf("agent %s connected", agentID)

	g, ctx := errgroup.WithContext(r.Context())

	g.Go(func() error {
		for {
			pd, err := o.agentQueue.Get(ctx, agentID)
			if err != nil {
				return err
			}

			if err := conn.WriteJSON(pd); err != nil {
				return err
			}
		}
	})

	g.Go(func() error {
		for {
			_, fieJSONString, err := conn.ReadMessage()
			if err != nil {
				return err
			}

			fie := new(api.ForwardingInfoElement)
			if err := json.Unmarshal(fieJSONString, fie); err != nil {
				return err
			}

			if err := o.clientBroadcaster.Broadcast(ctx, fie); err != nil {
				return err
			}
		}
	})

	if err := g.Wait(); err != nil && !errors.Is(err, ctx.Err()) {
		log.Printf("agent %s disconnected with error: %v", agentID, err)
		return
	}

	log.Printf("agent %s disconnected", agentID)
}

func (o *orchestrator) handleStream(w http.ResponseWriter, r *http.Request) {
	f, err := upgradeToSSE(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	clientID := uuid.New().String()

	if ok := o.connectedClients.Add(clientID); !ok {
		return
	}
	defer o.connectedClients.Remove(clientID)
	defer log.Printf("client %s disconnected", clientID)
	log.Printf("client %s connected", clientID)

	ctx := r.Context()

	clientChan, err := o.clientBroadcaster.Subscribe(ctx, 1024)
	if err != nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case fie, ok := <-clientChan:
			if !ok {
				return
			}

			fieJSONBytes, err := json.Marshal(fie)
			if err != nil {
				return
			}

			fmt.Fprintf(w, "%s\n", string(fieJSONBytes))
			f.Flush()
		}
	}

}

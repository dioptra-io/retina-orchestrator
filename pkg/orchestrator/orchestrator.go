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
	settings "github.com/dioptra-io/retina-orchestrator/pkg/api"
	"github.com/dioptra-io/retina-orchestrator/pkg/queue"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

var (
	ErrDisconnected          = errors.New("disconnected")
	ErrStreamingNotSupported = errors.New("streaming not supported")
)

type orchestrator struct {
	pdQueue       *queue.Queue[api.ProbingDirective]
	fieQueue      *queue.Queue[api.ForwardingInfoElement]
	settingsQueue *queue.Queue[settings.Settings]
	httpServer    *http.Server
}

func Run(parent context.Context, address string) error {
	g, ctx := errgroup.WithContext(parent)

	mux := http.NewServeMux()
	o := &orchestrator{
		pdQueue:       queue.NewQueue[api.ProbingDirective](1024),
		settingsQueue: queue.NewQueue[settings.Settings](1024),
		fieQueue:      queue.NewQueue[api.ForwardingInfoElement](1024),
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
	if err := o.settingsQueue.SubscribeWithID(generatorID); err != nil {
		log.Printf("failed to register generator %s to settings queue: %v", generatorID, err)
		return
	}
	defer o.settingsQueue.Unsubscribe(generatorID)

	log.Printf("generator %s connected", generatorID)

	g, ctx := errgroup.WithContext(r.Context())

	g.Go(func() error {
		for {
			settings, err := o.settingsQueue.Get(ctx, generatorID)
			if err != nil {
				return err
			}

			if err := conn.WriteJSON(settings); err != nil {
				return ErrDisconnected
			}
		}
	})

	g.Go(func() error {
		for {
			_, pdJSONString, err := conn.ReadMessage()
			if err != nil {
				return ErrDisconnected
			}

			pd := new(api.ProbingDirective)
			if err := json.Unmarshal(pdJSONString, pd); err != nil {
				return err
			}

			if err := o.pdQueue.Whisper(ctx, pd.AgentID, pd); err != nil {
				if err == queue.ErrSubscriberDoesNotExist {
					log.Printf("generator %s generated pd for a non existing agent, ignoring...", generatorID)
					continue
				}
				return err
			}
		}
	})

	if err := g.Wait(); err != nil && err != ctx.Err() && err != ErrDisconnected {
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

	agentID := r.URL.Query().Get("agent_id")
	if agentID == "" {
		log.Println("agent without an agent_id tried to establish connection.")
		return
	}

	if err := o.pdQueue.SubscribeWithID(agentID); err != nil {
		log.Printf("failed to register agent %s to pd queue: %v", agentID, err)
		return
	}
	defer o.pdQueue.Unsubscribe(agentID)
	log.Printf("agent %s connected", agentID)

	g, ctx := errgroup.WithContext(r.Context())

	g.Go(func() error {
		for {
			pd, err := o.pdQueue.Get(ctx, agentID)
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
				return ErrDisconnected
			}

			fie := new(api.ForwardingInfoElement)
			if err := json.Unmarshal(fieJSONString, fie); err != nil {
				return err
			}

			if err := o.fieQueue.Broadcast(ctx, fie); err != nil {
				return err
			}
		}
	})

	if err := g.Wait(); err != nil && err != ctx.Err() && err != ErrDisconnected {
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

	if err := o.fieQueue.SubscribeWithID(clientID); err != nil {
		log.Printf("failed to register client %s to fie queue: %v", clientID, err)
		return
	}
	defer o.fieQueue.Unsubscribe(clientID)
	log.Printf("client %s connected", clientID)

	g, ctx := errgroup.WithContext(r.Context())

	g.Go(func() error {
		for {
			fie, err := o.fieQueue.Get(ctx, clientID)
			if err != nil {
				return err
			}

			fieJSONBytes, err := json.Marshal(fie)
			if err != nil {
				return err
			}

			fmt.Fprintf(w, "%s\n", string(fieJSONBytes))
			f.Flush()
		}
	})

	if err := g.Wait(); err != nil && err != ctx.Err() {
		log.Printf("client %s disconnected with error: %v", clientID, err)
		return
	}

	log.Printf("client %s disconnected", clientID)
}

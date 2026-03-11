package retina

import (
	"context"
	"fmt"
	"net"
)

type jsonlServer struct {
	listenAddress string
}

func newJSONLServer(listenAddress string) (*jsonlServer, error) {
	return &jsonlServer{
		listenAddress: listenAddress,
	}, nil
}

func (s *jsonlServer) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.listenAddress)
	if err != nil {
		return fmt.Errorf("jsonl server listen error: %w", err)
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf("jsonl server accept error: %w", err)
			}
		}
		go s.handleConn(ctx, conn)
	}
}

func (s *jsonlServer) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	// TODO: decode JSONL ForwardingInfoElements and call s.scheduler.Update
}

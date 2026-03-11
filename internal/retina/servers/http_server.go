package servers

import (
	"net"
	"sync"
	"time"
)

type OrchHTTPServer struct {
	// TCPBufferLength is the size if the tcp read and write buffer.
	TCPBufferLength int
	// TCPTimeout is the timeout duration.
	TCPTimeout time.Duration
	// Address is the listening address of the tcp socket.
	Address string

	// streamHandler is the function called for each new connection.
	streamHandler StreamHandleFunc[S, R]
	// authHandler is the function called for each new connection's
	// authentification handling.
	authHandler AuthHandleFunc
	// closed indicates if the server has been shut down.
	closed bool
	// mutex protects the server state.
	mutex sync.Mutex
	// activeStreamers tracks all active connections for shutdown.
	activeStreamers map[int]*JSONLStream[S, R]
	// listener is the listener that listens for the incoming connections.
	listener net.Listener
	// nextID is used to keep track of the JSONLStreamer.
	nextID int
	// closeWG is the waiting group that is used to wait on shutdown.
	closeWG sync.WaitGroup
}

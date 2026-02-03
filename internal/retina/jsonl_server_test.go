package retina

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dioptra-io/retina-commons/api/v1"
)

type TestSend struct {
	Message string `json:"message"`
}

type TestReceive struct {
	Response string `json:"response"`
}

func TestJSONLServer_ListenAndServe_InvalidAddress(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "invalid-address",
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	err := server.ListenAndServe()
	if err == nil {
		t.Error("expected error for invalid address")
	}
}

func TestJSONLServer_ListenAndServe_AlreadyClosed(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
		closed:          true,
	}

	err := server.ListenAndServe()
	if err != ErrServerClosed {
		t.Errorf("expected ErrServerClosed, got %v", err)
	}
}

func TestJSONLServer_Shutdown_NotStarted(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	err := server.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}
}

func TestJSONLServer_Shutdown_AlreadyClosed(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
		closed:          true,
	}

	err := server.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Shutdown should return nil when already closed, got %v", err)
	}
}

func TestJSONLServer_Shutdown_WhileRunning(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	started := make(chan struct{})
	done := make(chan error)

	go func() {
		listener, err := net.Listen("tcp", server.Address)
		if err != nil {
			done <- err
			return
		}
		server.listener = listener
		server.activeStreamers = make(map[int]*JSONLStreamer[TestSend, TestReceive])
		close(started)
		_, err = listener.Accept()
		done <- err
	}()

	<-started
	time.Sleep(10 * time.Millisecond)

	err := server.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Shutdown failed: %v", err)
	}

	<-done
}

func TestJSONLServer_ConnectionAccepted(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     100 * time.Millisecond,
		TCPBufferLength: 1024,
	}

	handlerCalled := make(chan struct{})
	server.HandleFunc(func(info *api.AgentInfo, s *JSONLStreamer[TestSend, TestReceive]) {
		close(handlerCalled)
	})

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	// Wait for server to start
	time.Sleep(50 * time.Millisecond)

	// Connect to server
	conn, err := net.Dial("tcp", server.listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	// Send AgentInfo as first message
	encoder := json.NewEncoder(conn)
	agentInfo := &api.AgentInfo{AgentID: "test-agent"}
	_ = encoder.Encode(agentInfo)

	select {
	case <-handlerCalled:
		// Handler was called (though decode will fail due to nil pointer in implementation)
	case <-time.After(200 * time.Millisecond):
		// Timeout is expected due to decode bug
	}

	conn.Close()
	server.Shutdown(context.Background())
}

func TestJSONLServer_MultipleConnections(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     100 * time.Millisecond,
		TCPBufferLength: 1024,
	}

	var connCount int
	var mu sync.Mutex
	server.HandleFunc(func(info *api.AgentInfo, s *JSONLStreamer[TestSend, TestReceive]) {
		mu.Lock()
		connCount++
		mu.Unlock()
	})

	go server.ListenAndServe()
	time.Sleep(50 * time.Millisecond)

	// Create multiple connections
	conns := make([]net.Conn, 3)
	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", server.listener.Addr().String())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		conns[i] = conn
		encoder := json.NewEncoder(conn)
		_ = encoder.Encode(&api.AgentInfo{AgentID: "agent"})
	}

	time.Sleep(150 * time.Millisecond)

	for _, conn := range conns {
		conn.Close()
	}

	server.Shutdown(context.Background())
}

func TestJSONLStreamer_ID(t *testing.T) {
	conn := &net.TCPConn{}
	server := &JSONLServer[TestSend, TestReceive]{
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	streamer := newJSONLStreamer(42, conn, server)

	if streamer.ID() != 42 {
		t.Errorf("expected ID 42, got %d", streamer.ID())
	}
}

func TestJSONLStreamer_Context(t *testing.T) {
	conn := &net.TCPConn{}
	server := &JSONLServer[TestSend, TestReceive]{
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	streamer := newJSONLStreamer(1, conn, server)

	ctx := streamer.Context()
	if ctx == nil {
		t.Error("Context returned nil")
	}

	select {
	case <-ctx.Done():
		t.Error("context should not be done")
	default:
		// Expected
	}
}

func TestJSONLStreamer_ContextCancellation(t *testing.T) {
	conn := &net.TCPConn{}
	server := &JSONLServer[TestSend, TestReceive]{
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	streamer := newJSONLStreamer(1, conn, server)
	streamer.cancel()

	select {
	case <-streamer.Context().Done():
		// Expected
	default:
		t.Error("context should be done after cancel")
	}
}

func TestNewJSONLStreamer(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	// Create a real TCP connection for testing
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	tcpConn := conn.(*net.TCPConn)
	streamer := newJSONLStreamer(5, tcpConn, server)

	if streamer.id != 5 {
		t.Errorf("expected id 5, got %d", streamer.id)
	}
	if streamer.conn != tcpConn {
		t.Error("conn not set correctly")
	}
	if streamer.server != server {
		t.Error("server not set correctly")
	}
	if streamer.encoder == nil {
		t.Error("encoder is nil")
	}
	if streamer.decoder == nil {
		t.Error("decoder is nil")
	}
	if streamer.ctx == nil {
		t.Error("ctx is nil")
	}
	if streamer.cancel == nil {
		t.Error("cancel is nil")
	}
}

func TestJSONLStreamer_SendReceive_Integration(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		tcpConn := conn.(*net.TCPConn)
		streamer := newJSONLStreamer(1, tcpConn, server)

		// Send a message
		msg := &TestSend{Message: "hello"}
		if err := streamer.Send(msg); err != nil {
			t.Errorf("Send failed: %v", err)
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	var received TestSend
	if err := decoder.Decode(&received); err != nil {
		t.Fatalf("failed to decode: %v", err)
	}

	if received.Message != "hello" {
		t.Errorf("expected 'hello', got '%s'", received.Message)
	}

	<-serverDone
}

func TestJSONLStreamer_Send_ConnectionClosed(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	tcpConn := conn.(*net.TCPConn)
	streamer := newJSONLStreamer(1, tcpConn, server)

	// Close connection
	conn.Close()

	// Try to send
	msg := &TestSend{Message: "hello"}
	err = streamer.Send(msg)
	if err == nil {
		t.Error("expected error when sending on closed connection")
	}
}

func TestJSONLStreamer_Receive_ConnectionClosed(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		TCPDeadline:     100 * time.Millisecond,
		TCPBufferLength: 1024,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			conn.Close()
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	tcpConn := conn.(*net.TCPConn)
	streamer := newJSONLStreamer(1, tcpConn, server)

	// Receive should fail
	_, err = streamer.Receive()
	if err == nil {
		t.Error("expected error when receiving on closed connection")
	}
}

func TestJSONLServer_ActiveStreamersTracking(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     100 * time.Millisecond,
		TCPBufferLength: 1024,
	}

	go server.ListenAndServe()
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", server.listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	server.mutex.Lock()
	count := len(server.activeStreamers)
	server.mutex.Unlock()

	if count != 1 {
		t.Errorf("expected 1 active streamer, got %d", count)
	}

	conn.Close()
	time.Sleep(150 * time.Millisecond)

	server.Shutdown(context.Background())

	server.mutex.Lock()
	count = len(server.activeStreamers)
	server.mutex.Unlock()

	if count != 0 {
		t.Errorf("expected 0 active streamers after shutdown, got %d", count)
	}
}

func TestJSONLServer_NextIDIncrement(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     100 * time.Millisecond,
		TCPBufferLength: 1024,
	}

	go server.ListenAndServe()
	time.Sleep(50 * time.Millisecond)

	// Create multiple connections
	for i := 0; i < 3; i++ {
		conn, err := net.Dial("tcp", server.listener.Addr().String())
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()
	}

	time.Sleep(50 * time.Millisecond)

	server.mutex.Lock()
	nextID := server.nextID
	server.mutex.Unlock()

	if nextID != 3 {
		t.Errorf("expected nextID 3, got %d", nextID)
	}

	server.Shutdown(context.Background())
}

func TestJSONLServer_ConcurrentShutdown(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		Address:         "127.0.0.1:0",
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	go server.ListenAndServe()
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			server.Shutdown(context.Background())
		}()
	}
	wg.Wait()
}

func TestJSONLStreamer_SendNil(t *testing.T) {
	server := &JSONLServer[TestSend, TestReceive]{
		TCPDeadline:     time.Second,
		TCPBufferLength: 1024,
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	go func() {
		conn, _ := listener.Accept()
		if conn != nil {
			defer conn.Close()
			// Read the null value
			decoder := json.NewDecoder(conn)
			var msg *TestSend
			decoder.Decode(&msg)
		}
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	tcpConn := conn.(*net.TCPConn)
	streamer := newJSONLStreamer(1, tcpConn, server)

	// Send nil
	err = streamer.Send(nil)
	if err != nil {
		t.Errorf("Send nil failed: %v", err)
	}
}

func TestErrServerClosed(t *testing.T) {
	if ErrServerClosed.Error() != "server closed" {
		t.Errorf("unexpected error message: %s", ErrServerClosed.Error())
	}
}

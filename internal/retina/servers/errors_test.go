package servers

import (
	"testing"
	"time"
)

func TestAgentServer_ErrServerShutdown_ListenBeforeShutdown(t *testing.T) {
	s, _ := newTestServer(t, allowAll, nopAgent)

	// Shutdown before calling ListenAndServe
	_ = s.Shutdown(time.Second)

	err := s.ListenAndServe()
	if err != ErrServerShutdown {
		t.Fatalf("expected ErrServerShutdown, got %v", err)
	}
}

func TestAgentServer_ErrServerShutdown_AfterShutdown(t *testing.T) {
	s, _ := newTestServer(t, allowAll, nopAgent)

	done := make(chan error, 1)
	go func() {
		done <- s.ListenAndServe()
	}()
	time.Sleep(20 * time.Millisecond)

	// Trigger shutdown
	_ = s.Shutdown(time.Second)

	select {
	case err := <-done:
		if err != ErrServerShutdown {
			t.Fatalf("expected ErrServerShutdown after shutdown, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ListenAndServe did not return after shutdown")
	}
}

package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRegisterAndUnregister(t *testing.T) {
	m := NewMessenger[string](100)

	// Register should succeed
	if err := m.RegisterAs("consumer1"); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// Duplicate register should fail
	if err := m.RegisterAs("consumer1"); err != ErrIDAlreadyExist {
		t.Fatalf("expected ErrIDAlreadyExist, got %v", err)
	}

	// Unregister
	m.UnregisterAs("consumer1")

	// Re-register should succeed
	if err := m.RegisterAs("consumer1"); err != nil {
		t.Fatalf("expected no error after unregister, got %v", err)
	}
}

func TestSendAndReceive(t *testing.T) {
	m := NewMessenger[string](100)
	ctx := context.Background()

	if err := m.RegisterAs("consumer1"); err != nil {
		t.Fatal(err)
	}

	msg := "hello"
	if err := m.SendTo(ctx, "consumer1", &msg); err != nil {
		t.Fatalf("SendTo failed: %v", err)
	}

	received, err := m.GetAs(ctx, "consumer1")
	if err != nil {
		t.Fatalf("GetAs failed: %v", err)
	}

	if *received != msg {
		t.Fatalf("expected %q, got %q", msg, *received)
	}
}

func TestSendToNonExistent(t *testing.T) {
	m := NewMessenger[string](100)
	ctx := context.Background()

	msg := "hello"
	if err := m.SendTo(ctx, "nonexistent", &msg); err != ErrIDDoesNotExist {
		t.Fatalf("expected ErrIDDoesNotExist, got %v", err)
	}
}

func TestGetAsNonExistent(t *testing.T) {
	m := NewMessenger[string](100)
	ctx := context.Background()

	if _, err := m.GetAs(ctx, "nonexistent"); err != ErrIDDoesNotExist {
		t.Fatalf("expected ErrIDDoesNotExist, got %v", err)
	}
}

func TestContextCancellation(t *testing.T) {
	m := NewMessenger[string](100)

	if err := m.RegisterAs("consumer1"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	msg := "hello"
	if err := m.SendTo(ctx, "consumer1", &msg); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	if _, err := m.GetAs(ctx, "consumer1"); err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestUnregisterWhileBlocking(t *testing.T) {
	m := NewMessenger[string](100)

	if err := m.RegisterAs("consumer1"); err != nil {
		t.Fatal(err)
	}

	// Start a goroutine that will block on GetAs
	errChan := make(chan error, 1)
	go func() {
		_, err := m.GetAs(context.Background(), "consumer1")
		errChan <- err
	}()

	// Give it time to block
	time.Sleep(50 * time.Millisecond)

	// Unregister should unblock the GetAs
	m.UnregisterAs("consumer1")

	// Trigger cleanup by attempting a send (will fail, but triggers cleanup)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	msg := "test"
	m.SendTo(ctx, "consumer1", &msg)

	select {
	case err := <-errChan:
		if err != ErrIDDoesNotExist {
			t.Fatalf("expected ErrIDDoesNotExist, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("GetAs did not unblock after unregister")
	}
}

func TestSendAll(t *testing.T) {
	m := NewMessenger[string](100)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := m.RegisterAs(string(rune('a' + i))); err != nil {
			t.Fatal(err)
		}
	}

	msg := "broadcast"
	if err := m.SendAll(ctx, &msg); err != nil {
		t.Fatalf("SendAll failed: %v", err)
	}

	// All consumers should receive the message
	for i := 0; i < 5; i++ {
		received, err := m.GetAs(ctx, string(rune('a'+i)))
		if err != nil {
			t.Fatalf("GetAs failed for consumer %d: %v", i, err)
		}
		if *received != msg {
			t.Fatalf("expected %q, got %q", msg, *received)
		}
	}
}

// Race condition tests - run with -race flag

func TestConcurrentProducers(t *testing.T) {
	m := NewMessenger[int](100)
	ctx := context.Background()

	const numConsumers = 10
	const numProducers = 10
	const messagesPerProducer = 100

	// Register consumers
	for i := 0; i < numConsumers; i++ {
		if err := m.RegisterAs(string(rune('a' + i))); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup

	// Start consumers
	for i := 0; i < numConsumers; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for j := 0; j < numProducers*messagesPerProducer; j++ {
				_, err := m.GetAs(ctx, id)
				if err != nil {
					return // Consumer may be unregistered
				}
			}
		}(string(rune('a' + i)))
	}

	// Start producers
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerProducer; j++ {
				msg := producerID*messagesPerProducer + j
				m.SendAll(ctx, &msg)
			}
		}(i)
	}

	wg.Wait()
}

func TestConcurrentRegisterUnregister(t *testing.T) {
	m := NewMessenger[int](100)
	ctx := context.Background()

	const iterations = 100

	var wg sync.WaitGroup

	// Goroutine that repeatedly registers and unregisters
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				m.RegisterAs(id)
				time.Sleep(time.Microsecond)
				m.UnregisterAs(id)
			}
		}(string(rune('a' + i)))
	}

	// Goroutine that tries to send messages
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < iterations*10; j++ {
			msg := j
			m.SendAll(ctx, &msg)
		}
	}()

	wg.Wait()
}

func TestProducerUnregisterRace(t *testing.T) {
	m := NewMessenger[int](100)
	ctx := context.Background()

	const iterations = 100

	for i := 0; i < iterations; i++ {
		if err := m.RegisterAs("consumer"); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup

		// Producer trying to send
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				msg := j
				m.SendTo(ctx, "consumer", &msg)
			}
		}()

		// Consumer unregisters while producer is sending
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Microsecond)
			m.UnregisterAs("consumer")
		}()

		wg.Wait()

		// Cleanup for next iteration - force cleanup by re-registering
		m.RegisterAs("consumer")
		m.UnregisterAs("consumer")
		// Trigger cleanup
		msg := 0
		ctxShort, cancel := context.WithTimeout(ctx, time.Millisecond)
		m.SendTo(ctxShort, "consumer", &msg)
		cancel()
	}
}

func TestMultipleProducersSameConsumer(t *testing.T) {
	m := NewMessenger[int](100)
	ctx := context.Background()

	const numProducers = 10
	const messagesPerProducer = 100

	if err := m.RegisterAs("consumer"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	// Start consumer
	received := make(chan int, numProducers*messagesPerProducer)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numProducers*messagesPerProducer; i++ {
			msg, err := m.GetAs(ctx, "consumer")
			if err != nil {
				return
			}
			received <- *msg
		}
	}()

	// Start multiple producers all sending to same consumer
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < messagesPerProducer; j++ {
				msg := producerID*messagesPerProducer + j
				if err := m.SendTo(ctx, "consumer", &msg); err != nil {
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(received)

	count := 0
	for range received {
		count++
	}

	if count != numProducers*messagesPerProducer {
		t.Fatalf("expected %d messages, got %d", numProducers*messagesPerProducer, count)
	}
}

func TestCleanupRace(t *testing.T) {
	m := NewMessenger[int](100)
	ctx := context.Background()

	const numProducers = 10

	for iteration := 0; iteration < 50; iteration++ {
		if err := m.RegisterAs("consumer"); err != nil && err != ErrIDAlreadyExist {
			t.Fatal(err)
		}

		var wg sync.WaitGroup

		// Multiple producers all sending
		for i := 0; i < numProducers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					msg := j
					m.SendTo(ctx, "consumer", &msg)
				}
			}()
		}

		// Unregister to trigger cleanup race
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Microsecond)
			m.UnregisterAs("consumer")
		}()

		wg.Wait()
	}
}

func TestGetAsChannelReplacement(t *testing.T) {
	m := NewMessenger[int](100)
	ctx := context.Background()

	const iterations = 100

	for i := 0; i < iterations; i++ {
		m.RegisterAs("consumer")

		var wg sync.WaitGroup

		// Consumer blocks on GetAs
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctxShort, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
			defer cancel()
			m.GetAs(ctxShort, "consumer")
		}()

		// Re-register (which replaces the channel)
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.UnregisterAs("consumer")
			m.RegisterAs("consumer")
		}()

		wg.Wait()
		m.UnregisterAs("consumer")
	}
}

func BenchmarkSendTo(b *testing.B) {
	m := NewMessenger[int](100)
	ctx := context.Background()

	m.RegisterAs("consumer")

	// Drain messages in background
	go func() {
		for {
			m.GetAs(ctx, "consumer")
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := i
		m.SendTo(ctx, "consumer", &msg)
	}
}

func BenchmarkSendAllMultipleConsumers(b *testing.B) {
	m := NewMessenger[int](100)
	ctx := context.Background()

	const numConsumers = 10

	for i := 0; i < numConsumers; i++ {
		id := string(rune('a' + i))
		m.RegisterAs(id)

		// Drain messages in background
		go func(id string) {
			for {
				m.GetAs(ctx, id)
			}
		}(id)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := i
		m.SendAll(ctx, &msg)
	}
}

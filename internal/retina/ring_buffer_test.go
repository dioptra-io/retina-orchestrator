package retina

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNewRingBuffer(t *testing.T) {
	rb := NewRingBuffer[int](10)

	if rb == nil {
		t.Fatal("NewRingBuffer returned nil")
	}
	if rb.capacity != 10 {
		t.Errorf("expected capacity 10, got %d", rb.capacity)
	}
	if rb.buffer == nil {
		t.Error("buffer is nil")
	}
	if len(rb.buffer) != 10 {
		t.Errorf("expected buffer length 10, got %d", len(rb.buffer))
	}
	if rb.notify == nil {
		t.Error("notify channel is nil")
	}
	if rb.totalPushed != 0 {
		t.Errorf("expected totalPushed 0, got %d", rb.totalPushed)
	}
	if rb.closed {
		t.Error("ring buffer should not be closed")
	}
}

func TestNewRingBuffer_Capacities(t *testing.T) {
	tests := []uint64{1, 5, 100, 1000}
	for _, cap := range tests {
		rb := NewRingBuffer[int](cap)
		if rb.capacity != cap {
			t.Errorf("expected capacity %d, got %d", cap, rb.capacity)
		}
	}
}

func TestRingBuffer_NewConsumer(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()

	if consumer == nil {
		t.Fatal("NewConsumer returned nil")
	}
	if consumer.ringBuffer != rb {
		t.Error("consumer ringBuffer not set correctly")
	}
	if consumer.closed {
		t.Error("consumer should not be closed")
	}
}

func TestRingBuffer_NewConsumer_StartsAtCurrentPosition(t *testing.T) {
	rb := NewRingBuffer[int](10)
	ctx := context.Background()

	// Push some values
	for i := 0; i < 5; i++ {
		val := i
		_ = rb.Push(ctx, &val)
	}

	// New consumer starts at position 5
	consumer := rb.NewConsumer()
	if consumer.nextSeq != 5 {
		t.Errorf("expected nextSeq 5, got %d", consumer.nextSeq)
	}
}

func TestRingBuffer_Push(t *testing.T) {
	rb := NewRingBuffer[int](10)
	ctx := context.Background()

	val := 42
	err := rb.Push(ctx, &val)

	if err != nil {
		t.Errorf("Push failed: %v", err)
	}
	if rb.totalPushed != 1 {
		t.Errorf("expected totalPushed 1, got %d", rb.totalPushed)
	}
	if rb.buffer[0] != 42 {
		t.Errorf("expected buffer[0] = 42, got %d", rb.buffer[0])
	}
}

func TestRingBuffer_Push_Multiple(t *testing.T) {
	rb := NewRingBuffer[int](10)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		val := i * 10
		_ = rb.Push(ctx, &val)
	}

	if rb.totalPushed != 5 {
		t.Errorf("expected totalPushed 5, got %d", rb.totalPushed)
	}
}

func TestRingBuffer_Push_ContextCancelled(t *testing.T) {
	rb := NewRingBuffer[int](10)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	val := 42
	err := rb.Push(ctx, &val)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
	if rb.totalPushed != 0 {
		t.Errorf("expected totalPushed 0, got %d", rb.totalPushed)
	}
}

func TestRingBuffer_Push_Closed(t *testing.T) {
	rb := NewRingBuffer[int](10)
	rb.Close()

	ctx := context.Background()
	val := 42
	err := rb.Push(ctx, &val)

	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestRingBuffer_Push_Wraparound(t *testing.T) {
	rb := NewRingBuffer[int](3)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		val := i
		_ = rb.Push(ctx, &val)
	}

	// Buffer should contain [3, 4, 2] (indices 0, 1, 2)
	// Position 0: 3 (pushed at totalPushed=3)
	// Position 1: 4 (pushed at totalPushed=4)
	// Position 2: 2 (pushed at totalPushed=2, not overwritten yet)
	if rb.buffer[0] != 3 {
		t.Errorf("expected buffer[0] = 3, got %d", rb.buffer[0])
	}
	if rb.buffer[1] != 4 {
		t.Errorf("expected buffer[1] = 4, got %d", rb.buffer[1])
	}
}

func TestRingBuffer_Close(t *testing.T) {
	rb := NewRingBuffer[int](10)
	rb.Close()

	if !rb.closed {
		t.Error("ring buffer should be closed")
	}
}

func TestRingBuffer_Close_Idempotent(t *testing.T) {
	rb := NewRingBuffer[int](10)

	rb.Close()
	rb.Close()
	rb.Close()

	if !rb.closed {
		t.Error("ring buffer should be closed")
	}
}

func TestRingBufferConsumer_Next(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	val := 42
	_ = rb.Push(ctx, &val)

	elem, seq, err := consumer.Next(ctx)

	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if *elem != 42 {
		t.Errorf("expected 42, got %d", *elem)
	}
	if seq != 0 {
		t.Errorf("expected seq 0, got %d", seq)
	}
}

func TestRingBufferConsumer_Next_Multiple(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		val := i * 10
		_ = rb.Push(ctx, &val)
	}

	for i := 0; i < 5; i++ {
		elem, seq, err := consumer.Next(ctx)
		if err != nil {
			t.Errorf("Next failed: %v", err)
		}
		if *elem != i*10 {
			t.Errorf("expected %d, got %d", i*10, *elem)
		}
		if seq != uint64(i) {
			t.Errorf("expected seq %d, got %d", i, seq)
		}
	}
}

func TestRingBufferConsumer_Next_Blocks(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, err := consumer.Next(ctx)

	if err != context.DeadlineExceeded {
		t.Errorf("expected DeadlineExceeded, got %v", err)
	}
}

func TestRingBufferConsumer_Next_WakesOnPush(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	done := make(chan struct{})
	var result int
	var err error

	go func() {
		var elem *int
		elem, _, err = consumer.Next(ctx)
		if elem != nil {
			result = *elem
		}
		close(done)
	}()

	time.Sleep(10 * time.Millisecond)
	val := 99
	_ = rb.Push(ctx, &val)

	select {
	case <-done:
		if err != nil {
			t.Errorf("Next failed: %v", err)
		}
		if result != 99 {
			t.Errorf("expected 99, got %d", result)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Next did not wake up after Push")
	}
}

func TestRingBufferConsumer_Next_ContextCancelled(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := consumer.Next(ctx)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestRingBufferConsumer_Next_ConsumerClosed(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	consumer.Close()

	_, _, err := consumer.Next(ctx)

	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestRingBufferConsumer_Next_RingBufferClosed(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	rb.Close()

	_, _, err := consumer.Next(ctx)

	if err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}
}

func TestRingBufferConsumer_Next_WakesOnClose(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	done := make(chan error)
	go func() {
		_, _, err := consumer.Next(ctx)
		done <- err
	}()

	time.Sleep(10 * time.Millisecond)
	rb.Close()

	select {
	case err := <-done:
		if err != ErrClosed {
			t.Errorf("expected ErrClosed, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Next did not wake up after Close")
	}
}

func TestRingBufferConsumer_Next_SlowConsumer(t *testing.T) {
	rb := NewRingBuffer[int](5)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// Push 10 elements into buffer of size 5
	for i := 0; i < 10; i++ {
		val := i
		_ = rb.Push(ctx, &val)
	}

	// Consumer should skip to oldest available (seq 5)
	elem, seq, err := consumer.Next(ctx)

	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if seq != 5 {
		t.Errorf("expected seq 5 (skipped), got %d", seq)
	}
	if *elem != 5 {
		t.Errorf("expected value 5, got %d", *elem)
	}
}

func TestRingBufferConsumer_Next_SlowConsumer_ReadsRemaining(t *testing.T) {
	rb := NewRingBuffer[int](5)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// Push 10 elements
	for i := 0; i < 10; i++ {
		val := i
		_ = rb.Push(ctx, &val)
	}

	// Should read 5, 6, 7, 8, 9
	for expected := 5; expected < 10; expected++ {
		elem, seq, err := consumer.Next(ctx)
		if err != nil {
			t.Errorf("Next failed: %v", err)
		}
		if *elem != expected {
			t.Errorf("expected %d, got %d", expected, *elem)
		}
		if seq != uint64(expected) {
			t.Errorf("expected seq %d, got %d", expected, seq)
		}
	}
}

func TestRingBufferConsumer_Close(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()

	consumer.Close()

	if !consumer.closed {
		t.Error("consumer should be closed")
	}
}

func TestRingBufferConsumer_Close_Idempotent(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()

	consumer.Close()
	consumer.Close()
	consumer.Close()

	if !consumer.closed {
		t.Error("consumer should be closed")
	}
}

func TestRingBuffer_MultipleConsumers_Independent(t *testing.T) {
	rb := NewRingBuffer[int](10)
	c1 := rb.NewConsumer()
	c2 := rb.NewConsumer()
	ctx := context.Background()

	// Push values
	for i := 0; i < 3; i++ {
		val := i
		_ = rb.Push(ctx, &val)
	}

	// c1 reads all
	for i := 0; i < 3; i++ {
		_, _, _ = c1.Next(ctx)
	}

	// c2 should still read all
	for i := 0; i < 3; i++ {
		elem, _, err := c2.Next(ctx)
		if err != nil {
			t.Errorf("c2.Next failed: %v", err)
		}
		if *elem != i {
			t.Errorf("expected %d, got %d", i, *elem)
		}
	}
}

func TestRingBuffer_ConsumerCreatedAfterPush(t *testing.T) {
	rb := NewRingBuffer[int](10)
	ctx := context.Background()

	// Push values first
	for i := 0; i < 5; i++ {
		val := i
		_ = rb.Push(ctx, &val)
	}

	// Create consumer after push
	consumer := rb.NewConsumer()

	// Push more values
	val := 100
	_ = rb.Push(ctx, &val)

	// Consumer should only see new value
	elem, seq, err := consumer.Next(ctx)
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if *elem != 100 {
		t.Errorf("expected 100, got %d", *elem)
	}
	if seq != 5 {
		t.Errorf("expected seq 5, got %d", seq)
	}
}

func TestRingBuffer_ConcurrentPush(t *testing.T) {
	rb := NewRingBuffer[int](100)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				val := n*10 + j
				_ = rb.Push(ctx, &val)
			}
		}(i)
	}
	wg.Wait()

	if rb.totalPushed != 100 {
		t.Errorf("expected 100 pushes, got %d", rb.totalPushed)
	}
}

func TestRingBuffer_ConcurrentConsumers(t *testing.T) {
	rb := NewRingBuffer[int](100)
	ctx := context.Background()

	var wg sync.WaitGroup

	// Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			val := i
			_ = rb.Push(ctx, &val)
		}
		rb.Close()
	}()

	// Multiple consumers
	for i := 0; i < 3; i++ {
		consumer := rb.NewConsumer()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				_, _, err := consumer.Next(ctx)
				if err != nil {
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestRingBuffer_SequenceNumbersAreUnique(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		val := i
		_ = rb.Push(ctx, &val)
	}

	seen := make(map[uint64]bool)
	for i := 0; i < 10; i++ {
		_, seq, err := consumer.Next(ctx)
		if err != nil {
			t.Errorf("Next failed: %v", err)
		}
		if seen[seq] {
			t.Errorf("duplicate sequence number: %d", seq)
		}
		seen[seq] = true
	}
}

func TestRingBuffer_StringType(t *testing.T) {
	rb := NewRingBuffer[string](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	msg := "hello"
	_ = rb.Push(ctx, &msg)

	elem, _, err := consumer.Next(ctx)
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if *elem != "hello" {
		t.Errorf("expected 'hello', got '%s'", *elem)
	}
}

func TestRingBuffer_StructType(t *testing.T) {
	type Data struct {
		ID   int
		Name string
	}

	rb := NewRingBuffer[Data](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	data := Data{ID: 1, Name: "test"}
	_ = rb.Push(ctx, &data)

	elem, _, err := consumer.Next(ctx)
	if err != nil {
		t.Errorf("Next failed: %v", err)
	}
	if elem.ID != 1 || elem.Name != "test" {
		t.Errorf("expected {1, test}, got %+v", *elem)
	}
}

func TestErrClosed(t *testing.T) {
	if ErrClosed.Error() != "closed" {
		t.Errorf("unexpected error message: %s", ErrClosed.Error())
	}
}

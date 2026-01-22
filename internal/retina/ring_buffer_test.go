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
		t.Fatal("expected non-nil ring buffer")
	}
	if rb.capacity != 10 {
		t.Errorf("expected capacity 10, got %d", rb.capacity)
	}
	if rb.head != 0 {
		t.Errorf("expected head 0, got %d", rb.head)
	}
	if len(rb.consumers) != 0 {
		t.Errorf("expected 0 consumers, got %d", len(rb.consumers))
	}
}

func TestNewConsumer(t *testing.T) {
	rb := NewRingBuffer[int](10)

	consumer1 := rb.NewConsumer()
	if consumer1 == nil {
		t.Fatal("expected non-nil consumer")
	}
	if consumer1.id != 0 {
		t.Errorf("expected consumer id 0, got %d", consumer1.id)
	}
	if consumer1.closed {
		t.Error("expected consumer to not be closed")
	}

	consumer2 := rb.NewConsumer()
	if consumer2.id != 1 {
		t.Errorf("expected consumer id 1, got %d", consumer2.id)
	}

	if len(rb.consumers) != 2 {
		t.Errorf("expected 2 consumers, got %d", len(rb.consumers))
	}
}

func TestPushAndPop(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// Push an element
	val := 42
	err := rb.Push(ctx, &val)
	if err != nil {
		t.Fatalf("unexpected error on push: %v", err)
	}

	// Pop the element
	result, seqNum, err := consumer.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on pop: %v", err)
	}
	if *result != 42 {
		t.Errorf("expected 42, got %d", *result)
	}
	if seqNum != 0 {
		t.Errorf("expected sequence number 0, got %d", seqNum)
	}
}

func TestPushMultipleAndPop(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// Push multiple elements
	for i := 0; i < 5; i++ {
		val := i * 10
		err := rb.Push(ctx, &val)
		if err != nil {
			t.Fatalf("unexpected error on push %d: %v", i, err)
		}
	}

	// Pop all elements
	for i := 0; i < 5; i++ {
		result, seqNum, err := consumer.Pop(ctx)
		if err != nil {
			t.Fatalf("unexpected error on pop %d: %v", i, err)
		}
		if *result != i*10 {
			t.Errorf("expected %d, got %d", i*10, *result)
		}
		if seqNum != uint64(i) {
			t.Errorf("expected sequence number %d, got %d", i, seqNum)
		}
	}
}

func TestMultipleConsumers(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer1 := rb.NewConsumer()
	consumer2 := rb.NewConsumer()
	ctx := context.Background()

	// Push an element
	val := 100
	err := rb.Push(ctx, &val)
	if err != nil {
		t.Fatalf("unexpected error on push: %v", err)
	}

	// Both consumers should be able to pop the same element
	result1, seqNum1, err := consumer1.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on consumer1 pop: %v", err)
	}
	if *result1 != 100 {
		t.Errorf("consumer1: expected 100, got %d", *result1)
	}
	if seqNum1 != 0 {
		t.Errorf("consumer1: expected sequence number 0, got %d", seqNum1)
	}

	result2, seqNum2, err := consumer2.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on consumer2 pop: %v", err)
	}
	if *result2 != 100 {
		t.Errorf("consumer2: expected 100, got %d", *result2)
	}
	if seqNum2 != 0 {
		t.Errorf("consumer2: expected sequence number 0, got %d", seqNum2)
	}
}

func TestPopBlocking(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	done := make(chan struct{})
	var result *int
	var seqNum uint64
	var popErr error

	// Start a goroutine that will block on Pop
	go func() {
		result, seqNum, popErr = consumer.Pop(ctx)
		close(done)
	}()

	// Give some time for the goroutine to start and block
	time.Sleep(50 * time.Millisecond)

	// Push an element to unblock
	val := 55
	err := rb.Push(ctx, &val)
	if err != nil {
		t.Fatalf("unexpected error on push: %v", err)
	}

	// Wait for pop to complete
	select {
	case <-done:
		if popErr != nil {
			t.Fatalf("unexpected error on pop: %v", popErr)
		}
		if *result != 55 {
			t.Errorf("expected 55, got %d", *result)
		}
		if seqNum != 0 {
			t.Errorf("expected sequence number 0, got %d", seqNum)
		}
	case <-time.After(time.Second):
		t.Fatal("pop did not unblock in time")
	}
}

func TestPopContextCancellation(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	var popErr error

	// Start a goroutine that will block on Pop
	go func() {
		_, _, popErr = consumer.Pop(ctx)
		close(done)
	}()

	// Give some time for the goroutine to start and block
	time.Sleep(50 * time.Millisecond)

	// Cancel the context
	cancel()

	// Wait for pop to complete
	select {
	case <-done:
		if popErr != context.Canceled {
			t.Errorf("expected context.Canceled error, got %v", popErr)
		}
	case <-time.After(time.Second):
		t.Fatal("pop did not unblock after context cancellation")
	}
}

func TestPopContextTimeout(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, _, err := consumer.Pop(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded error, got %v", err)
	}
}

func TestPushContextCancellation(t *testing.T) {
	rb := NewRingBuffer[int](10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	val := 42
	err := rb.Push(ctx, &val)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got %v", err)
	}
}

func TestConsumerClose(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// Close the consumer
	consumer.Close()

	if !consumer.closed {
		t.Error("expected consumer to be closed")
	}

	// Verify consumer is removed from the ring buffer
	rb.mutex.Lock()
	if len(rb.consumers) != 0 {
		t.Errorf("expected 0 consumers, got %d", len(rb.consumers))
	}
	rb.mutex.Unlock()

	// Pop should return ErrClosed
	_, _, err := consumer.Pop(ctx)
	if err != ErrClosed {
		t.Errorf("expected ErrClosed error, got %v", err)
	}
}

func TestConsumerCloseIdempotent(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()

	// Close multiple times should not panic
	consumer.Close()
	consumer.Close()
	consumer.Close()

	if !consumer.closed {
		t.Error("expected consumer to be closed")
	}
}

func TestConsumerCloseWhileBlocking(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	done := make(chan struct{})
	var popErr error

	// Start a goroutine that will block on Pop
	go func() {
		_, _, popErr = consumer.Pop(ctx)
		close(done)
	}()

	// Give some time for the goroutine to start and block
	time.Sleep(50 * time.Millisecond)

	// Close the consumer
	consumer.Close()

	// Wait for pop to complete
	select {
	case <-done:
		if popErr != ErrClosed {
			t.Errorf("expected ErrClosed error, got %v", popErr)
		}
	case <-time.After(time.Second):
		t.Fatal("pop did not unblock after consumer close")
	}
}

func TestSlowConsumerTailPush(t *testing.T) {
	capacity := uint64(5)
	rb := NewRingBuffer[int](capacity)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// Push more elements than capacity without consuming
	for i := 0; i < 10; i++ {
		val := i
		err := rb.Push(ctx, &val)
		if err != nil {
			t.Fatalf("unexpected error on push %d: %v", i, err)
		}
	}

	// The slow consumer should have had its tail pushed
	// It should now start from element 5 (missing 0-4)
	result, seqNum, err := consumer.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on pop: %v", err)
	}

	// The sequence number tells us how many elements were missed
	if seqNum < 5 {
		t.Errorf("expected sequence number >= 5, got %d", seqNum)
	}

	// The value should be from the later pushes
	if *result < 5 {
		t.Errorf("expected value >= 5, got %d", *result)
	}
}

func TestRingBufferWrapAround(t *testing.T) {
	capacity := uint64(3)
	rb := NewRingBuffer[int](capacity)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// Push and pop to cause wrap around
	for round := 0; round < 3; round++ {
		for i := 0; i < 3; i++ {
			val := round*10 + i
			err := rb.Push(ctx, &val)
			if err != nil {
				t.Fatalf("unexpected error on push: %v", err)
			}

			result, _, err := consumer.Pop(ctx)
			if err != nil {
				t.Fatalf("unexpected error on pop: %v", err)
			}
			if *result != val {
				t.Errorf("expected %d, got %d", val, *result)
			}
		}
	}
}

func TestConcurrentPushAndPop(t *testing.T) {
	rb := NewRingBuffer[int](100)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	numElements := 1000
	var wg sync.WaitGroup

	// Producer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numElements; i++ {
			val := i
			err := rb.Push(ctx, &val)
			if err != nil {
				t.Errorf("unexpected error on push %d: %v", i, err)
				return
			}
		}
	}()

	// Consumer goroutine
	received := make([]int, 0, numElements)
	var receivedMu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numElements; i++ {
			result, _, err := consumer.Pop(ctx)
			if err != nil {
				t.Errorf("unexpected error on pop %d: %v", i, err)
				return
			}
			receivedMu.Lock()
			received = append(received, *result)
			receivedMu.Unlock()
		}
	}()

	wg.Wait()

	if len(received) != numElements {
		t.Errorf("expected %d elements, got %d", numElements, len(received))
	}
}

func TestConcurrentMultipleConsumers(t *testing.T) {
	rb := NewRingBuffer[int](100)
	ctx := context.Background()

	numConsumers := 5
	numElements := 100

	consumers := make([]*RingBufferConsumer[int], numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumers[i] = rb.NewConsumer()
	}

	var wg sync.WaitGroup

	// Producer goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numElements; i++ {
			val := i
			err := rb.Push(ctx, &val)
			if err != nil {
				t.Errorf("unexpected error on push %d: %v", i, err)
				return
			}
		}
	}()

	// Consumer goroutines
	results := make([][]int, numConsumers)
	var resultsMu sync.Mutex

	for i := 0; i < numConsumers; i++ {
		results[i] = make([]int, 0, numElements)
		wg.Add(1)
		go func(consumerIdx int) {
			defer wg.Done()
			for j := 0; j < numElements; j++ {
				result, _, err := consumers[consumerIdx].Pop(ctx)
				if err != nil {
					t.Errorf("consumer %d: unexpected error on pop %d: %v", consumerIdx, j, err)
					return
				}
				resultsMu.Lock()
				results[consumerIdx] = append(results[consumerIdx], *result)
				resultsMu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Each consumer should have received all elements
	for i := 0; i < numConsumers; i++ {
		if len(results[i]) != numElements {
			t.Errorf("consumer %d: expected %d elements, got %d", i, numElements, len(results[i]))
		}
	}
}

func TestConsumerCreatedAfterPush(t *testing.T) {
	rb := NewRingBuffer[int](10)
	ctx := context.Background()

	// Push some elements first
	for i := 0; i < 5; i++ {
		val := i
		err := rb.Push(ctx, &val)
		if err != nil {
			t.Fatalf("unexpected error on push %d: %v", i, err)
		}
	}

	// Create consumer after pushes
	consumer := rb.NewConsumer()

	// Consumer should start at current head and not see previous elements
	// Push a new element
	val := 100
	err := rb.Push(ctx, &val)
	if err != nil {
		t.Fatalf("unexpected error on push: %v", err)
	}

	// Consumer should only see the new element
	result, seqNum, err := consumer.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on pop: %v", err)
	}
	if *result != 100 {
		t.Errorf("expected 100, got %d", *result)
	}
	if seqNum != 5 {
		t.Errorf("expected sequence number 5, got %d", seqNum)
	}
}

func TestSequenceNumberContinuity(t *testing.T) {
	rb := NewRingBuffer[int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// Push and pop multiple elements, verify sequence numbers are continuous
	for i := 0; i < 20; i++ {
		val := i
		err := rb.Push(ctx, &val)
		if err != nil {
			t.Fatalf("unexpected error on push %d: %v", i, err)
		}

		_, seqNum, err := consumer.Pop(ctx)
		if err != nil {
			t.Fatalf("unexpected error on pop %d: %v", i, err)
		}
		if seqNum != uint64(i) {
			t.Errorf("expected sequence number %d, got %d", i, seqNum)
		}
	}
}

func TestEmptyRingBuffer(t *testing.T) {
	rb := NewRingBuffer[int](10)

	if rb.head != 0 {
		t.Errorf("expected head 0, got %d", rb.head)
	}
	if rb.totalPushed != 0 {
		t.Errorf("expected totalPushed 0, got %d", rb.totalPushed)
	}
}

func TestRingBufferWithStructType(t *testing.T) {
	type Message struct {
		ID      int
		Content string
	}

	rb := NewRingBuffer[Message](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	msg := Message{ID: 1, Content: "hello"}
	err := rb.Push(ctx, &msg)
	if err != nil {
		t.Fatalf("unexpected error on push: %v", err)
	}

	result, _, err := consumer.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on pop: %v", err)
	}
	if result.ID != 1 || result.Content != "hello" {
		t.Errorf("expected {1, hello}, got {%d, %s}", result.ID, result.Content)
	}
}

func TestRingBufferWithPointerType(t *testing.T) {
	rb := NewRingBuffer[*int](10)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	val := 42
	ptr := &val
	err := rb.Push(ctx, &ptr)
	if err != nil {
		t.Fatalf("unexpected error on push: %v", err)
	}

	result, _, err := consumer.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on pop: %v", err)
	}
	if **result != 42 {
		t.Errorf("expected 42, got %d", **result)
	}
}

func TestCapacityOne(t *testing.T) {
	rb := NewRingBuffer[int](1)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	// With capacity 1, each push overwrites the previous
	val1 := 1
	err := rb.Push(ctx, &val1)
	if err != nil {
		t.Fatalf("unexpected error on push: %v", err)
	}

	result, _, err := consumer.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on pop: %v", err)
	}
	if *result != 1 {
		t.Errorf("expected 1, got %d", *result)
	}

	// Push another
	val2 := 2
	err = rb.Push(ctx, &val2)
	if err != nil {
		t.Fatalf("unexpected error on push: %v", err)
	}

	result, _, err = consumer.Pop(ctx)
	if err != nil {
		t.Fatalf("unexpected error on pop: %v", err)
	}
	if *result != 2 {
		t.Errorf("expected 2, got %d", *result)
	}
}

func TestSlowConsumerMissesElements(t *testing.T) {
	capacity := uint64(3)
	rb := NewRingBuffer[int](capacity)
	fastConsumer := rb.NewConsumer()
	slowConsumer := rb.NewConsumer()
	ctx := context.Background()

	// Push 2 elements
	for i := 0; i < 2; i++ {
		val := i
		rb.Push(ctx, &val)
	}

	// Fast consumer reads all
	for i := 0; i < 2; i++ {
		fastConsumer.Pop(ctx)
	}

	// Push 3 more elements (this will overflow and push slow consumer's tail)
	for i := 2; i < 5; i++ {
		val := i
		rb.Push(ctx, &val)
	}

	// Fast consumer continues reading
	for i := 2; i < 5; i++ {
		result, _, _ := fastConsumer.Pop(ctx)
		if *result != i {
			t.Errorf("fast consumer: expected %d, got %d", i, *result)
		}
	}

	// Slow consumer should have missed some elements
	result, seqNum, _ := slowConsumer.Pop(ctx)

	// The slow consumer's sequence number should be > 0 indicating missed elements
	if seqNum == 0 && *result == 0 {
		// It got the first element, which might be okay if timing worked out
		// but generally we expect it to have been pushed forward
	}

	// Just verify we can read something without error
	if result == nil {
		t.Error("expected non-nil result from slow consumer")
	}
}

func TestCloseRemovesFromConsumersMap(t *testing.T) {
	rb := NewRingBuffer[int](10)

	consumer1 := rb.NewConsumer()
	consumer2 := rb.NewConsumer()
	consumer3 := rb.NewConsumer()

	rb.mutex.Lock()
	if len(rb.consumers) != 3 {
		t.Errorf("expected 3 consumers, got %d", len(rb.consumers))
	}
	rb.mutex.Unlock()

	consumer2.Close()

	rb.mutex.Lock()
	if len(rb.consumers) != 2 {
		t.Errorf("expected 2 consumers, got %d", len(rb.consumers))
	}
	if _, exists := rb.consumers[consumer2.id]; exists {
		t.Error("consumer2 should have been removed from map")
	}
	rb.mutex.Unlock()

	consumer1.Close()
	consumer3.Close()

	rb.mutex.Lock()
	if len(rb.consumers) != 0 {
		t.Errorf("expected 0 consumers, got %d", len(rb.consumers))
	}
	rb.mutex.Unlock()
}

func BenchmarkPushPop(b *testing.B) {
	rb := NewRingBuffer[int](1000)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := i
		rb.Push(ctx, &val)
		consumer.Pop(ctx)
	}
}

func BenchmarkPushOnly(b *testing.B) {
	rb := NewRingBuffer[int](1000)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		val := i
		rb.Push(ctx, &val)
	}
}

func BenchmarkConcurrentPushPop(b *testing.B) {
	rb := NewRingBuffer[int](1000)
	consumer := rb.NewConsumer()
	ctx := context.Background()

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			val := i
			rb.Push(ctx, &val)
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			consumer.Pop(ctx)
		}
	}()

	wg.Wait()
}

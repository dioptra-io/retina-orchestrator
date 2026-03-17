package structures

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- NewQueue ---

func TestNewQueue_Valid(t *testing.T) {
	q, err := NewQueue[int](10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if q == nil {
		t.Fatal("expected non-nil Queue")
	}
}

// --- NewConsumer ---

func TestNewConsumer_Valid(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, err := q.NewConsumer("a")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cons == nil {
		t.Fatal("expected non-nil consumer")
	}
	cons.Close()
}

func TestNewConsumer_DuplicateID(t *testing.T) {
	q, _ := NewQueue[int](4)
	_, err := q.NewConsumer("a")
	if err != nil {
		t.Fatalf("unexpected error on first consumer: %v", err)
	}
	_, err = q.NewConsumer("a")
	if err == nil {
		t.Fatal("expected error on duplicate id")
	}
}

func TestNewConsumer_DuplicateID_AfterClose(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	cons.Close()

	// After close the id should be free to reuse.
	_, err := q.NewConsumer("a")
	if err != nil {
		t.Fatalf("expected to reuse id after close, got error: %v", err)
	}
}

func TestNewConsumer_MultipleDistinctIDs(t *testing.T) {
	q, _ := NewQueue[int](4)
	c1, err1 := q.NewConsumer("a")
	c2, err2 := q.NewConsumer("b")
	defer c1.Close()
	defer c2.Close()

	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected errors: %v, %v", err1, err2)
	}
	if c1 == c2 {
		t.Fatal("expected distinct consumers")
	}
}

// --- Push / Pop ---

func TestQueuePushPop_SingleConsumer(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 42
	if err := q.Push(context.Background(), "a", &v); err != nil {
		t.Fatalf("unexpected push error: %v", err)
	}

	got, err := cons.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected pop error: %v", err)
	}
	if *got != 42 {
		t.Fatalf("expected 42, got %d", *got)
	}
}

func TestQueuePushPop_Order(t *testing.T) {
	q, _ := NewQueue[int](8)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	for i := range 5 {
		v := i
		if err := q.Push(context.Background(), "a", &v); err != nil {
			t.Fatalf("unexpected push error: %v", err)
		}
	}

	for i := range 5 {
		got, err := cons.Pop(context.Background())
		if err != nil {
			t.Fatalf("unexpected pop error: %v", err)
		}
		if *got != i {
			t.Fatalf("expected %d, got %d", i, *got)
		}
	}
}

func TestQueuePushPop_MultipleConsumers_Independent(t *testing.T) {
	q, _ := NewQueue[int](4)
	c1, _ := q.NewConsumer("a")
	c2, _ := q.NewConsumer("b")
	defer c1.Close()
	defer c2.Close()

	v1, v2 := 1, 2
	q.Push(context.Background(), "a", &v1)
	q.Push(context.Background(), "b", &v2)

	got1, err := c1.Pop(context.Background())
	if err != nil || *got1 != 1 {
		t.Fatalf("c1: expected 1, got %v, err %v", got1, err)
	}

	got2, err := c2.Pop(context.Background())
	if err != nil || *got2 != 2 {
		t.Fatalf("c2: expected 2, got %v, err %v", got2, err)
	}
}

func TestQueuePop_BlocksUntilPush(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 7
	go func() {
		time.Sleep(50 * time.Millisecond)
		q.Push(context.Background(), "a", &v)
	}()

	got, err := cons.Pop(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if *got != 7 {
		t.Fatalf("expected 7, got %d", *got)
	}
}

func TestPush_BlocksWhenBufferFull(t *testing.T) {
	q, _ := NewQueue[int](2)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 1
	q.Push(context.Background(), "a", &v)
	q.Push(context.Background(), "a", &v)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := q.Push(ctx, "a", &v)
	if err == nil {
		t.Fatal("expected error when pushing to full buffer")
	}
}

// --- Push to unregistered / closed consumer ---

func TestPush_UnknownID(t *testing.T) {
	q, _ := NewQueue[int](4)

	v := 1
	err := q.Push(context.Background(), "nonexistent", &v)
	if err == nil {
		t.Fatal("expected error when pushing to unknown id")
	}
}

func TestPush_AfterClose(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	cons.Close()

	v := 1
	err := q.Push(context.Background(), "a", &v)
	if err == nil {
		t.Fatal("expected error pushing to closed consumer")
	}
}

// --- Context cancellation ---

func TestQueuePop_ContextCancelledBeforeCall(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := cons.Pop(ctx)
	if err == nil {
		t.Fatal("expected error on cancelled context")
	}
}

func TestQueuePop_ContextCancelledWhileBlocking(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := cons.Pop(ctx)
	if err == nil {
		t.Fatal("expected error on context cancellation")
	}
}

func TestQueuePop_ContextTimeout(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := cons.Pop(ctx)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

func TestPush_ContextCancelledWhileBlocking(t *testing.T) {
	q, _ := NewQueue[int](1)
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 1
	q.Push(context.Background(), "a", &v) // fill the buffer

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := q.Push(ctx, "a", &v)
	if err == nil {
		t.Fatal("expected error on context cancellation")
	}
}

// --- Close ---

func TestQueueClose_Idempotent(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")

	cons.Close()
	cons.Close() // should not panic
}

func TestQueueClose_PopAfterClose(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	cons.Close()

	_, err := cons.Pop(context.Background())
	if err == nil {
		t.Fatal("expected error after Close")
	}
}

func TestClose_UnregistersFromQueue(t *testing.T) {
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")
	cons.Close()

	q.mu.Lock()
	_, stillRegistered := q.consumers["a"]
	q.mu.Unlock()

	if stillRegistered {
		t.Fatal("expected consumer to be removed from queue after Close")
	}
}

// --- Concurrency ---

func TestQueueConcurrent_PushPop(t *testing.T) {
	q, _ := NewQueue[int](128)
	cons, _ := q.NewConsumer("a")
	const n = 100

	var wg sync.WaitGroup
	results := make([]int, 0, n)
	var mu sync.Mutex

	wg.Go(func() {
		defer cons.Close()
		for range n {
			v, err := cons.Pop(context.Background())
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			mu.Lock()
			results = append(results, *v)
			mu.Unlock()
		}
	})

	for i := range n {
		v := i
		if err := q.Push(context.Background(), "a", &v); err != nil {
			t.Fatalf("unexpected push error: %v", err)
		}
	}

	wg.Wait()

	if len(results) != n {
		t.Fatalf("expected %d results, got %d", n, len(results))
	}
}

func TestConcurrent_MultipleConsumers(t *testing.T) {
	q, _ := NewQueue[int](128)
	const n = 100
	ids := []string{"a", "b", "c"}

	consumers := make([]*queueConsumer[int], len(ids))
	for i, id := range ids {
		cons, err := q.NewConsumer(id)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		consumers[i] = cons
	}

	var wg sync.WaitGroup
	for i, cons := range consumers {
		id := ids[i]
		wg.Go(func() {
			defer cons.Close()
			for range n {
				_, err := cons.Pop(context.Background())
				if err != nil {
					t.Errorf("[%s] unexpected error: %v", id, err)
					return
				}
			}
		})
	}

	for i := range n {
		v := i
		for _, id := range ids {
			if err := q.Push(context.Background(), id, &v); err != nil {
				t.Fatalf("unexpected push error: %v", err)
			}
		}
	}

	wg.Wait()
}

func TestNewQueue_ZeroBufferSize(t *testing.T) {
	// Zero buffer size creates an unbuffered channel — Push blocks until Pop is called.
	q, err := NewQueue[int](0)
	if err != nil {
		t.Fatalf("expected not to get an error")
	}
	cons, _ := q.NewConsumer("a")
	defer cons.Close()

	v := 1
	done := make(chan struct{})
	go func() {
		q.Push(context.Background(), "a", &v)
		close(done)
	}()

	// Push should be blocked until we pop.
	select {
	case <-done:
		t.Fatal("expected Push to block on unbuffered queue")
	case <-time.After(50 * time.Millisecond):
	}

	cons.Pop(context.Background())

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected Push to unblock after Pop")
	}
}

func TestNewQueue_NegativeBufferSize(t *testing.T) {
	_, err := NewQueue[int](-1)
	if err == nil {
		t.Fatal("expected error for negative buffer size")
	}
}

func TestPush_DoesNotDeliverToOtherConsumers(t *testing.T) {
	q, _ := NewQueue[int](4)
	c1, _ := q.NewConsumer("a")
	c2, _ := q.NewConsumer("b")
	defer c1.Close()
	defer c2.Close()

	v := 42
	q.Push(context.Background(), "a", &v)

	// c1 should receive it.
	got, err := c1.Pop(context.Background())
	if err != nil || *got != 42 {
		t.Fatalf("c1: expected 42, got %v, err %v", got, err)
	}

	// c2 should not receive anything — Pop should time out.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = c2.Pop(ctx)
	if err == nil {
		t.Fatal("c2: expected timeout, got an element")
	}
}

func TestClose_BufferedItemsLost(t *testing.T) {
	// Closing a consumer with buffered items should cause subsequent Pops to error,
	// not return the buffered items.
	q, _ := NewQueue[int](4)
	cons, _ := q.NewConsumer("a")

	v := 1
	q.Push(context.Background(), "a", &v)
	cons.Close()

	_, err := cons.Pop(context.Background())
	if err == nil {
		t.Fatal("expected error after Close, even with buffered items")
	}
}

func TestConcurrent_CloseAndPush(t *testing.T) {
	// Race between Close and Push — the recover() in Push should handle the panic.
	for range 100 {
		q, _ := NewQueue[int](4)
		cons, _ := q.NewConsumer("a")

		var wg sync.WaitGroup

		wg.Go(func() {
			v := 1
			q.Push(context.Background(), "a", &v) // may panic-recover if closed
		})

		wg.Go(func() {
			cons.Close()
		})

		wg.Wait()
	}
}

func TestConcurrent_CloseAndPop(t *testing.T) {
	// Race between Close and Pop — Pop should return an error, not panic.
	for range 100 {
		q, _ := NewQueue[int](4)
		cons, _ := q.NewConsumer("a")

		var wg sync.WaitGroup

		wg.Go(func() {
			cons.Pop(context.Background()) // may return error if closed
		})

		wg.Go(func() {
			cons.Close()
		})

		wg.Wait()
	}
}

func TestConcurrent_ManyConsumers(t *testing.T) {
	q, _ := NewQueue[int](16)
	const numConsumers = 100
	const n = 50

	consumers := make([]*queueConsumer[int], numConsumers)
	for i := range numConsumers {
		cons, err := q.NewConsumer(fmt.Sprintf("consumer-%d", i))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		consumers[i] = cons
	}

	var wg sync.WaitGroup
	for i, cons := range consumers {
		id := fmt.Sprintf("consumer-%d", i)
		wg.Go(func() {
			defer cons.Close()
			for range n {
				_, err := cons.Pop(context.Background())
				if err != nil {
					t.Errorf("[%s] unexpected error: %v", id, err)
					return
				}
			}
		})
	}

	for i := range n {
		v := i
		for j := range numConsumers {
			id := fmt.Sprintf("consumer-%d", j)
			if err := q.Push(context.Background(), id, &v); err != nil {
				t.Fatalf("unexpected push error: %v", err)
			}
		}
	}

	wg.Wait()
}

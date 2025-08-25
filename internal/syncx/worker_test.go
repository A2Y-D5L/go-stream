package syncx

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --------------------- Worker Pool Tests ---------------------

func TestWorkerPool_Creation(t *testing.T) {
	config := WorkerConfig{
		Workers:      4,
		QueueSize:    100,
		Backpressure: BackpressureBlock,
		Metrics:      true,
	}
	
	wp := NewWorkerPool(config)
	
	if wp.workers != 4 {
		t.Errorf("Expected 4 workers, got %d", wp.workers)
	}
	
	if cap(wp.queue) != 100 {
		t.Errorf("Expected queue size 100, got %d", cap(wp.queue))
	}
	
	if wp.backpressure != BackpressureBlock {
		t.Errorf("Expected BackpressureBlock, got %v", wp.backpressure)
	}
	
	if wp.metrics == nil {
		t.Error("Expected metrics to be enabled")
	}
}

func TestWorkerPool_StartStop(t *testing.T) {
	wp := NewWorkerPool(WorkerConfig{Workers: 2, QueueSize: 10})
	
	// Test start
	err := wp.Start()
	if err != nil {
		t.Fatalf("Failed to start worker pool: %v", err)
	}
	
	if !wp.IsStarted() {
		t.Error("Worker pool should be started")
	}
	
	// Test double start
	err = wp.Start()
	if err != ErrWorkerPoolAlreadyStarted {
		t.Errorf("Expected ErrWorkerPoolAlreadyStarted, got %v", err)
	}
	
	// Test stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err = wp.Stop(ctx)
	if err != nil {
		t.Fatalf("Failed to stop worker pool: %v", err)
	}
	
	if !wp.IsStopped() {
		t.Error("Worker pool should be stopped")
	}
	
	// Test double stop
	err = wp.Stop(ctx)
	if err != ErrWorkerPoolAlreadyStopped {
		t.Errorf("Expected ErrWorkerPoolAlreadyStopped, got %v", err)
	}
}

func TestWorkerPool_TaskExecution(t *testing.T) {
	wp := NewWorkerPool(WorkerConfig{
		Workers:   2,
		QueueSize: 10,
		Metrics:   true,
	})
	
	err := wp.Start()
	if err != nil {
		t.Fatalf("Failed to start worker pool: %v", err)
	}
	defer wp.Stop(context.Background())
	
	var counter int64
	var wg sync.WaitGroup
	
	// Submit tasks
	numTasks := 10
	wg.Add(numTasks)
	
	for i := 0; i < numTasks; i++ {
		err := wp.Submit(func() {
			atomic.AddInt64(&counter, 1)
			wg.Done()
		})
		if err != nil {
			t.Errorf("Failed to submit task: %v", err)
		}
	}
	
	// Wait for completion
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// Success - wait a bit for metrics to be updated
		time.Sleep(10 * time.Millisecond)
	case <-time.After(5 * time.Second):
		t.Fatal("Tasks did not complete within timeout")
	}
	
	if atomic.LoadInt64(&counter) != int64(numTasks) {
		t.Errorf("Expected %d tasks executed, got %d", numTasks, atomic.LoadInt64(&counter))
	}
	
	// Check metrics
	metrics := wp.Metrics()
	if metrics.TasksSubmitted != uint64(numTasks) {
		t.Errorf("Expected %d tasks submitted, got %d", numTasks, metrics.TasksSubmitted)
	}
	if metrics.TasksCompleted != uint64(numTasks) {
		t.Errorf("Expected %d tasks completed, got %d", numTasks, metrics.TasksCompleted)
	}
}

func TestWorkerPool_BackpressurePolicies(t *testing.T) {
	tests := []struct {
		name   string
		policy BackpressurePolicy
	}{
		{"DropOldest", BackpressureDropOldest},
		{"DropNewest", BackpressureDropNewest},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wp := NewWorkerPool(WorkerConfig{
				Workers:      1,
				QueueSize:    2,
				Backpressure: tt.policy,
				Metrics:      true,
			})
			
			err := wp.Start()
			if err != nil {
				t.Fatalf("Failed to start worker pool: %v", err)
			}
			defer wp.Stop(context.Background())
			
			// Fill queue and worker
			blocker := make(chan struct{})
			
			// Block the worker
			wp.Submit(func() { <-blocker })
			
			// Fill the queue
			wp.Submit(func() {})
			wp.Submit(func() {})
			
			// This should trigger backpressure
			switch tt.policy {
			case BackpressureDropNewest:
				err := wp.Submit(func() {})
				if err != ErrTaskDropped {
					t.Errorf("Expected ErrTaskDropped, got %v", err)
				}
			case BackpressureDropOldest:
				err := wp.Submit(func() {})
				if err != nil {
					t.Errorf("Expected no error for DropOldest, got %v", err)
				}
			}
			
			close(blocker) // Unblock worker
		})
	}
}

func TestWorkerPool_SubmitWithTimeout(t *testing.T) {
	wp := NewWorkerPool(WorkerConfig{Workers: 1, QueueSize: 1})
	
	err := wp.Start()
	if err != nil {
		t.Fatalf("Failed to start worker pool: %v", err)
	}
	defer wp.Stop(context.Background())
	
	// Block the worker and fill the queue
	blocker := make(chan struct{})
	wp.Submit(func() { <-blocker })
	wp.Submit(func() {})
	
	// This should timeout
	err = wp.SubmitWithTimeout(func() {}, 100*time.Millisecond)
	if err != ErrTimeout {
		t.Errorf("Expected ErrTimeout, got %v", err)
	}
	
	close(blocker) // Unblock worker
}

// --------------------- Multi-Error Tests ---------------------

func TestMultiError(t *testing.T) {
	me := NewMultiError()
	
	if me.HasErrors() {
		t.Error("New MultiError should not have errors")
	}
	
	if me.Count() != 0 {
		t.Errorf("Expected 0 errors, got %d", me.Count())
	}
	
	if me.ToError() != nil {
		t.Error("ToError should return nil for empty MultiError")
	}
}

func TestMultiError_AddErrors(t *testing.T) {
	me := NewMultiError()
	
	// Add nil error (should be ignored)
	me.Add(nil)
	if me.HasErrors() {
		t.Error("Adding nil error should not create errors")
	}
	
	// Add real errors
	err1 := context.Canceled
	err2 := context.DeadlineExceeded
	
	me.Add(err1)
	me.Add(err2)
	
	if !me.HasErrors() {
		t.Error("MultiError should have errors")
	}
	
	if me.Count() != 2 {
		t.Errorf("Expected 2 errors, got %d", me.Count())
	}
	
	errors := me.Errors()
	if len(errors) != 2 {
		t.Errorf("Expected 2 errors in slice, got %d", len(errors))
	}
	
	if errors[0] != err1 || errors[1] != err2 {
		t.Error("Errors not in expected order")
	}
}

func TestMultiError_ConcurrentAccess(t *testing.T) {
	me := NewMultiError()
	
	var wg sync.WaitGroup
	numGoroutines := 100
	errorsPerGoroutine := 10
	
	wg.Add(numGoroutines)
	
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < errorsPerGoroutine; j++ {
				me.Add(context.Canceled)
			}
		}(i)
	}
	
	wg.Wait()
	
	expectedCount := numGoroutines * errorsPerGoroutine
	if me.Count() != expectedCount {
		t.Errorf("Expected %d errors, got %d", expectedCount, me.Count())
	}
}

// --------------------- Queue Tests ---------------------

func TestBoundedQueue_Basic(t *testing.T) {
	q := NewBoundedQueue(3, OverflowReject)
	
	if q.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", q.Len())
	}
	
	if q.Capacity() != 3 {
		t.Errorf("Expected capacity 3, got %d", q.Capacity())
	}
	
	// Push items
	for i := 0; i < 3; i++ {
		err := q.Push(i)
		if err != nil {
			t.Errorf("Failed to push item %d: %v", i, err)
		}
	}
	
	if q.Len() != 3 {
		t.Errorf("Expected length 3, got %d", q.Len())
	}
	
	// Queue should be full, next push should fail with OverflowReject
	err := q.Push(3)
	if err != ErrQueueFull {
		t.Errorf("Expected ErrQueueFull, got %v", err)
	}
	
	// Pop items
	for i := 0; i < 3; i++ {
		item, ok := q.Pop()
		if !ok {
			t.Errorf("Failed to pop item %d", i)
		}
		if item.(int) != i {
			t.Errorf("Expected item %d, got %v", i, item)
		}
	}
	
	if q.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", q.Len())
	}
	
	// Pop from empty queue
	item, ok := q.Pop()
	if ok {
		t.Errorf("Expected false for empty queue pop, got item %v", item)
	}
}

func TestBoundedQueue_OverflowPolicies(t *testing.T) {
	tests := []struct {
		name   string
		policy OverflowPolicy
	}{
		{"DropOldest", OverflowDropOldest},
		{"DropNewest", OverflowDropNewest},
		{"Reject", OverflowReject},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := NewBoundedQueue(2, tt.policy)
			
			// Fill queue
			q.Push(1)
			q.Push(2)
			
			// Test overflow behavior
			switch tt.policy {
			case OverflowDropOldest:
				err := q.Push(3)
				if err != nil {
					t.Errorf("DropOldest should not return error, got %v", err)
				}
				item, _ := q.Pop()
				if item.(int) != 2 { // 1 should be dropped
					t.Errorf("Expected 2 (oldest dropped), got %v", item)
				}
				
			case OverflowDropNewest:
				err := q.Push(3)
				if err != ErrItemDropped {
					t.Errorf("Expected ErrItemDropped, got %v", err)
				}
				item, _ := q.Pop()
				if item.(int) != 1 { // 3 should be dropped
					t.Errorf("Expected 1 (newest dropped), got %v", item)
				}
				
			case OverflowReject:
				err := q.Push(3)
				if err != ErrQueueFull {
					t.Errorf("Expected ErrQueueFull, got %v", err)
				}
			}
		})
	}
}

func TestPriorityQueue(t *testing.T) {
	pq := NewPriorityQueue()
	
	if pq.Len() != 0 {
		t.Errorf("Expected empty priority queue, got length %d", pq.Len())
	}
	
	// Push items with different priorities
	items := []struct {
		value    string
		priority int
	}{
		{"low", 1},
		{"high", 10},
		{"medium", 5},
		{"highest", 15},
	}
	
	for _, item := range items {
		err := pq.Push(item.value, item.priority)
		if err != nil {
			t.Errorf("Failed to push item: %v", err)
		}
	}
	
	if pq.Len() != len(items) {
		t.Errorf("Expected length %d, got %d", len(items), pq.Len())
	}
	
	// Peek at highest priority
	value, priority, ok := pq.Peek()
	if !ok {
		t.Error("Peek should return true for non-empty queue")
	}
	if value.(string) != "highest" || priority != 15 {
		t.Errorf("Expected highest priority item, got %v with priority %d", value, priority)
	}
	
	// Pop items (should come out in priority order)
	expected := []string{"highest", "high", "medium", "low"}
	for i, exp := range expected {
		value, ok := pq.Pop()
		if !ok {
			t.Errorf("Failed to pop item %d", i)
		}
		if value.(string) != exp {
			t.Errorf("Expected %s, got %v", exp, value)
		}
	}
	
	if pq.Len() != 0 {
		t.Errorf("Expected empty queue, got length %d", pq.Len())
	}
}

// --------------------- Context Utilities Tests ---------------------

func TestMergeContexts(t *testing.T) {
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	
	merged, cancel := MergeContexts(ctx1, ctx2)
	defer cancel()
	
	// Cancel ctx1, merged should be cancelled
	cancel1()
	
	select {
	case <-merged.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Merged context should be cancelled when any input context is cancelled")
	}
	
	cancel2() // cleanup
}

func TestWaitGroupWithContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	
	wgc := NewWaitGroupWithContext(ctx)
	
	wgc.Add(1)
	
	// This should timeout
	err := wgc.Wait()
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}

// --------------------- Synchronization Tests ---------------------

func TestAtomicBool(t *testing.T) {
	ab := NewAtomicBool(false)
	
	if ab.Load() {
		t.Error("Expected false initial value")
	}
	
	ab.Store(true)
	if !ab.Load() {
		t.Error("Expected true after Store(true)")
	}
	
	old := ab.Swap(false)
	if !old {
		t.Error("Expected true from Swap")
	}
	if ab.Load() {
		t.Error("Expected false after Swap(false)")
	}
	
	new := ab.Toggle()
	if !new {
		t.Error("Expected true from Toggle")
	}
	if !ab.Load() {
		t.Error("Expected true after Toggle")
	}
}

func TestAtomicCounter(t *testing.T) {
	ac := NewAtomicCounter(5)
	
	if ac.Load() != 5 {
		t.Errorf("Expected 5 initial value, got %d", ac.Load())
	}
	
	new := ac.Inc()
	if new != 6 {
		t.Errorf("Expected 6 from Inc, got %d", new)
	}
	
	new = ac.Dec()
	if new != 5 {
		t.Errorf("Expected 5 from Dec, got %d", new)
	}
	
	new = ac.Add(10)
	if new != 15 {
		t.Errorf("Expected 15 from Add(10), got %d", new)
	}
	
	old := ac.Reset()
	if old != 15 {
		t.Errorf("Expected 15 from Reset, got %d", old)
	}
	if ac.Load() != 0 {
		t.Errorf("Expected 0 after Reset, got %d", ac.Load())
	}
}

func TestSemaphore(t *testing.T) {
	sem := NewSemaphore(2)
	
	if sem.Available() != 2 {
		t.Errorf("Expected 2 available permits, got %d", sem.Available())
	}
	
	// Acquire permits
	if !sem.TryAcquire() {
		t.Error("First TryAcquire should succeed")
	}
	if !sem.TryAcquire() {
		t.Error("Second TryAcquire should succeed")
	}
	if sem.TryAcquire() {
		t.Error("Third TryAcquire should fail")
	}
	
	if sem.Available() != 0 {
		t.Errorf("Expected 0 available permits, got %d", sem.Available())
	}
	
	// Release one permit
	sem.Release()
	if sem.Available() != 1 {
		t.Errorf("Expected 1 available permit after release, got %d", sem.Available())
	}
	
	// Test successful acquire with timeout
	if !sem.AcquireWithTimeout(50 * time.Millisecond) {
		t.Error("AcquireWithTimeout should succeed when permits available")
	}
	
	// Now test timeout when no permits available
	if sem.AcquireWithTimeout(50 * time.Millisecond) {
		t.Error("AcquireWithTimeout should fail when no permits available")
	}
}

func TestLatch(t *testing.T) {
	latch := NewLatch()
	
	if latch.IsReleased() {
		t.Error("New latch should not be released")
	}
	
	// Test timeout
	err := latch.AwaitWithTimeout(50 * time.Millisecond)
	if err != ErrTimeout {
		t.Errorf("Expected ErrTimeout, got %v", err)
	}
	
	// Release latch
	latch.CountDown()
	
	if !latch.IsReleased() {
		t.Error("Latch should be released after CountDown")
	}
	
	// Await should return immediately
	done := make(chan struct{})
	go func() {
		latch.Await()
		close(done)
	}()
	
	select {
	case <-done:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Await should return immediately for released latch")
	}
}

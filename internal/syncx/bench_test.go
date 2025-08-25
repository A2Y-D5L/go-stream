package syncx

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Worker Pool Benchmarks

func BenchmarkWorkerPool_Submit(b *testing.B) {
	wp := NewWorkerPool(WorkerConfig{
		Workers:   4,
		QueueSize: 1000,
		Metrics:   false,
	})
	
	err := wp.Start()
	if err != nil {
		b.Fatalf("Failed to start worker pool: %v", err)
	}
	defer wp.Stop(context.Background())
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wp.Submit(func() {
				// Minimal work
			})
		}
	})
}

func BenchmarkWorkerPool_SubmitWithWork(b *testing.B) {
	wp := NewWorkerPool(WorkerConfig{
		Workers:   4,
		QueueSize: 1000,
		Metrics:   false,
	})
	
	err := wp.Start()
	if err != nil {
		b.Fatalf("Failed to start worker pool: %v", err)
	}
	defer wp.Stop(context.Background())
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			wp.Submit(func() {
				// Simulate some work
				time.Sleep(1 * time.Microsecond)
			})
		}
	})
}

// Multi-Error Benchmarks

func BenchmarkMultiError_Add(b *testing.B) {
	me := NewMultiError()
	err := context.Canceled
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			me.Add(err)
		}
	})
}

func BenchmarkMultiError_AddConcurrent(b *testing.B) {
	me := NewMultiError()
	err := context.Canceled
	
	b.ResetTimer()
	
	var wg sync.WaitGroup
	numGoroutines := 100
	
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N/numGoroutines; j++ {
				me.Add(err)
			}
		}()
	}
	
	wg.Wait()
}

// Queue Benchmarks

func BenchmarkBoundedQueue_Push(b *testing.B) {
	q := NewBoundedQueue(1000, OverflowDropOldest)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Push(i)
	}
}

func BenchmarkBoundedQueue_Pop(b *testing.B) {
	q := NewBoundedQueue(1000, OverflowReject)
	
	// Pre-fill queue
	for i := 0; i < 1000; i++ {
		q.Push(i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Pop()
		if q.Len() == 0 {
			// Refill
			for j := 0; j < 1000; j++ {
				q.Push(j)
			}
		}
	}
}

func BenchmarkBoundedQueue_PushPop(b *testing.B) {
	q := NewBoundedQueue(100, OverflowReject)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				q.Push(i)
			} else {
				q.Pop()
			}
			i++
		}
	})
}

func BenchmarkPriorityQueue_Push(b *testing.B) {
	pq := NewPriorityQueue()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pq.Push(i, i%100)
	}
}

func BenchmarkPriorityQueue_Pop(b *testing.B) {
	pq := NewPriorityQueue()
	
	// Pre-fill queue
	for i := 0; i < 1000; i++ {
		pq.Push(i, i%100)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pq.Pop()
		if pq.Len() == 0 {
			// Refill
			for j := 0; j < 1000; j++ {
				pq.Push(j, j%100)
			}
		}
	}
}

// Atomic Operations Benchmarks

func BenchmarkAtomicBool_Load(b *testing.B) {
	ab := NewAtomicBool(true)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ab.Load()
		}
	})
}

func BenchmarkAtomicBool_Store(b *testing.B) {
	ab := NewAtomicBool(false)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ab.Store(true)
		}
	})
}

func BenchmarkAtomicBool_CompareAndSwap(b *testing.B) {
	ab := NewAtomicBool(false)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ab.CompareAndSwap(false, true)
			ab.CompareAndSwap(true, false)
		}
	})
}

func BenchmarkAtomicCounter_Inc(b *testing.B) {
	ac := NewAtomicCounter(0)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ac.Inc()
		}
	})
}

func BenchmarkAtomicCounter_Add(b *testing.B) {
	ac := NewAtomicCounter(0)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ac.Add(1)
		}
	})
}

// Synchronization Benchmarks

func BenchmarkSemaphore_AcquireRelease(b *testing.B) {
	sem := NewSemaphore(10)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if sem.TryAcquire() {
				sem.Release()
			}
		}
	})
}

func BenchmarkTimeoutMutex_LockUnlock(b *testing.B) {
	tm := NewTimeoutMutex()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.Lock()
		tm.Unlock()
	}
}

func BenchmarkTimeoutMutex_TryLock(b *testing.B) {
	tm := NewTimeoutMutex()
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if tm.TryLock() {
				tm.Unlock()
			}
		}
	})
}

// Context Utilities Benchmarks

func BenchmarkMergeContexts_Two(b *testing.B) {
	ctx1 := context.Background()
	ctx2 := context.Background()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merged, cancel := MergeContexts(ctx1, ctx2)
		cancel()
		_ = merged
	}
}

func BenchmarkMergeContexts_Five(b *testing.B) {
	ctxs := make([]context.Context, 5)
	for i := range ctxs {
		ctxs[i] = context.Background()
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merged, cancel := MergeContexts(ctxs...)
		cancel()
		_ = merged
	}
}

// Comparison Benchmarks

func BenchmarkStdMutex_LockUnlock(b *testing.B) {
	var mu sync.Mutex
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		mu.Unlock()
	}
}

func BenchmarkStdRWMutex_RLockRUnlock(b *testing.B) {
	var mu sync.RWMutex
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.RLock()
			mu.RUnlock()
		}
	})
}

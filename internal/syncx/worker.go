package syncx

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool represents a pool of workers that process tasks concurrently
type WorkerPool struct {
	workers      int
	queue        chan func()
	quit         chan struct{}
	wg           sync.WaitGroup
	metrics      *WorkerMetrics
	backpressure BackpressurePolicy

	started int32
	stopped int32
}

// WorkerConfig holds configuration for creating a worker pool
type WorkerConfig struct {
	Workers      int
	QueueSize    int
	Backpressure BackpressurePolicy
	Metrics      bool
}

// BackpressurePolicy defines how the worker pool handles queue overflow
type BackpressurePolicy int

const (
	BackpressureBlock BackpressurePolicy = iota
	BackpressureDropOldest
	BackpressureDropNewest
)

// WorkerMetrics tracks worker pool performance metrics
type WorkerMetrics struct {
	TasksSubmitted uint64
	TasksCompleted uint64
	TasksDropped   uint64
	QueueDepth     int64
	ActiveWorkers  int64
}

// NewWorkerPool creates a new worker pool with the given configuration
func NewWorkerPool(config WorkerConfig) *WorkerPool {
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 100
	}

	wp := &WorkerPool{
		workers:      config.Workers,
		queue:        make(chan func(), config.QueueSize),
		quit:         make(chan struct{}),
		backpressure: config.Backpressure,
	}

	if config.Metrics {
		wp.metrics = &WorkerMetrics{}
	}

	return wp
}

// Start begins the worker pool operation
func (wp *WorkerPool) Start() error {
	if !atomic.CompareAndSwapInt32(&wp.started, 0, 1) {
		return ErrWorkerPoolAlreadyStarted
	}

	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}

	return nil
}

// Submit submits a task to the worker pool
func (wp *WorkerPool) Submit(task func()) error {
	if atomic.LoadInt32(&wp.stopped) == 1 {
		return ErrWorkerPoolStopped
	}

	if wp.metrics != nil {
		atomic.AddUint64(&wp.metrics.TasksSubmitted, 1)
		atomic.StoreInt64(&wp.metrics.QueueDepth, int64(len(wp.queue)))
	}

	select {
	case wp.queue <- task:
		return nil
	default:
		// Queue is full, apply backpressure policy
		return wp.handleBackpressure(task)
	}
}

// SubmitWithTimeout submits a task with a timeout
func (wp *WorkerPool) SubmitWithTimeout(task func(), timeout time.Duration) error {
	if atomic.LoadInt32(&wp.stopped) == 1 {
		return ErrWorkerPoolStopped
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case wp.queue <- task:
		if wp.metrics != nil {
			atomic.AddUint64(&wp.metrics.TasksSubmitted, 1)
			atomic.StoreInt64(&wp.metrics.QueueDepth, int64(len(wp.queue)))
		}
		return nil
	case <-timer.C:
		return ErrTimeout
	}
}

// Stop gracefully stops the worker pool
func (wp *WorkerPool) Stop(ctx context.Context) error {
	if !atomic.CompareAndSwapInt32(&wp.stopped, 0, 1) {
		return ErrWorkerPoolAlreadyStopped
	}

	close(wp.quit)

	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Metrics returns the current worker pool metrics
func (wp *WorkerPool) Metrics() WorkerMetrics {
	if wp.metrics == nil {
		return WorkerMetrics{}
	}

	return WorkerMetrics{
		TasksSubmitted: atomic.LoadUint64(&wp.metrics.TasksSubmitted),
		TasksCompleted: atomic.LoadUint64(&wp.metrics.TasksCompleted),
		TasksDropped:   atomic.LoadUint64(&wp.metrics.TasksDropped),
		QueueDepth:     atomic.LoadInt64(&wp.metrics.QueueDepth),
		ActiveWorkers:  atomic.LoadInt64(&wp.metrics.ActiveWorkers),
	}
}

// QueueDepth returns the current number of tasks in the queue
func (wp *WorkerPool) QueueDepth() int {
	return len(wp.queue)
}

// IsStarted returns true if the worker pool has been started
func (wp *WorkerPool) IsStarted() bool {
	return atomic.LoadInt32(&wp.started) == 1
}

// IsStopped returns true if the worker pool has been stopped
func (wp *WorkerPool) IsStopped() bool {
	return atomic.LoadInt32(&wp.stopped) == 1
}

// worker is the main worker loop
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	if wp.metrics != nil {
		atomic.AddInt64(&wp.metrics.ActiveWorkers, 1)
		defer atomic.AddInt64(&wp.metrics.ActiveWorkers, -1)
	}

	for {
		select {
		case task := <-wp.queue:
			if task != nil {
				task()
				if wp.metrics != nil {
					atomic.AddUint64(&wp.metrics.TasksCompleted, 1)
					atomic.StoreInt64(&wp.metrics.QueueDepth, int64(len(wp.queue)))
				}
			}
		case <-wp.quit:
			return
		}
	}
}

// handleBackpressure applies the configured backpressure policy
func (wp *WorkerPool) handleBackpressure(task func()) error {
	switch wp.backpressure {
	case BackpressureBlock:
		// Block until space is available
		select {
		case wp.queue <- task:
			return nil
		case <-wp.quit:
			return ErrWorkerPoolStopped
		}

	case BackpressureDropOldest:
		// Remove oldest task and add new one
		select {
		case <-wp.queue:
			if wp.metrics != nil {
				atomic.AddUint64(&wp.metrics.TasksDropped, 1)
			}
		default:
		}

		select {
		case wp.queue <- task:
			return nil
		case <-wp.quit:
			return ErrWorkerPoolStopped
		}

	case BackpressureDropNewest:
		// Drop the new task
		if wp.metrics != nil {
			atomic.AddUint64(&wp.metrics.TasksDropped, 1)
		}
		return ErrTaskDropped

	default:
		return ErrUnknownBackpressurePolicy
	}
}

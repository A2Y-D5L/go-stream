package syncx

import "errors"

// Worker pool errors
var (
	ErrWorkerPoolAlreadyStarted     = errors.New("worker pool already started")
	ErrWorkerPoolStopped            = errors.New("worker pool is stopped")
	ErrWorkerPoolAlreadyStopped     = errors.New("worker pool already stopped")
	ErrTaskDropped                  = errors.New("task dropped due to backpressure")
	ErrTimeout                      = errors.New("operation timed out")
	ErrUnknownBackpressurePolicy    = errors.New("unknown backpressure policy")
)

// Queue errors
var (
	ErrQueueClosed               = errors.New("queue is closed")
	ErrQueueFull                 = errors.New("queue is full")
	ErrItemDropped               = errors.New("item dropped due to overflow policy")
	ErrUnknownOverflowPolicy     = errors.New("unknown overflow policy")
)

// Context errors
var (
	ErrContextMerge = errors.New("context merge failed")
)

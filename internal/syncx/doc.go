// Package syncx provides enhanced synchronization utilities for the go-stream library.
//
// This package contains internal utilities that support concurrent operations,
// worker pool management, error collection, and advanced synchronization primitives.
//
// Key Components:
//
// • WorkerPool: Enhanced bounded worker pool with configurable backpressure policies
// • MultiError: Thread-safe error collection and reporting
// • Queue: Bounded and priority queue implementations with overflow policies
// • Context utilities: Context merging, timeout handling, and cancellation helpers
// • Synchronization primitives: Atomic types, mutex groups, semaphores, and barriers
//
// These utilities are designed for high-performance, concurrent messaging scenarios
// and provide the foundation for the go-stream library's internal operations.
package syncx

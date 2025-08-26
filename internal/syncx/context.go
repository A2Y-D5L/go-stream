package syncx

import (
	"context"
	"reflect"
	"sync"
	"time"
)

// MergeContexts creates a context that is cancelled when any of the input contexts are cancelled
func MergeContexts(ctxs ...context.Context) (context.Context, context.CancelFunc) {
	if len(ctxs) == 0 {
		return context.WithCancel(context.Background())
	}

	if len(ctxs) == 1 {
		return context.WithCancel(ctxs[0])
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()

		cases := make([]reflect.SelectCase, len(ctxs))
		for i, c := range ctxs {
			cases[i] = reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(c.Done()),
			}
		}

		reflect.Select(cases)
	}()

	return ctx, cancel
}

// WithTimeoutFallback creates a context with timeout and fallback behavior
func WithTimeoutFallback(parent context.Context, timeout time.Duration, fallback func()) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, timeout)

	go func() {
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded && fallback != nil {
			fallback()
		}
	}()

	return ctx, cancel
}

// WithValue creates a context with multiple key-value pairs
func WithValues(parent context.Context, keyValues ...interface{}) context.Context {
	if len(keyValues)%2 != 0 {
		panic("keyValues must contain an even number of elements")
	}

	ctx := parent
	for i := 0; i < len(keyValues); i += 2 {
		key := keyValues[i]
		value := keyValues[i+1]
		ctx = context.WithValue(ctx, key, value)
	}

	return ctx
}

// WaitGroupWithContext wraps sync.WaitGroup with context cancellation
type WaitGroupWithContext struct {
	wg  sync.WaitGroup
	ctx context.Context
}

// NewWaitGroupWithContext creates a new WaitGroupWithContext
func NewWaitGroupWithContext(ctx context.Context) *WaitGroupWithContext {
	return &WaitGroupWithContext{ctx: ctx}
}

// Add adds delta to the WaitGroup counter
func (wgc *WaitGroupWithContext) Add(delta int) {
	wgc.wg.Add(delta)
}

// Done decrements the WaitGroup counter
func (wgc *WaitGroupWithContext) Done() {
	wgc.wg.Done()
}

// Wait waits for the WaitGroup counter to reach zero or for context cancellation
func (wgc *WaitGroupWithContext) Wait() error {
	done := make(chan struct{})

	go func() {
		wgc.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-wgc.ctx.Done():
		return wgc.ctx.Err()
	}
}

// WaitWithTimeout waits for the WaitGroup with a specific timeout
func (wgc *WaitGroupWithContext) WaitWithTimeout(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(wgc.ctx, timeout)
	defer cancel()

	done := make(chan struct{})

	go func() {
		wgc.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Barrier provides a synchronization barrier for multiple goroutines
type Barrier struct {
	n        int
	count    int
	mu       sync.Mutex
	cond     *sync.Cond
	released bool
}

// NewBarrier creates a new barrier for n goroutines
func NewBarrier(n int) *Barrier {
	b := &Barrier{n: n}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// Wait waits for all goroutines to reach the barrier
func (b *Barrier) Wait() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.released {
		return
	}

	b.count++
	if b.count == b.n {
		b.released = true
		b.cond.Broadcast()
	} else {
		for !b.released {
			b.cond.Wait()
		}
	}
}

// WaitWithContext waits for the barrier with context cancellation
func (b *Barrier) WaitWithContext(ctx context.Context) error {
	done := make(chan struct{})

	go func() {
		b.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Reset resets the barrier to be reused
func (b *Barrier) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.count = 0
	b.released = false
}

// Latch provides a one-time synchronization primitive
type Latch struct {
	done chan struct{}
	once sync.Once
}

// NewLatch creates a new latch
func NewLatch() *Latch {
	return &Latch{
		done: make(chan struct{}),
	}
}

// CountDown releases the latch, allowing all waiting goroutines to proceed
func (l *Latch) CountDown() {
	l.once.Do(func() {
		close(l.done)
	})
}

// Await waits for the latch to be released
func (l *Latch) Await() {
	<-l.done
}

// AwaitWithContext waits for the latch with context cancellation
func (l *Latch) AwaitWithContext(ctx context.Context) error {
	select {
	case <-l.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AwaitWithTimeout waits for the latch with a timeout
func (l *Latch) AwaitWithTimeout(timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-l.done:
		return nil
	case <-timer.C:
		return ErrTimeout
	}
}

// IsReleased returns true if the latch has been released
func (l *Latch) IsReleased() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

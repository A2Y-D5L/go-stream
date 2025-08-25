package syncx

import (
	"sync"
	"sync/atomic"
	"time"
)

// AtomicBool provides an atomic boolean with convenient methods
type AtomicBool struct {
	value int32
}

// NewAtomicBool creates a new AtomicBool with the specified initial value
func NewAtomicBool(initial bool) *AtomicBool {
	ab := &AtomicBool{}
	if initial {
		ab.value = 1
	}
	return ab
}

// Load returns the current value
func (ab *AtomicBool) Load() bool {
	return atomic.LoadInt32(&ab.value) != 0
}

// Store sets the value
func (ab *AtomicBool) Store(value bool) {
	var newValue int32
	if value {
		newValue = 1
	}
	atomic.StoreInt32(&ab.value, newValue)
}

// CompareAndSwap atomically compares and swaps the value
func (ab *AtomicBool) CompareAndSwap(old, new bool) bool {
	var oldValue, newValue int32
	if old {
		oldValue = 1
	}
	if new {
		newValue = 1
	}
	return atomic.CompareAndSwapInt32(&ab.value, oldValue, newValue)
}

// Swap atomically swaps the value and returns the old value
func (ab *AtomicBool) Swap(new bool) bool {
	var newValue int32
	if new {
		newValue = 1
	}
	oldValue := atomic.SwapInt32(&ab.value, newValue)
	return oldValue != 0
}

// Toggle atomically toggles the value and returns the new value
func (ab *AtomicBool) Toggle() bool {
	for {
		current := atomic.LoadInt32(&ab.value)
		new := 1 - current
		if atomic.CompareAndSwapInt32(&ab.value, current, new) {
			return new != 0
		}
	}
}

// AtomicCounter provides an atomic counter with convenient methods
type AtomicCounter struct {
	value int64
}

// NewAtomicCounter creates a new AtomicCounter with the specified initial value
func NewAtomicCounter(initial int64) *AtomicCounter {
	return &AtomicCounter{value: initial}
}

// Load returns the current value
func (ac *AtomicCounter) Load() int64 {
	return atomic.LoadInt64(&ac.value)
}

// Store sets the value
func (ac *AtomicCounter) Store(value int64) {
	atomic.StoreInt64(&ac.value, value)
}

// Add adds delta to the counter and returns the new value
func (ac *AtomicCounter) Add(delta int64) int64 {
	return atomic.AddInt64(&ac.value, delta)
}

// Inc increments the counter by 1 and returns the new value
func (ac *AtomicCounter) Inc() int64 {
	return atomic.AddInt64(&ac.value, 1)
}

// Dec decrements the counter by 1 and returns the new value
func (ac *AtomicCounter) Dec() int64 {
	return atomic.AddInt64(&ac.value, -1)
}

// CompareAndSwap atomically compares and swaps the value
func (ac *AtomicCounter) CompareAndSwap(old, new int64) bool {
	return atomic.CompareAndSwapInt64(&ac.value, old, new)
}

// Swap atomically swaps the value and returns the old value
func (ac *AtomicCounter) Swap(new int64) int64 {
	return atomic.SwapInt64(&ac.value, new)
}

// Reset sets the counter to zero and returns the old value
func (ac *AtomicCounter) Reset() int64 {
	return atomic.SwapInt64(&ac.value, 0)
}

// RWMutexGroup manages a group of RWMutexes for fine-grained locking
type RWMutexGroup struct {
	mutexes []sync.RWMutex
	size    int
}

// NewRWMutexGroup creates a new RWMutexGroup with the specified number of mutexes
func NewRWMutexGroup(size int) *RWMutexGroup {
	if size <= 0 {
		size = 16 // default size
	}
	return &RWMutexGroup{
		mutexes: make([]sync.RWMutex, size),
		size:    size,
	}
}

// Lock locks the mutex for the given key
func (mg *RWMutexGroup) Lock(key string) {
	index := mg.hash(key) % mg.size
	mg.mutexes[index].Lock()
}

// Unlock unlocks the mutex for the given key
func (mg *RWMutexGroup) Unlock(key string) {
	index := mg.hash(key) % mg.size
	mg.mutexes[index].Unlock()
}

// RLock locks the mutex for reading for the given key
func (mg *RWMutexGroup) RLock(key string) {
	index := mg.hash(key) % mg.size
	mg.mutexes[index].RLock()
}

// RUnlock unlocks the read lock for the given key
func (mg *RWMutexGroup) RUnlock(key string) {
	index := mg.hash(key) % mg.size
	mg.mutexes[index].RUnlock()
}

// hash computes a simple hash for a string
func (mg *RWMutexGroup) hash(s string) int {
	h := 0
	for _, c := range s {
		h = 31*h + int(c)
	}
	if h < 0 {
		h = -h
	}
	return h
}

// OnceGroup provides a group of sync.Once instances for different keys
type OnceGroup struct {
	mu    sync.Mutex
	onces map[string]*sync.Once
}

// NewOnceGroup creates a new OnceGroup
func NewOnceGroup() *OnceGroup {
	return &OnceGroup{
		onces: make(map[string]*sync.Once),
	}
}

// Do calls the function f if and only if Do is being called for the first time for the given key
func (og *OnceGroup) Do(key string, f func()) {
	og.mu.Lock()
	once, exists := og.onces[key]
	if !exists {
		once = &sync.Once{}
		og.onces[key] = once
	}
	og.mu.Unlock()
	
	once.Do(f)
}

// Reset removes the once instance for the given key, allowing Do to be called again
func (og *OnceGroup) Reset(key string) {
	og.mu.Lock()
	delete(og.onces, key)
	og.mu.Unlock()
}

// Clear removes all once instances
func (og *OnceGroup) Clear() {
	og.mu.Lock()
	for key := range og.onces {
		delete(og.onces, key)
	}
	og.mu.Unlock()
}

// TimeoutMutex is a mutex with timeout support
type TimeoutMutex struct {
	ch   chan struct{}
	once sync.Once
}

// NewTimeoutMutex creates a new TimeoutMutex
func NewTimeoutMutex() *TimeoutMutex {
	return &TimeoutMutex{
		ch: make(chan struct{}, 1),
	}
}

// init initializes the channel with a token
func (tm *TimeoutMutex) init() {
	tm.once.Do(func() {
		tm.ch <- struct{}{}
	})
}

// Lock locks the mutex
func (tm *TimeoutMutex) Lock() {
	tm.init()
	<-tm.ch
}

// TryLock attempts to lock the mutex without blocking
func (tm *TimeoutMutex) TryLock() bool {
	tm.init()
	select {
	case <-tm.ch:
		return true
	default:
		return false
	}
}

// LockWithTimeout attempts to lock the mutex with a timeout
func (tm *TimeoutMutex) LockWithTimeout(timeout time.Duration) bool {
	tm.init()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	
	select {
	case <-tm.ch:
		return true
	case <-timer.C:
		return false
	}
}

// Unlock unlocks the mutex
func (tm *TimeoutMutex) Unlock() {
	select {
	case tm.ch <- struct{}{}:
	default:
		panic("unlock of unlocked TimeoutMutex")
	}
}

// Semaphore provides a counting semaphore
type Semaphore struct {
	ch chan struct{}
}

// NewSemaphore creates a new semaphore with the specified capacity
func NewSemaphore(capacity int) *Semaphore {
	return &Semaphore{
		ch: make(chan struct{}, capacity),
	}
}

// Acquire acquires a permit from the semaphore
func (s *Semaphore) Acquire() {
	s.ch <- struct{}{}
}

// TryAcquire attempts to acquire a permit without blocking
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.ch <- struct{}{}:
		return true
	default:
		return false
	}
}

// AcquireWithTimeout attempts to acquire a permit with a timeout
func (s *Semaphore) AcquireWithTimeout(timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	
	select {
	case s.ch <- struct{}{}:
		return true
	case <-timer.C:
		return false
	}
}

// Release releases a permit back to the semaphore
func (s *Semaphore) Release() {
	select {
	case <-s.ch:
	default:
		panic("release without acquire")
	}
}

// Available returns the number of available permits
func (s *Semaphore) Available() int {
	return cap(s.ch) - len(s.ch)
}

// DrainPermits acquires all available permits and returns the count
func (s *Semaphore) DrainPermits() int {
	count := 0
	for s.TryAcquire() {
		count++
	}
	return count
}

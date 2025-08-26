package syncx

import (
	"container/heap"
	"sync"
	"time"
)

// Queue defines a generic queue interface
type Queue interface {
	Push(item interface{}) error
	Pop() (interface{}, bool)
	Len() int
	Drain() []interface{}
	Close()
}

// OverflowPolicy defines how a queue handles overflow situations
type OverflowPolicy int

const (
	OverflowBlock OverflowPolicy = iota
	OverflowDropOldest
	OverflowDropNewest
	OverflowReject
)

// BoundedQueue implements a thread-safe bounded queue with overflow policies
type BoundedQueue struct {
	items    []interface{}
	capacity int
	mu       sync.Mutex
	notEmpty *sync.Cond
	closed   bool
	policy   OverflowPolicy
}

// NewBoundedQueue creates a new bounded queue with the specified capacity and overflow policy
func NewBoundedQueue(capacity int, policy OverflowPolicy) *BoundedQueue {
	if capacity <= 0 {
		capacity = 100
	}

	bq := &BoundedQueue{
		items:    make([]interface{}, 0, capacity),
		capacity: capacity,
		policy:   policy,
	}
	bq.notEmpty = sync.NewCond(&bq.mu)
	return bq
}

// Push adds an item to the queue according to the overflow policy
func (bq *BoundedQueue) Push(item interface{}) error {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if bq.closed {
		return ErrQueueClosed
	}

	if len(bq.items) < bq.capacity {
		bq.items = append(bq.items, item)
		bq.notEmpty.Signal()
		return nil
	}

	// Queue is full, apply overflow policy
	switch bq.policy {
	case OverflowDropOldest:
		bq.items = bq.items[1:]
		bq.items = append(bq.items, item)
		bq.notEmpty.Signal()
		return nil

	case OverflowDropNewest:
		return ErrItemDropped

	case OverflowReject:
		return ErrQueueFull

	case OverflowBlock:
		// For blocking, we would need a more complex implementation
		// For now, we'll reject when full in blocking mode
		return ErrQueueFull

	default:
		return ErrUnknownOverflowPolicy
	}
}

// Pop removes and returns an item from the queue
func (bq *BoundedQueue) Pop() (interface{}, bool) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	if len(bq.items) > 0 {
		item := bq.items[0]
		bq.items = bq.items[1:]
		return item, true
	}

	return nil, false
}

// PopBlocking removes and returns an item from the queue, blocking until available
func (bq *BoundedQueue) PopBlocking() (interface{}, bool) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	for len(bq.items) == 0 && !bq.closed {
		bq.notEmpty.Wait()
	}

	if len(bq.items) == 0 {
		return nil, false
	}

	item := bq.items[0]
	bq.items = bq.items[1:]
	return item, true
}

// PopWithTimeout removes and returns an item with a timeout
func (bq *BoundedQueue) PopWithTimeout(timeout time.Duration) (interface{}, bool) {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	// If items are available, return immediately
	if len(bq.items) > 0 {
		item := bq.items[0]
		bq.items = bq.items[1:]
		return item, true
	}

	// If queue is closed, return false
	if bq.closed {
		return nil, false
	}

	// Wait with timeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	done := make(chan struct{})
	go func() {
		bq.notEmpty.Wait()
		close(done)
	}()

	bq.mu.Unlock()
	select {
	case <-done:
		bq.mu.Lock()
		if len(bq.items) > 0 {
			item := bq.items[0]
			bq.items = bq.items[1:]
			return item, true
		}
		return nil, false
	case <-timer.C:
		return nil, false
	}
}

// Len returns the current number of items in the queue
func (bq *BoundedQueue) Len() int {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return len(bq.items)
}

// Drain removes and returns all items from the queue
func (bq *BoundedQueue) Drain() []interface{} {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	items := make([]interface{}, len(bq.items))
	copy(items, bq.items)
	bq.items = bq.items[:0]
	return items
}

// Close closes the queue, preventing new items from being added
func (bq *BoundedQueue) Close() {
	bq.mu.Lock()
	defer bq.mu.Unlock()

	bq.closed = true
	bq.notEmpty.Broadcast()
}

// IsClosed returns true if the queue is closed
func (bq *BoundedQueue) IsClosed() bool {
	bq.mu.Lock()
	defer bq.mu.Unlock()
	return bq.closed
}

// Capacity returns the maximum capacity of the queue
func (bq *BoundedQueue) Capacity() int {
	return bq.capacity
}

// PriorityQueue implements a thread-safe priority queue
type PriorityQueue struct {
	items  *priorityHeap
	mu     sync.Mutex
	closed bool
}

// PriorityItem represents an item in the priority queue
type PriorityItem struct {
	Value    interface{}
	Priority int
	Index    int
}

type priorityHeap []*PriorityItem

func (pq priorityHeap) Len() int { return len(pq) }

func (pq priorityHeap) Less(i, j int) bool {
	return pq[i].Priority > pq[j].Priority // Higher priority first
}

func (pq priorityHeap) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *priorityHeap) Push(x interface{}) {
	n := len(*pq)
	item := x.(*PriorityItem)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *priorityHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	*pq = old[0 : n-1]
	return item
}

// NewPriorityQueue creates a new priority queue
func NewPriorityQueue() *PriorityQueue {
	return &PriorityQueue{
		items: &priorityHeap{},
	}
}

// Push adds an item with the specified priority
func (pq *PriorityQueue) Push(value interface{}, priority int) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return ErrQueueClosed
	}

	item := &PriorityItem{
		Value:    value,
		Priority: priority,
	}

	heap.Push(pq.items, item)
	return nil
}

// Pop removes and returns the highest priority item
func (pq *PriorityQueue) Pop() (interface{}, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.items.Len() == 0 {
		return nil, false
	}

	item := heap.Pop(pq.items).(*PriorityItem)
	return item.Value, true
}

// Len returns the number of items in the priority queue
func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.items.Len()
}

// Peek returns the highest priority item without removing it
func (pq *PriorityQueue) Peek() (interface{}, int, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.items.Len() == 0 {
		return nil, 0, false
	}

	item := (*pq.items)[0]
	return item.Value, item.Priority, true
}

// Drain removes and returns all items from the priority queue
func (pq *PriorityQueue) Drain() []interface{} {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	items := make([]interface{}, 0, pq.items.Len())
	for pq.items.Len() > 0 {
		item := heap.Pop(pq.items).(*PriorityItem)
		items = append(items, item.Value)
	}
	return items
}

// Close closes the priority queue
func (pq *PriorityQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	pq.closed = true
}

// IsClosed returns true if the priority queue is closed
func (pq *PriorityQueue) IsClosed() bool {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.closed
}

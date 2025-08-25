package sub

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Backpressure Policy Tests
// ============================================================================

func TestBackpressure_Block(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.backpressure.block")

	slowSubscriber := &SlowSubscriber{delay: 100 * time.Millisecond}

	// Use small buffer and blocking backpressure
	sub, err := s.Subscribe(topic, slowSubscriber,
		WithBufferSize(3),
		WithBackpressure(BackpressureBlock),
		WithConcurrency(1))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages rapidly - should block when buffer is full
	numMessages := 10
	publishStart := time.Now()
	
	for i := range numMessages {
		msg := Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "blocking message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}
	
	publishDuration := time.Since(publishStart)
	
	// Publishing should take some time due to blocking
	assert.Greater(t, publishDuration, 200*time.Millisecond, 
		"Publishing should be slower due to blocking backpressure")

	// Wait for all messages to be processed
	time.Sleep(2 * time.Second)

	// All messages should eventually be processed
	assert.Equal(t, int64(numMessages), slowSubscriber.Count())
}

func TestBackpressure_DropNewest(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.backpressure.drop.newest")

	slowSubscriber := &SlowSubscriber{delay: 100 * time.Millisecond}

	// Use small buffer and drop newest backpressure
	sub, err := s.Subscribe(topic, slowSubscriber,
		WithBufferSize(2), // Very small buffer to trigger drops quickly
		WithBackpressure(BackpressureDropNewest),
		WithConcurrency(1))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages rapidly
	numMessages := 10
	publishStart := time.Now()
	
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("drop newest message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
		
		// Small delay to allow some processing but still overwhelm the buffer
		if i%3 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	
	publishDuration := time.Since(publishStart)
	
	// Publishing should still be reasonably fast (but may not be instant due to buffer)
	assert.Less(t, publishDuration, 2*time.Second, 
		"Publishing should not be severely blocked with drop newest policy")

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Should have processed fewer messages than published due to dropping
	processedCount := slowSubscriber.Count()
	assert.Less(t, processedCount, int64(numMessages),
		"Some messages should have been dropped")
	assert.Greater(t, processedCount, int64(0),
		"Some messages should have been processed")
}

func TestBackpressure_DropOldest(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.backpressure.drop.oldest")

	slowSubscriber := &SlowSubscriber{delay: 100 * time.Millisecond}

	// Use small buffer and drop oldest backpressure
	sub, err := s.Subscribe(topic, slowSubscriber,
		WithBufferSize(2), // Very small buffer
		WithBackpressure(BackpressureDropOldest),
		WithConcurrency(1))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages rapidly
	numMessages := 10
	publishStart := time.Now()
	
	for i := range numMessages {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("drop oldest message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
		
		// Small delay to allow some processing but still overwhelm the buffer
		if i%3 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	
	publishDuration := time.Since(publishStart)
	
	// Publishing should still be reasonably fast
	assert.Less(t, publishDuration, 2*time.Second, 
		"Publishing should not be severely blocked with drop oldest policy")

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Should have processed fewer messages than published due to dropping
	processedCount := slowSubscriber.Count()
	assert.Less(t, processedCount, int64(numMessages),
		"Some messages should have been dropped")
	assert.Greater(t, processedCount, int64(0),
		"Some messages should have been processed")
}

// ============================================================================
// Buffer Management Tests
// ============================================================================

func TestBuffer_SizeLimit(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.buffer.size.limit")

	slowSubscriber := &SlowSubscriber{delay: 500 * time.Millisecond}
	bufferSize := 5

	sub, err := s.Subscribe(topic, slowSubscriber,
		WithBufferSize(bufferSize),
		WithBackpressure(BackpressureDropNewest),
		WithConcurrency(1))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish more messages than buffer can hold
	numMessages := bufferSize * 3
	for i := range numMessages {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("buffer test message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Let some processing happen
	time.Sleep(2 * time.Second)

	// Should have processed some but not all messages
	processedCount := slowSubscriber.Count()
	assert.Greater(t, processedCount, int64(0), "Should process some messages")
	assert.LessOrEqual(t, processedCount, int64(numMessages), "Cannot process more than published")
}

func TestBuffer_DrainOnShutdown(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.buffer.drain.shutdown")

	processed := make(chan Message, 100)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		processed <- msg
		time.Sleep(50 * time.Millisecond) // Small delay to simulate work
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber,
		WithBufferSize(20),
		WithConcurrency(2))
	require.NoError(t, err)

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publish several messages
	numMessages := 15
	for i := range numMessages {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("drain test message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Give some time for messages to enter buffer
	time.Sleep(200 * time.Millisecond)

	// Drain the subscription
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer drainCancel()

	err = sub.Drain(drainCtx)
	assert.NoError(t, err)

	// Should have processed all messages
	processedCount := len(processed)
	assert.Equal(t, numMessages, processedCount, "All messages should be processed during drain")
}

// ============================================================================
// Concurrency and Worker Pool Tests
// ============================================================================

func TestConcurrency_SingleWorker(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.concurrency.single")

	processOrder := make([]int, 0, 10)
	var mu sync.Mutex

	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		time.Sleep(50 * time.Millisecond) // Simulate work
		mu.Lock()
		// Extract message number from data
		var msgNum int
		fmt.Sscanf(string(msg.Data), "message %d", &msgNum)
		processOrder = append(processOrder, msgNum)
		mu.Unlock()
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber,
		WithConcurrency(1), // Single worker
		WithBufferSize(10))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages sequentially
	numMessages := 5
	for i := range numMessages {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// With single worker, messages should be processed in order
	assert.Equal(t, numMessages, len(processOrder))
	for i := 0; i < len(processOrder)-1; i++ {
		assert.Less(t, processOrder[i], processOrder[i+1], 
			"Messages should be processed in order with single worker")
	}
}

func TestConcurrency_MultipleWorkers(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.concurrency.multiple")

	processCount := int64(0)
	maxConcurrent := int64(0)
	currentConcurrent := int64(0)

	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		current := atomic.AddInt64(&currentConcurrent, 1)
		defer atomic.AddInt64(&currentConcurrent, -1)

		// Track maximum concurrent processing
		for {
			max := atomic.LoadInt64(&maxConcurrent)
			if current <= max || atomic.CompareAndSwapInt64(&maxConcurrent, max, current) {
				break
			}
		}

		time.Sleep(100 * time.Millisecond) // Simulate work
		atomic.AddInt64(&processCount, 1)
		return nil
	})

	concurrency := 5
	sub, err := s.Subscribe(topic, subscriber,
		WithConcurrency(concurrency),
		WithBufferSize(20))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish many messages at once
	numMessages := 20
	for i := range numMessages {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("concurrent message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(5 * time.Second)

	// Should have processed all messages
	finalCount := atomic.LoadInt64(&processCount)
	assert.Equal(t, int64(numMessages), finalCount)

	// Should have achieved some level of concurrency
	finalMaxConcurrent := atomic.LoadInt64(&maxConcurrent)
	assert.Greater(t, finalMaxConcurrent, int64(1), 
		"Should achieve some concurrent processing")
	assert.LessOrEqual(t, finalMaxConcurrent, int64(concurrency), 
		"Should not exceed configured concurrency")
}

// ============================================================================
// Load and Stress Tests
// ============================================================================

func TestLoad_HighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	s := CreateTestStream(t)
	topic := Topic("test.load.high.throughput")

	processedCount := int64(0)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&processedCount, 1)
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber,
		WithConcurrency(10),
		WithBufferSize(1000),
		WithBackpressure(BackpressureDropNewest))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish high volume of messages
	numMessages := 10000
	startTime := time.Now()

	var wg sync.WaitGroup
	publishersCount := 5
	messagesPerPublisher := numMessages / publishersCount

	for p := range publishersCount {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
			for i := 0; i < messagesPerPublisher; i++ {
				msg := Message{
					Topic: topic,
					Data:  []byte(fmt.Sprintf("load test %d-%d", publisherID, i)),
					Time:  time.Now(),
				}
				err := s.Publish(ctx, topic, msg)
				if err != nil {
					t.Errorf("Publisher %d failed to publish message %d: %v", publisherID, i, err)
				}
			}
		}(p)
	}

	wg.Wait()
	publishDuration := time.Since(startTime)

	// Wait for processing to complete
	time.Sleep(5 * time.Second)

	finalProcessed := atomic.LoadInt64(&processedCount)
	
	// Calculate throughput
	publishThroughput := float64(numMessages) / publishDuration.Seconds()
	processingThroughput := float64(finalProcessed) / (publishDuration.Seconds() + 5)

	t.Logf("Published %d messages in %v (%.0f msg/s)", 
		numMessages, publishDuration, publishThroughput)
	t.Logf("Processed %d messages (%.0f msg/s)", 
		finalProcessed, processingThroughput)

	// Should process a significant portion of messages
	assert.Greater(t, finalProcessed, int64(numMessages/2), 
		"Should process at least half of the messages")
}

func TestLoad_BackpressureUnderLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping load test in short mode")
	}

	s := CreateTestStream(t)
	topic := Topic("test.load.backpressure")

	// Very slow subscriber
	slowSubscriber := &SlowSubscriber{delay: 10 * time.Millisecond}

	sub, err := s.Subscribe(topic, slowSubscriber,
		WithConcurrency(2),
		WithBufferSize(10),
		WithBackpressure(BackpressureDropOldest))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages rapidly to trigger backpressure
	numMessages := 1000
	startTime := time.Now()

	for i := range numMessages {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("backpressure test %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	publishDuration := time.Since(startTime)

	// Wait for processing
	time.Sleep(3 * time.Second)

	processedCount := slowSubscriber.Count()

	t.Logf("Published %d messages in %v", numMessages, publishDuration)
	t.Logf("Processed %d messages under backpressure", processedCount)

	// Should drop many messages due to backpressure
	assert.Less(t, processedCount, int64(numMessages), 
		"Should drop messages under backpressure")
	assert.Greater(t, processedCount, int64(0), 
		"Should process some messages")

	// Publishing should remain relatively fast despite backpressure
	assert.Less(t, publishDuration, 2*time.Second, 
		"Publishing should not be severely impacted by backpressure")
}

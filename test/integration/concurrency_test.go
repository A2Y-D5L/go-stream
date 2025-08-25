package stream

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Concurrent Operation Tests
// ============================================================================

// TestConcurrency_100ConcurrentPublishers tests high-concurrency publishing
func TestConcurrency_100ConcurrentPublishers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}
	
	s := CreateTestStream(t)
	topic := Topic("concurrency.publishers")
	
	received := make(chan Message, 10000)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})
	
	sub, err := s.Subscribe(topic, subscriber, WithConcurrency(10))
	require.NoError(t, err)
	defer sub.Stop()
	
	// Wait for subscription to establish
	time.Sleep(200 * time.Millisecond)
	
	numPublishers := 100
	messagesPerPublisher := 50
	totalMessages := numPublishers * messagesPerPublisher
	
	var wg sync.WaitGroup
	publishedCount := int64(0)
	
	// Start concurrent publishers
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
			
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			
			for j := 0; j < messagesPerPublisher; j++ {
				msg := Message{
					Topic: topic,
					Data:  []byte(fmt.Sprintf("publisher-%d-message-%d", publisherID, j)),
					Headers: map[string]string{
						"Publisher-ID": fmt.Sprintf("%d", publisherID),
						"Message-Seq":  fmt.Sprintf("%d", j),
					},
					Time: time.Now(),
				}
				
				err := s.Publish(ctx, topic, msg)
				if err != nil {
					t.Errorf("Publisher %d failed to publish message %d: %v", publisherID, j, err)
				} else {
					atomic.AddInt64(&publishedCount, 1)
				}
			}
		}(i)
	}
	
	wg.Wait()
	
	// Allow time for message processing
	time.Sleep(5 * time.Second)
	
	// Verify results
	finalPublished := atomic.LoadInt64(&publishedCount)
	receivedCount := len(received)
	
	t.Logf("Published: %d messages", finalPublished)
	t.Logf("Received: %d messages", receivedCount)
	
	// Should have published all messages
	assert.Equal(t, int64(totalMessages), finalPublished)
	
	// Should have received most messages (allow for some timing variations)
	assert.GreaterOrEqual(t, receivedCount, int(float64(totalMessages)*0.95), 
		"Should receive at least 95%% of published messages")
}

// TestConcurrency_100ConcurrentSubscribers tests high-concurrency subscribing
func TestConcurrency_100ConcurrentSubscribers(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrency test in short mode")
	}
	
	s := CreateTestStream(t)
	topic := Topic("concurrency.subscribers")
	
	numSubscribers := 100
	numMessages := 1000
	
	// Track messages per subscriber
	subscriberCounts := make([]int64, numSubscribers)
	subscribers := make([]Subscription, numSubscribers)
	
	// Create subscribers with different queue groups (each gets all messages)
	for i := 0; i < numSubscribers; i++ {
		index := i // Capture for closure
		
		subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
			atomic.AddInt64(&subscriberCounts[index], 1)
			// Simulate varying processing times
			processingTime := time.Duration(index%10) * time.Millisecond
			time.Sleep(processingTime)
			return nil
		})
		
		sub, err := s.Subscribe(topic, subscriber, 
			WithQueueGroupName(fmt.Sprintf("subscriber-group-%d", i)),
			WithConcurrency(2))
		require.NoError(t, err)
		subscribers[i] = sub
		defer sub.Stop()
	}
	
	// Wait for all subscriptions to establish
	time.Sleep(500 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	
	// Publish messages
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("broadcast message %d", i)),
			Headers: map[string]string{
				"Message-ID": fmt.Sprintf("msg-%d", i),
			},
			Time: time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
		
		// Add small delay to avoid overwhelming the system
		if i%100 == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	
	// Allow time for processing
	time.Sleep(10 * time.Second)
	
	// Verify each subscriber received all messages
	for i := 0; i < numSubscribers; i++ {
		count := atomic.LoadInt64(&subscriberCounts[i])
		assert.GreaterOrEqual(t, count, int64(float64(numMessages)*0.95),
			"Subscriber %d should receive at least 95%% of messages, got %d", i, count)
	}
	
	// Calculate total processed messages
	totalProcessed := int64(0)
	for i := 0; i < numSubscribers; i++ {
		totalProcessed += atomic.LoadInt64(&subscriberCounts[i])
	}
	
	expectedTotal := int64(numSubscribers * numMessages)
	processingRate := float64(totalProcessed) / float64(expectedTotal) * 100
	
	t.Logf("Total messages processed: %d/%d (%.2f%%)", 
		totalProcessed, expectedTotal, processingRate)
	
	assert.GreaterOrEqual(t, processingRate, 90.0, 
		"Should process at least 90%% of expected messages")
}

// TestConcurrency_MixedConcurrentOperations tests mixed concurrent operations
func TestConcurrency_MixedConcurrentOperations(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping mixed concurrency test in short mode")
	}
	
	s := CreateTestStream(t)
	
	// Multiple topics for different operation types
	pubsubTopic := Topic("concurrency.mixed.pubsub")
	requestTopic := Topic("concurrency.mixed.request")
	
	// Track different operation types
	pubsubReceived := int64(0)
	requestsReceived := int64(0)
	responsesReceived := int64(0)
	
	// Pub/Sub subscriber
	pubsubSubscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&pubsubReceived, 1)
		time.Sleep(1 * time.Millisecond) // Simulate work
		return nil
	})
	
	pubsubSub, err := s.Subscribe(pubsubTopic, pubsubSubscriber, WithConcurrency(5))
	require.NoError(t, err)
	defer pubsubSub.Stop()
	
	// Request responder
	responder := SubscriberFunc(func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&requestsReceived, 1)
		
		// Send response
		response := Message{
			Topic: Topic(msg.Headers["Reply-To"]),
			Data:  []byte("response-" + string(msg.Data)),
			Headers: map[string]string{
				"Request-ID": msg.Headers["Request-ID"],
			},
			Time: time.Now(),
		}
		
		if replyTo := msg.Headers["Reply-To"]; replyTo != "" {
			return s.Publish(ctx, Topic(replyTo), response)
		}
		return nil
	})
	
	requestSub, err := s.Subscribe(requestTopic, responder, WithConcurrency(3))
	require.NoError(t, err)
	defer requestSub.Stop()
	
	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)
	
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	
	// Concurrent pub/sub operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			msg := Message{
				Topic: pubsubTopic,
				Data:  []byte(fmt.Sprintf("pubsub-msg-%d", i)),
				Time:  time.Now(),
			}
			err := s.Publish(ctx, pubsubTopic, msg)
			if err != nil {
				t.Errorf("Failed to publish pubsub message %d: %v", i, err)
			}
		}
	}()
	
	// Concurrent request/reply operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			request := Message{
				Topic: requestTopic,
				Data:  []byte(fmt.Sprintf("request-%d", i)),
				Headers: map[string]string{
					"Request-ID": fmt.Sprintf("req-%d", i),
				},
				Time: time.Now(),
			}
			
			response, err := s.Request(ctx, requestTopic, request, 5*time.Second)
			if err != nil {
				t.Errorf("Failed request %d: %v", i, err)
			} else {
				atomic.AddInt64(&responsesReceived, 1)
				if !assert.Contains(t, string(response.Data), fmt.Sprintf("request-%d", i)) {
					t.Errorf("Invalid response for request %d", i)
				}
			}
		}
	}()
	
	// Mixed subscription operations
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			// Create temporary subscribers
			tempTopic := Topic(fmt.Sprintf("concurrency.temp.%d", i))
			tempReceived := int64(0)
			
			tempSubscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
				atomic.AddInt64(&tempReceived, 1)
				return nil
			})
			
			tempSub, err := s.Subscribe(tempTopic, tempSubscriber)
			if err != nil {
				t.Errorf("Failed to create temp subscriber %d: %v", i, err)
				continue
			}
			
			// Send messages to temp topic
			for j := 0; j < 10; j++ {
				msg := Message{
					Topic: tempTopic,
					Data:  []byte(fmt.Sprintf("temp-msg-%d-%d", i, j)),
					Time:  time.Now(),
				}
				_ = s.Publish(ctx, tempTopic, msg)
			}
			
			time.Sleep(100 * time.Millisecond)
			
			// Stop temp subscriber
			_ = tempSub.Stop()
			
			// Verify temp subscriber received messages
			if tempReceived < 8 { // Allow for some timing variations
				t.Errorf("Temp subscriber %d received %d messages, expected at least 8", i, tempReceived)
			}
		}
	}()
	
	wg.Wait()
	
	// Allow time for final processing
	time.Sleep(3 * time.Second)
	
	// Verify results
	finalPubsub := atomic.LoadInt64(&pubsubReceived)
	finalRequests := atomic.LoadInt64(&requestsReceived)
	finalResponses := atomic.LoadInt64(&responsesReceived)
	
	t.Logf("Pub/Sub messages received: %d", finalPubsub)
	t.Logf("Requests received: %d", finalRequests)
	t.Logf("Responses received: %d", finalResponses)
	
	assert.GreaterOrEqual(t, finalPubsub, int64(450), "Should receive most pub/sub messages")
	assert.GreaterOrEqual(t, finalRequests, int64(180), "Should receive most requests")
	assert.GreaterOrEqual(t, finalResponses, int64(180), "Should receive most responses")
}

// TestConcurrency_LoadBalancingAcrossSubscribers tests load balancing
func TestConcurrency_LoadBalancingAcrossSubscribers(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("concurrency.load.balancing")
	
	numSubscribers := 5
	subscriberCounts := make([]int64, numSubscribers)
	
	// Create subscribers in the same queue group for load balancing
	for i := 0; i < numSubscribers; i++ {
		index := i // Capture for closure
		
		subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
			atomic.AddInt64(&subscriberCounts[index], 1)
			// Simulate varying processing times
			processingTime := time.Duration((index+1)*5) * time.Millisecond
			time.Sleep(processingTime)
			return nil
		})
		
		sub, err := s.Subscribe(topic, subscriber, WithQueueGroupName("load-balance-group"))
		require.NoError(t, err)
		defer sub.Stop()
	}
	
	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Publish messages
	numMessages := 500
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("load-balance-msg-%d", i)),
			Headers: map[string]string{
				"Message-ID": fmt.Sprintf("lb-%d", i),
			},
			Time: time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}
	
	// Allow time for processing
	time.Sleep(10 * time.Second)
	
	// Verify load balancing
	totalProcessed := int64(0)
	for i := 0; i < numSubscribers; i++ {
		count := atomic.LoadInt64(&subscriberCounts[i])
		totalProcessed += count
		t.Logf("Subscriber %d processed %d messages", i, count)
	}
	
	// Should have processed all messages
	assert.Equal(t, int64(numMessages), totalProcessed, "All messages should be processed")
	
	// Check load distribution (each subscriber should get some messages)
	for i := 0; i < numSubscribers; i++ {
		count := atomic.LoadInt64(&subscriberCounts[i])
		assert.Greater(t, count, int64(0), "Subscriber %d should process at least one message", i)
		
		// No subscriber should process more than 80% of messages (good distribution)
		maxExpected := int64(float64(numMessages) * 0.8)
		assert.LessOrEqual(t, count, maxExpected, 
			"Subscriber %d processed too many messages (%d), load balancing may be poor", i, count)
	}
}

// TestConcurrency_ResourceSharingUnderHighLoad tests resource sharing
func TestConcurrency_ResourceSharingUnderHighLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping resource sharing test in short mode")
	}
	
	s := CreateTestStream(t)
	
	// Multiple topics competing for resources
	topics := []Topic{
		Topic("concurrency.resource.topic1"),
		Topic("concurrency.resource.topic2"),
		Topic("concurrency.resource.topic3"),
	}
	
	// Track processing per topic
	topicCounts := make([]int64, len(topics))
	
	// Create subscribers for each topic
	for i, topic := range topics {
		index := i // Capture for closure
		
		subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
			atomic.AddInt64(&topicCounts[index], 1)
			// Simulate CPU-intensive work
			time.Sleep(2 * time.Millisecond)
			return nil
		})
		
		sub, err := s.Subscribe(topic, subscriber, 
			WithConcurrency(5),
			WithBufferSize(100))
		require.NoError(t, err)
		defer sub.Stop()
	}
	
	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)
	
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	
	// Concurrent publishers for each topic
	messagesPerTopic := 300
	for i, topic := range topics {
		wg.Add(1)
		go func(topicIndex int, topicObj Topic) {
			defer wg.Done()
			
			for j := 0; j < messagesPerTopic; j++ {
				msg := Message{
					Topic: topicObj,
					Data:  []byte(fmt.Sprintf("topic%d-msg-%d", topicIndex, j)),
					Headers: map[string]string{
						"Topic-Index": fmt.Sprintf("%d", topicIndex),
						"Message-ID":  fmt.Sprintf("t%d-m%d", topicIndex, j),
					},
					Time: time.Now(),
				}
				
				err := s.Publish(ctx, topicObj, msg)
				if err != nil {
					t.Errorf("Failed to publish to topic %d, message %d: %v", topicIndex, j, err)
				}
				
				// Small delay to simulate realistic load
				if j%50 == 0 {
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(i, topic)
	}
	
	wg.Wait()
	
	// Allow time for processing
	time.Sleep(15 * time.Second)
	
	// Verify resource sharing fairness
	totalProcessed := int64(0)
	for i, topic := range topics {
		count := atomic.LoadInt64(&topicCounts[i])
		totalProcessed += count
		t.Logf("Topic %s processed %d messages", topic, count)
	}
	
	expectedTotal := int64(len(topics) * messagesPerTopic)
	processingRate := float64(totalProcessed) / float64(expectedTotal) * 100
	
	t.Logf("Total processed: %d/%d (%.2f%%)", totalProcessed, expectedTotal, processingRate)
	
	// Should process most messages
	assert.GreaterOrEqual(t, processingRate, 85.0, "Should process at least 85%% of messages")
	
	// Each topic should get fair share of resources
	for i := range topics {
		count := atomic.LoadInt64(&topicCounts[i])
		expectedCount := float64(messagesPerTopic)
		actualRate := float64(count) / expectedCount * 100
		
		assert.GreaterOrEqual(t, actualRate, 75.0, 
			"Topic %d should process at least 75%% of its messages", i)
	}
}

// ============================================================================
// Stress Testing
// ============================================================================

// TestStress_SustainedLoad tests system behavior under sustained load
func TestStress_SustainedLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}
	
	s := CreateTestStream(t)
	topic := Topic("stress.sustained.load")
	
	processed := int64(0)
	errors := int64(0)
	
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&processed, 1)
		
		// Simulate varying workloads
		if string(msg.Data) == "heavy-task" {
			time.Sleep(10 * time.Millisecond)
		} else {
			time.Sleep(1 * time.Millisecond)
		}
		
		return nil
	})
	
	sub, err := s.Subscribe(topic, subscriber, 
		WithConcurrency(10),
		WithBufferSize(1000))
	require.NoError(t, err)
	defer sub.Stop()
	
	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	
	// Sustained load for 60 seconds
	duration := 60 * time.Second
	startTime := time.Now()
	messageID := int64(0)
	
	var wg sync.WaitGroup
	wg.Add(1)
	
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond) // 100 messages per second
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if time.Since(startTime) > duration {
					return
				}
				
				id := atomic.AddInt64(&messageID, 1)
				
				// Mix of different message types
				var data []byte
				if id%10 == 0 {
					data = []byte("heavy-task")
				} else {
					data = []byte(fmt.Sprintf("light-task-%d", id))
				}
				
				msg := Message{
					Topic: topic,
					Data:  data,
					Headers: map[string]string{
						"Message-ID": fmt.Sprintf("%d", id),
						"Timestamp":  time.Now().Format(time.RFC3339Nano),
					},
					Time: time.Now(),
				}
				
				err := s.Publish(ctx, topic, msg)
				if err != nil {
					atomic.AddInt64(&errors, 1)
				}
			}
		}
	}()
	
	// Monitor progress every 10 seconds
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				currentProcessed := atomic.LoadInt64(&processed)
				currentErrors := atomic.LoadInt64(&errors)
				elapsed := time.Since(startTime)
				
				t.Logf("Elapsed: %v, Processed: %d, Errors: %d, Rate: %.1f msg/s",
					elapsed, currentProcessed, currentErrors,
					float64(currentProcessed)/elapsed.Seconds())
			}
		}
	}()
	
	wg.Wait()
	
	// Allow time for final processing
	time.Sleep(5 * time.Second)
	
	finalProcessed := atomic.LoadInt64(&processed)
	finalErrors := atomic.LoadInt64(&errors)
	finalPublished := atomic.LoadInt64(&messageID)
	
	t.Logf("Final stats:")
	t.Logf("  Published: %d messages", finalPublished)
	t.Logf("  Processed: %d messages", finalProcessed)
	t.Logf("  Errors: %d", finalErrors)
	t.Logf("  Processing rate: %.2f%%", float64(finalProcessed)/float64(finalPublished)*100)
	
	// Verify system handled sustained load
	assert.GreaterOrEqual(t, finalProcessed, int64(float64(finalPublished)*0.90),
		"Should process at least 90%% of messages under sustained load")
	
	assert.LessOrEqual(t, finalErrors, int64(float64(finalPublished)*0.05),
		"Error rate should be less than 5%%")
}

// TestStress_MemoryUsageStabilityOverTime tests memory stability
func TestStress_MemoryUsageStabilityOverTime(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory stress test in short mode")
	}
	
	s := CreateTestStream(t)
	topic := Topic("stress.memory.stability")
	
	// Record memory stats
	var memStats []runtime.MemStats
	
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		// Create some temporary data to simulate real workloads
		_ = make([]byte, 1024) // 1KB per message
		time.Sleep(1 * time.Millisecond)
		return nil
	})
	
	sub, err := s.Subscribe(topic, subscriber, WithConcurrency(5))
	require.NoError(t, err)
	defer sub.Stop()
	
	time.Sleep(100 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	
	// Initial memory reading
	runtime.GC()
	var initialMem runtime.MemStats
	runtime.ReadMemStats(&initialMem)
	memStats = append(memStats, initialMem)
	
	// Run for multiple cycles
	cycles := 5
	messagesPerCycle := 1000
	
	for cycle := 0; cycle < cycles; cycle++ {
		t.Logf("Starting cycle %d/%d", cycle+1, cycles)
		
		// Publish messages
		for i := 0; i < messagesPerCycle; i++ {
			msg := Message{
				Topic: topic,
				Data:  make([]byte, 2048), // 2KB message
				Headers: map[string]string{
					"Cycle":      fmt.Sprintf("%d", cycle),
					"Message-ID": fmt.Sprintf("c%d-m%d", cycle, i),
				},
				Time: time.Now(),
			}
			
			// Fill with some data
			for j := range msg.Data {
				msg.Data[j] = byte(j % 256)
			}
			
			err := s.Publish(ctx, topic, msg)
			require.NoError(t, err)
		}
		
		// Wait for processing
		time.Sleep(3 * time.Second)
		
		// Force GC and record memory
		runtime.GC()
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		memStats = append(memStats, mem)
		
		t.Logf("Cycle %d - Memory: %d bytes, Heap: %d bytes",
			cycle+1, mem.Alloc, mem.HeapAlloc)
	}
	
	// Analyze memory growth
	initialAlloc := memStats[0].Alloc
	finalAlloc := memStats[len(memStats)-1].Alloc
	
	memoryGrowth := finalAlloc - initialAlloc
	maxAcceptableGrowth := uint64(50 * 1024 * 1024) // 50MB
	
	t.Logf("Memory analysis:")
	t.Logf("  Initial memory: %d bytes", initialAlloc)
	t.Logf("  Final memory: %d bytes", finalAlloc)
	t.Logf("  Growth: %d bytes", memoryGrowth)
	t.Logf("  Max acceptable: %d bytes", maxAcceptableGrowth)
	
	assert.Less(t, memoryGrowth, maxAcceptableGrowth,
		"Memory growth should be reasonable over time")
	
	// Check for memory stability (no continuous growth)
	growthCounts := 0
	for i := 1; i < len(memStats); i++ {
		if memStats[i].Alloc > memStats[i-1].Alloc*110/100 { // 10% growth threshold
			growthCounts++
		}
	}
	
	// Allow some growth, but not continuous
	assert.LessOrEqual(t, growthCounts, cycles/2,
		"Memory should not continuously grow across cycles")
}

// TestStress_ConnectionStabilityUnderStress tests connection stability
func TestStress_ConnectionStabilityUnderStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping connection stress test in short mode")
	}
	
	s := CreateTestStream(t)
	topic := Topic("stress.connection.stability")
	
	// Create many short-lived subscribers
	numCycles := 20
	subscribersPerCycle := 10
	messagesPerSubscriber := 50
	
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	
	for cycle := 0; cycle < numCycles; cycle++ {
		t.Logf("Connection stress cycle %d/%d", cycle+1, numCycles)
		
		var wg sync.WaitGroup
		
		// Create subscribers for this cycle
		for i := 0; i < subscribersPerCycle; i++ {
			wg.Add(1)
			go func(subID int) {
				defer wg.Done()
				
				received := int64(0)
				
				subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
					atomic.AddInt64(&received, 1)
					time.Sleep(1 * time.Millisecond)
					return nil
				})
				
				sub, err := s.Subscribe(topic, subscriber,
					WithQueueGroupName(fmt.Sprintf("cycle-%d-sub-%d", cycle, subID)))
				if err != nil {
					t.Errorf("Failed to create subscriber %d in cycle %d: %v", subID, cycle, err)
					return
				}
				
				// Wait a bit for subscription to establish
				time.Sleep(50 * time.Millisecond)
				
				// Publish messages for this subscriber
				for j := 0; j < messagesPerSubscriber; j++ {
					msg := Message{
						Topic: topic,
						Data:  []byte(fmt.Sprintf("cycle-%d-sub-%d-msg-%d", cycle, subID, j)),
						Headers: map[string]string{
							"Cycle":        fmt.Sprintf("%d", cycle),
							"Subscriber":   fmt.Sprintf("%d", subID),
							"Message-Seq":  fmt.Sprintf("%d", j),
						},
						Time: time.Now(),
					}
					
					err := s.Publish(ctx, topic, msg)
					if err != nil {
						t.Errorf("Failed to publish message in cycle %d: %v", cycle, err)
					}
				}
				
				// Wait for messages to be processed
				time.Sleep(200 * time.Millisecond)
				
				// Stop subscriber
				err = sub.Stop()
				if err != nil {
					t.Errorf("Failed to stop subscriber %d in cycle %d: %v", subID, cycle, err)
				}
				
				finalReceived := atomic.LoadInt64(&received)
				if finalReceived < int64(messagesPerSubscriber/2) {
					t.Errorf("Subscriber %d in cycle %d received too few messages: %d", 
						subID, cycle, finalReceived)
				}
			}(i)
		}
		
		wg.Wait()
		
		// Small pause between cycles
		time.Sleep(100 * time.Millisecond)
	}
	
	// Verify the original stream is still healthy
	healthMsg := Message{
		Topic: topic,
		Data:  []byte("health-check"),
		Time:  time.Now(),
	}
	
	err := s.Publish(ctx, topic, healthMsg)
	assert.NoError(t, err, "Stream should remain healthy after connection stress")
	
	t.Logf("Successfully completed %d connection stress cycles with %d subscribers each",
		numCycles, subscribersPerCycle)
}

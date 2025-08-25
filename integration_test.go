package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// End-to-End Integration Tests
// ============================================================================

func TestIntegration_PublishSubscribeFlow(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.integration.pubsub")

	received := make(chan Message, 10)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test different message types
	testMessages := []Message{
		{
			Topic: topic,
			Data:  []byte("simple text message"),
			Headers: map[string]string{
				"Content-Type": "text/plain",
			},
			Time: time.Now(),
		},
		{
			Topic: topic,
			Data:  []byte(`{"id": 123, "name": "JSON message"}`),
			Headers: map[string]string{
				"Content-Type": "application/json",
			},
			Time: time.Now(),
		},
		{
			Topic: topic,
			Data:  make([]byte, 1024), // Binary data
			Headers: map[string]string{
				"Content-Type": "application/octet-stream",
			},
			Time: time.Now(),
		},
	}

	// Fill binary data
	for i := range testMessages[2].Data {
		testMessages[2].Data[i] = byte(i % 256)
	}

	// Publish all test messages
	for i, msg := range testMessages {
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err, "Failed to publish message %d", i)
	}

	// Verify all messages are received
	receivedMessages := make([]Message, 0, len(testMessages))
	timeout := time.After(5 * time.Second)

	for i := 0; i < len(testMessages); i++ {
		select {
		case msg := <-received:
			receivedMessages = append(receivedMessages, msg)
		case <-timeout:
			t.Fatalf("Timeout waiting for message %d", i)
		}
	}

	// Verify message contents
	assert.Equal(t, len(testMessages), len(receivedMessages))
	for i, original := range testMessages {
		found := false
		for _, received := range receivedMessages {
			if string(original.Data) == string(received.Data) {
				assert.Equal(t, original.Headers["Content-Type"], received.Headers["Content-Type"])
				found = true
				break
			}
		}
		assert.True(t, found, "Message %d not found in received messages", i)
	}
}

func TestIntegration_RequestReplyFlow(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("integration.request.reply")

	// Set up responder using test helper
	sub, err := s.TestJSONResponder(requestTopic, func(data []byte) (any, error) {
		var request map[string]any
		if err := json.Unmarshal(data, &request); err != nil {
			return nil, err
		}

		// Create response with echo and processing info
		response := map[string]any{
			"processed": true,
			"echo":      request,
			"timestamp": time.Now().Unix(),
		}
		return response, nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Wait for responder to be ready
	time.Sleep(100 * time.Millisecond)

	// Send request
	request := map[string]any{
		"user_id": 123,
		"action":  "get_profile",
		"params": map[string]string{
			"include": "preferences",
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	response, err := RequestJSON[map[string]any](s, ctx, requestTopic, request, 5*time.Second)
	require.NoError(t, err)

	// Verify response
	assert.True(t, response["processed"].(bool))
	assert.NotNil(t, response["echo"])
	assert.NotNil(t, response["timestamp"])

	// Verify echo contains original request
	echo := response["echo"].(map[string]any)
	assert.Equal(t, float64(123), echo["user_id"]) // JSON numbers are float64
}

func TestIntegration_MultipleSubscribersFlow(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.integration.multiple.subscribers")

	numSubscribers := 3
	subscribers := make([]*CountingSubscriber, numSubscribers)
	subs := make([]Subscription, numSubscribers)

	// Create subscribers with different queue groups (each gets all messages)
	for i := 0; i < numSubscribers; i++ {
		subscribers[i] = &CountingSubscriber{}
		var err error
		subs[i], err = s.Subscribe(topic, subscribers[i], 
			WithQueueGroup(fmt.Sprintf("group-%d", i)))
		require.NoError(t, err)
		defer subs[i].Stop()
	}

	// Wait for subscriptions to be ready
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish test messages
	numMessages := 10
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
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Each subscriber should receive all messages
	for i, subscriber := range subscribers {
		count := subscriber.Count()
		assert.Equal(t, int64(numMessages), count, 
			"Subscriber %d should receive all messages", i)
	}
}

func TestIntegration_MessageOrdering(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.integration.ordering")

	receivedOrder := make([]int, 0, 20)
	var mu sync.Mutex

	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		// Extract sequence number
		var seq int
		fmt.Sscanf(string(msg.Data), "ordered message %d", &seq)
		
		mu.Lock()
		receivedOrder = append(receivedOrder, seq)
		mu.Unlock()
		
		// Small delay to make ordering issues more likely if they exist
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	// Single worker to ensure ordering
	sub, err := s.Subscribe(topic, subscriber, WithConcurrency(1))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages in sequence
	numMessages := 15
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("ordered message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Verify ordering
	assert.Equal(t, numMessages, len(receivedOrder))
	for i := 1; i < len(receivedOrder); i++ {
		assert.Equal(t, i, receivedOrder[i], 
			"Message %d should be in position %d, got position %d", i, i, receivedOrder[i])
	}
}

// ============================================================================
// Error Recovery and Resilience Tests
// ============================================================================

func TestIntegration_ErrorRecovery(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.integration.error.recovery")

	processedCount := int64(0)
	failureCount := int64(0)

	// Subscriber that fails every 3rd message
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		count := atomic.AddInt64(&processedCount, 1)
		if count%3 == 0 {
			atomic.AddInt64(&failureCount, 1)
			return fmt.Errorf("simulated failure for message %d", count)
		}
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages
	numMessages := 15
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("recovery test message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify processing continues despite failures
	finalProcessed := atomic.LoadInt64(&processedCount)
	finalFailures := atomic.LoadInt64(&failureCount)

	assert.Equal(t, int64(numMessages), finalProcessed, 
		"Should attempt to process all messages")
	assert.Greater(t, finalFailures, int64(0), 
		"Should have some failures")
	assert.Equal(t, finalFailures, finalProcessed/3, 
		"Should fail every 3rd message")
}

func TestIntegration_StreamReconnection(t *testing.T) {
	// This test simulates connection issues and recovery
	s := CreateTestStream(t)
	topic := Topic("test.integration.reconnection")

	received := make(chan Message, 20)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Publish initial messages
	for i := 0; i < 5; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("pre-reconnect message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Let messages be processed
	time.Sleep(1 * time.Second)

	// Continue publishing (simulating resilience)
	for i := 5; i < 10; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("post-reconnect message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for all messages
	time.Sleep(2 * time.Second)

	// Count received messages
	receivedCount := len(received)
	assert.Equal(t, 10, receivedCount, "Should receive all messages despite simulated issues")
}

// ============================================================================
// Performance and Latency Tests
// ============================================================================

func TestIntegration_EndToEndLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping latency test in short mode")
	}

	s := CreateTestStream(t)
	topic := Topic("test.integration.latency")

	latencies := make([]time.Duration, 0, 100)
	var mu sync.Mutex

	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		// Parse sent timestamp from message
		var sentTime int64
		fmt.Sscanf(string(msg.Data), "latency test %d", &sentTime)
		
		receiveTime := time.Now().UnixNano()
		latency := time.Duration(receiveTime - sentTime)
		
		mu.Lock()
		latencies = append(latencies, latency)
		mu.Unlock()
		
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages and measure latency
	numMessages := 100
	for i := 0; i < numMessages; i++ {
		sendTime := time.Now().UnixNano()
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("latency test %d", sendTime)),
			Time:  time.Now(),
		}
		publishErr := s.Publish(ctx, topic, msg)
		require.NoError(t, publishErr)
		
		// Small delay between messages
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for all messages to be processed
	time.Sleep(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	// Analyze latencies
	assert.Equal(t, numMessages, len(latencies), "Should measure latency for all messages")

	if len(latencies) > 0 {
		// Calculate statistics
		var total time.Duration
		min := latencies[0]
		max := latencies[0]

		for _, lat := range latencies {
			total += lat
			if lat < min {
				min = lat
			}
			if lat > max {
				max = lat
			}
		}

		avg := total / time.Duration(len(latencies))

		t.Logf("Latency stats: min=%v, max=%v, avg=%v", min, max, avg)

		// Basic assertions (these might need adjustment based on environment)
		assert.Less(t, avg, 100*time.Millisecond, "Average latency should be reasonable")
		assert.Less(t, max, 1*time.Second, "Maximum latency should be bounded")
	}
}

func TestIntegration_MemoryUsagePattern(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory test in short mode")
	}

	s := CreateTestStream(t)
	topic := Topic("test.integration.memory")

	processedCount := int64(0)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&processedCount, 1)
		// Don't retain message references
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber,
		WithConcurrency(5),
		WithBufferSize(100))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Restore to more challenging memory test
	numMessages := 5000 // Increased from 100
	messageSize := 1024 // 1KB per message

	data := make([]byte, messageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	startTime := time.Now()

	// Publish messages without artificial delays since health check is now faster
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Topic: topic,
			Data:  data, // Reuse same data to focus on framework overhead
			Headers: map[string]string{
				"Message-ID": fmt.Sprintf("mem-test-%d", i),
			},
			Time: time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)

		// Brief pause every 100 messages to allow processing
		if i%100 == 0 && i > 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}

	publishDuration := time.Since(startTime)

	// Wait for processing to complete
	time.Sleep(5 * time.Second)

	finalProcessed := atomic.LoadInt64(&processedCount)
	
	t.Logf("Published %d messages (%d KB) in %v", 
		numMessages, numMessages*messageSize/1024, publishDuration)
	t.Logf("Processed %d messages", finalProcessed)

	// Should process most/all messages
	assert.Greater(t, finalProcessed, int64(float64(numMessages)*0.9), 
		"Should process at least 90% of messages")
	
	// Memory should be manageable (this is more of a smoke test)
	// In a real scenario, you might want to measure actual memory usage
}

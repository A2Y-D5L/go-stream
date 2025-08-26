package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream"
	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/pub"
	"github.com/a2y-d5l/go-stream/sub"
	"github.com/a2y-d5l/go-stream/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Network and Connectivity Tests
// ============================================================================

// TestNetwork_ConnectionRecoveryAfterDisconnection tests reconnection behavior
func TestNetwork_ConnectionRecoveryAfterDisconnection(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("network.connection.recovery")

	received := make(chan message.Message, 50)
	subscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Send initial messages to verify connection
	for i := range 5 {
		msg := message.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "pre-disconnect-%d", i),
			Headers: map[string]string{
				"Phase": "pre-disconnect",
				"ID":    fmt.Sprintf("%d", i),
			},
			Time: time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for initial messages
	time.Sleep(500 * time.Millisecond)
	initialReceived := len(received)
	assert.Equal(t, 5, initialReceived, "Should receive initial messages")

	// Clear received channel
	for len(received) > 0 {
		<-received
	}

	// Simulate network issues by trying to stress the connection
	// Note: In embedded NATS mode, we can't easily simulate true network disconnects,
	// but we can test connection stability under stress

	// Send messages in rapid succession to test connection stability
	for i := range 20 {
		msg := message.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "stress-test-%d", i),
			Headers: map[string]string{
				"Phase": "stress-test",
				"ID":    fmt.Sprintf("%d", i),
			},
			Time: time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		if err != nil {
			t.Logf("Publish error during stress test (may be expected): %v", err)
		}

		// Very short delay to stress the connection
		time.Sleep(1 * time.Millisecond)
	}

	// Allow time for recovery and processing
	time.Sleep(2 * time.Second)

	// Send final messages to verify connection is stable
	for i := range 5 {
		msg := message.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "post-recovery-%d", i),
			Headers: map[string]string{
				"Phase": "post-recovery",
				"ID":    fmt.Sprintf("%d", i),
			},
			Time: time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		assert.NoError(t, err, "Should be able to publish after recovery")
	}

	// Allow time for final processing
	time.Sleep(1 * time.Second)

	finalReceived := len(received)

	// Should have received most messages (allowing for some loss during stress)
	assert.GreaterOrEqual(t, finalReceived, 20,
		"Should receive most messages despite connection stress")

	t.Logf("Received %d messages during network stress and recovery test", finalReceived)
}

// TestNetwork_TimeoutHandlingInNetworkScenarios tests timeout behavior
func TestNetwork_TimeoutHandlingInNetworkScenarios(t *testing.T) {
	s := helpers.CreateTestStream(t)
	requestTopic := stream.Topic("network.timeout.request")

	// Set up slow responder to test timeouts
	slowResponder := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		// Simulate network delay
		time.Sleep(2 * time.Second)

		response := message.Message{
			Topic: stream.Topic(msg.Headers["Reply-To"]),
			Data:  []byte("slow-response-" + string(msg.Data)),
			Headers: map[string]string{
				"Request-ID": msg.Headers["Request-ID"],
			},
			Time: time.Now(),
		}

		if replyTo := msg.Headers["Reply-To"]; replyTo != "" {
			return s.Publish(ctx, stream.Topic(replyTo), response)
		}
		return nil
	})

	sub, err := s.Subscribe(requestTopic, slowResponder)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test short timeout (should timeout)
	request1 := message.Message{
		Topic: requestTopic,
		Data:  []byte("timeout-test-1"),
		Headers: map[string]string{
			"Request-ID": "req-1",
		},
		Time: time.Now(),
	}

	start := time.Now()
	_, err = s.Request(ctx, requestTopic, request1, 500*time.Millisecond)
	elapsed := time.Since(start)

	assert.Error(t, err, "Should timeout with short timeout")
	assert.GreaterOrEqual(t, elapsed, 500*time.Millisecond, "Should wait at least timeout duration")
	assert.Less(t, elapsed, 1*time.Second, "Should not wait much longer than timeout")

	// Test longer timeout (should succeed)
	request2 := message.Message{
		Topic: requestTopic,
		Data:  []byte("timeout-test-2"),
		Headers: map[string]string{
			"Request-ID": "req-2",
		},
		Time: time.Now(),
	}

	start = time.Now()
	response, err := s.Request(ctx, requestTopic, request2, 5*time.Second)
	elapsed = time.Since(start)

	assert.NoError(t, err, "Should succeed with longer timeout")
	assert.Contains(t, string(response.Data), "timeout-test-2")
	assert.GreaterOrEqual(t, elapsed, 2*time.Second, "Should wait for slow responder")
	assert.Less(t, elapsed, 3*time.Second, "Should not wait much longer than necessary")

	t.Logf("Short timeout test: %v (expected error)", elapsed)
	t.Logf("Long timeout test: %v (successful)", elapsed)
}

// TestNetwork_MessageDeliveryGuaranteesUnderNetworkIssues tests delivery guarantees
func TestNetwork_MessageDeliveryGuaranteesUnderNetworkIssues(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("network.delivery.guarantees")

	received := make(chan message.Message, 200)
	messageIDs := make(map[string]bool) // Track unique message IDs

	subscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber, sub.WithConcurrency(3))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Send messages with different reliability requirements
	numMessages := 100
	publishedIDs := make(map[string]bool)

	for i := range numMessages {
		messageID := fmt.Sprintf("msg-%d", i)
		publishedIDs[messageID] = true

		msg := message.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "delivery-test-%d", i),
			Headers: map[string]string{
				"message.Message-ID": messageID,
				"Sequence":           fmt.Sprintf("%d", i),
			},
			Time: time.Now(),
		}

		// Use flush for important messages (every 10th message)
		if i%10 == 0 {
			err = s.Publish(ctx, topic, msg, pub.WithFlush(true))
		} else {
			err = s.Publish(ctx, topic, msg)
		}

		if err != nil {
			t.Logf("Publish error for message %d: %v", i, err)
			delete(publishedIDs, messageID) // Remove from expected if publish failed
		}

		// Simulate variable network conditions
		if i%20 == 0 {
			time.Sleep(10 * time.Millisecond) // Occasional delay
		}
	}

	// Allow time for message delivery
	time.Sleep(5 * time.Second)

	// Collect received messages
	receivedCount := len(received)
	for range receivedCount {
		msg := <-received
		messageID := msg.Headers["message.Message-ID"]
		if messageID != "" {
			messageIDs[messageID] = true
		}
	}

	publishedCount := len(publishedIDs)
	uniqueReceived := len(messageIDs)

	t.Logf("Published: %d messages", publishedCount)
	t.Logf("Received: %d messages (%d unique)", receivedCount, uniqueReceived)

	// Verify delivery guarantees
	deliveryRate := float64(uniqueReceived) / float64(publishedCount) * 100
	assert.GreaterOrEqual(t, deliveryRate, 95.0,
		"Should deliver at least 95%% of messages under network stress")

	// Check for duplicate delivery (should be minimal)
	duplicateRate := float64(receivedCount-uniqueReceived) / float64(receivedCount) * 100
	assert.LessOrEqual(t, duplicateRate, 5.0,
		"Duplicate message rate should be low")

	// Verify important messages (flushed) were delivered
	importantMissing := 0
	for i := range numMessages {
		messageID := fmt.Sprintf("msg-%d", i)
		if publishedIDs[messageID] && !messageIDs[messageID] {
			importantMissing++
		}
	}

	assert.Equal(t, 0, importantMissing,
		"All important (flushed) messages should be delivered")

	t.Logf("Delivery rate: %.2f%%", deliveryRate)
	t.Logf("Duplicate rate: %.2f%%", duplicateRate)
	t.Logf("Important messages missing: %d", importantMissing)
}

// TestNetwork_NetworkLatencyImpact tests behavior under network latency
func TestNetwork_NetworkLatencyImpact(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("network.latency.impact")

	latencies := make(chan time.Duration, 100)

	subscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		// Calculate end-to-end latency
		if timestampStr := msg.Headers["Send-Time"]; timestampStr != "" {
			if sendTime, err := time.Parse(time.RFC3339Nano, timestampStr); err == nil {
				latency := time.Since(sendTime)
				latencies <- latency
			}
		}
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Send messages with timestamps
	numMessages := 50
	for i := range numMessages {
		sendTime := time.Now()

		msg := message.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "latency-test-%d", i),
			Headers: map[string]string{
				"message.Message-ID": fmt.Sprintf("lat-%d", i),
				"Send-Time":          sendTime.Format(time.RFC3339Nano),
			},
			Time: sendTime,
		}

		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)

		// Add artificial delay to simulate network conditions
		time.Sleep(10 * time.Millisecond)
	}

	// Allow time for processing
	time.Sleep(2 * time.Second)

	// Analyze latencies
	receivedLatencies := len(latencies)
	assert.GreaterOrEqual(t, receivedLatencies, numMessages-5,
		"Should receive latency measurements for most messages")

	var totalLatency time.Duration
	var maxLatency time.Duration
	minLatency := time.Hour // Start with a large value

	for range receivedLatencies {
		latency := <-latencies
		totalLatency += latency

		if latency > maxLatency {
			maxLatency = latency
		}
		if latency < minLatency {
			minLatency = latency
		}
	}

	if receivedLatencies > 0 {
		avgLatency := totalLatency / time.Duration(receivedLatencies)

		t.Logf("Latency statistics:")
		t.Logf("  Average: %v", avgLatency)
		t.Logf("  Min: %v", minLatency)
		t.Logf("  Max: %v", maxLatency)
		t.Logf("  Samples: %d", receivedLatencies)

		// Verify reasonable latency characteristics
		assert.Less(t, avgLatency, 100*time.Millisecond,
			"Average latency should be reasonable")
		assert.Less(t, maxLatency, 500*time.Millisecond,
			"Maximum latency should be bounded")
	}
}

// TestNetwork_BandwidthConstraints tests behavior under bandwidth limitations
func TestNetwork_BandwidthConstraints(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("network.bandwidth.constraints")

	processed := int64(0)
	totalBytes := int64(0)

	subscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		atomic.AddInt64(&processed, 1)
		atomic.AddInt64(&totalBytes, int64(len(msg.Data)))
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber, sub.WithConcurrency(2))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Test different message sizes to simulate bandwidth constraints
	messageSizes := []int{
		100,    // Small messages
		1024,   // 1KB messages
		10240,  // 10KB messages
		102400, // 100KB messages
	}

	messagesPerSize := 20
	startTime := time.Now()

	for sizeIndex, size := range messageSizes {
		t.Logf("Testing with %d byte messages", size)

		for i := range messagesPerSize {
			data := make([]byte, size)
			// Fill with some pattern
			for j := range data {
				data[j] = byte((sizeIndex*100 + i + j) % 256)
			}

			msg := message.Message{
				Topic: topic,
				Data:  data,
				Headers: map[string]string{
					"message.Message-ID": fmt.Sprintf("size-%d-msg-%d", size, i),
					"Size":               fmt.Sprintf("%d", size),
				},
				Time: time.Now(),
			}

			err = s.Publish(ctx, topic, msg)
			require.NoError(t, err)

			// Simulate bandwidth limitations with larger messages
			if size > 10240 {
				time.Sleep(10 * time.Millisecond)
			}
		}

		// Allow time for processing before next size
		time.Sleep(500 * time.Millisecond)
	}

	// Allow time for final processing
	time.Sleep(3 * time.Second)

	elapsed := time.Since(startTime)
	finalProcessed := atomic.LoadInt64(&processed)
	finalBytes := atomic.LoadInt64(&totalBytes)

	expectedMessages := int64(len(messageSizes) * messagesPerSize)

	t.Logf("Bandwidth test results:")
	t.Logf("  Processed: %d/%d messages", finalProcessed, expectedMessages)
	t.Logf("  Total bytes: %d", finalBytes)
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Throughput: %.2f MB/s", float64(finalBytes)/elapsed.Seconds()/1024/1024)

	// Verify message processing
	assert.GreaterOrEqual(t, finalProcessed, expectedMessages*90/100,
		"Should process at least 90%% of messages regardless of size")

	// Verify throughput is reasonable
	throughputMBps := float64(finalBytes) / elapsed.Seconds() / 1024 / 1024
	assert.Greater(t, throughputMBps, 1.0,
		"Should achieve reasonable throughput")
}

// ============================================================================
// Protocol-Level Integration Tests
// ============================================================================

// TestNetwork_ProtocolCompliance tests NATS protocol compliance
func TestNetwork_ProtocolCompliance(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("network.protocol.compliance")

	// Test various message formats and features
	received := make(chan message.Message, 50)

	subscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test different protocol features
	testCases := []struct {
		name    string
		message message.Message
		options []stream.PublishOption
	}{
		{
			name: "basic message",
			message: message.Message{
				Topic: topic,
				Data:  []byte("basic protocol test"),
				Time:  time.Now(),
			},
		},
		{
			name: "message with headers",
			message: message.Message{
				Topic: topic,
				Data:  []byte("headers test"),
				Headers: map[string]string{
					"Content-Type":  "text/plain",
					"Custom-Header": "custom-value",
				},
				Time: time.Now(),
			},
		},
		{
			name: "message with ID",
			message: message.Message{
				Topic: topic,
				Data:  []byte("message id test"),
				Time:  time.Now(),
			},
			options: []stream.PublishOption{
				pub.WithMessageID("test-id-123"),
			},
		},
		{
			name: "flushed message",
			message: message.Message{
				Topic: topic,
				Data:  []byte("flush test"),
				Time:  time.Now(),
			},
			options: []stream.PublishOption{
				pub.WithFlush(true),
			},
		},
		{
			name: "empty message",
			message: message.Message{
				Topic: topic,
				Data:  []byte{},
				Time:  time.Now(),
			},
		},
		{
			name: "large message",
			message: message.Message{
				Topic: topic,
				Data:  make([]byte, 65536), // 64KB
				Time:  time.Now(),
			},
		},
	}

	// Fill large message with pattern
	for i := range testCases[len(testCases)-1].message.Data {
		testCases[len(testCases)-1].message.Data[i] = byte(i % 256)
	}

	// Publish test messages
	for i, tc := range testCases {
		tc.message.Headers = make(map[string]string)
		tc.message.Headers["Test-Case"] = tc.name
		tc.message.Headers["Test-Index"] = fmt.Sprintf("%d", i)

		err = s.Publish(ctx, topic, tc.message, tc.options...)
		require.NoError(t, err, "Failed to publish %s", tc.name)
	}

	// Verify all messages received
	timeout := time.After(10 * time.Second)
	receivedCount := 0

	for receivedCount < len(testCases) {
		select {
		case msg := <-received:
			receivedCount++
			testCase := msg.Headers["Test-Case"]
			t.Logf("Received: %s (data length: %d)", testCase, len(msg.Data))

			// Verify protocol compliance
			assert.NotEmpty(t, string(msg.Topic), "Topic should not be empty")
			assert.NotNil(t, msg.Data, "Data should not be nil")
			assert.NotNil(t, msg.Headers, "Headers should not be nil")

		case <-timeout:
			t.Fatalf("Timeout waiting for messages, received %d/%d", receivedCount, len(testCases))
		}
	}

	assert.Equal(t, len(testCases), receivedCount, "Should receive all protocol test messages")
}

// TestNetwork_MessageFormatConsistency tests message format consistency
func TestNetwork_MessageFormatConsistency(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("network.message.format")

	received := make(chan message.Message, 20)

	subscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Test JSON format consistency
	type TestData struct {
		ID      int       `json:"id"`
		Name    string    `json:"name"`
		Active  bool      `json:"active"`
		Created time.Time `json:"created"`
		Tags    []string  `json:"tags"`
	}

	testData := []TestData{
		{
			ID:      1,
			Name:    "Test User 1",
			Active:  true,
			Created: time.Now(),
			Tags:    []string{"tag1", "tag2"},
		},
		{
			ID:      2,
			Name:    "Test User 2",
			Active:  false,
			Created: time.Now().Add(-time.Hour),
			Tags:    []string{"tag3"},
		},
	}

	// Publish JSON messages
	for i, data := range testData {
		require.NoError(t, s.PublishJSON(ctx, topic, data, pub.WithHeaders(map[string]string{"Data-Index": fmt.Sprintf("%d", i)})))
	}

	// Verify JSON format consistency
	timeout := time.After(5 * time.Second)

	for i := range len(testData) {
		select {
		case msg := <-received:
			// Verify JSON format
			assert.Equal(t, "application/json", msg.Headers["Content-Type"],
				"JSON messages should have correct content type")

			// Verify data can be unmarshaled
			var decoded TestData
			err = json.Unmarshal(msg.Data, &decoded)
			assert.NoError(t, err, "JSON data should be valid")

			dataIndex := msg.Headers["Data-Index"]
			assert.NotEmpty(t, dataIndex, "Should have data index header")

		case <-timeout:
			t.Fatalf("Timeout waiting for JSON message %d", i)
		}
	}
}

// TestNetwork_SubscriptionManagementAtProtocolLevel tests subscription management
func TestNetwork_SubscriptionManagementAtProtocolLevel(t *testing.T) {
	s := helpers.CreateTestStream(t)
	baseTopic := "network.subscription.management"

	// Test wildcard-like patterns (using different topics)
	topics := []stream.Topic{
		stream.Topic(baseTopic + ".user.created"),
		stream.Topic(baseTopic + ".user.updated"),
		stream.Topic(baseTopic + ".user.deleted"),
		stream.Topic(baseTopic + ".order.created"),
		stream.Topic(baseTopic + ".order.completed"),
	}

	// Track messages per topic
	topicCounts := make(map[string]*int64)

	// Subscribe to each topic separately
	subscriptions := make([]stream.Subscription, len(topics))

	for i, topic := range topics {
		topicStr := string(topic)
		counter := new(int64) // Create pointer for atomic operations
		topicCounts[topicStr] = counter

		subscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
			atomic.AddInt64(counter, 1)
			return nil
		})

		sub, err := s.Subscribe(topic, subscriber)
		require.NoError(t, err)
		subscriptions[i] = sub
		defer sub.Stop()
	}

	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages to each topic
	messagesPerTopic := 10

	for _, topic := range topics {
		for i := range messagesPerTopic {
			msg := message.Message{
				Topic: topic,
				Data:  fmt.Appendf(nil, "message-%d", i),
				Headers: map[string]string{
					"Topic":              string(topic),
					"message.Message-ID": fmt.Sprintf("%s-%d", string(topic), i),
				},
				Time: time.Now(),
			}

			err := s.Publish(ctx, topic, msg)
			require.NoError(t, err)
		}
	}

	// Allow time for processing
	time.Sleep(2 * time.Second)

	// Verify subscription management
	for _, topic := range topics {
		topicStr := string(topic)
		count := atomic.LoadInt64(topicCounts[topicStr])
		assert.Equal(t, int64(messagesPerTopic), count,
			"Topic %s should receive all its messages", topicStr)
	}

	// Test dynamic subscription management
	// Add new subscription
	newTopic := stream.Topic(baseTopic + ".dynamic.test")
	dynamicCount := int64(0)

	dynamicSubscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		atomic.AddInt64(&dynamicCount, 1)
		return nil
	})

	dynamicSub, err := s.Subscribe(newTopic, dynamicSubscriber)
	require.NoError(t, err)

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	// Send messages to new topic
	for i := range 5 {
		msg := message.Message{
			Topic: newTopic,
			Data:  fmt.Appendf(nil, "dynamic-%d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, newTopic, msg)
		require.NoError(t, err)
	}

	// Allow processing
	time.Sleep(500 * time.Millisecond)

	// Verify dynamic subscription
	assert.Equal(t, int64(5), atomic.LoadInt64(&dynamicCount),
		"Dynamic subscription should receive messages")

	// Stop dynamic subscription
	err = dynamicSub.Stop()
	assert.NoError(t, err)

	// Send more messages (should not be received)
	for i := range 3 {
		msg := message.Message{
			Topic: newTopic,
			Data:  fmt.Appendf(nil, "after-stop-%d", i),
			Time:  time.Now(),
		}
		_ = s.Publish(ctx, newTopic, msg)
	}

	time.Sleep(500 * time.Millisecond)

	// Count should not increase
	assert.Equal(t, int64(5), atomic.LoadInt64(&dynamicCount),
		"Stopped subscription should not receive new messages")
}

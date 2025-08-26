package stream

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream"
	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/pub"
	"github.com/a2y-d5l/go-stream/sub"
	"github.com/a2y-d5l/go-stream/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Basic Integration Tests
// ============================================================================

// TestIntegration_CompletePublishSubscribeFlow tests the complete publish-subscribe flow
func TestIntegration_CompletePublishSubscribeFlow(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("integration.complete.pubsub")

	received := make(chan stream.Message, 100)
	subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		received <- msg
		return nil
	})

	subscription, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer subscription.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish test messages with different characteristics
	testMessages := []stream.Message{
		{
			Topic: topic,
			Data:  []byte("simple text message"),
			Headers: map[string]string{
				"Content-Type":      "text/plain",
				"stream.Message-ID": "msg-1",
			},
			Time: time.Now(),
		},
		{
			Topic: topic,
			Data:  []byte(`{"id": 123, "name": "JSON message"}`),
			Headers: map[string]string{
				"Content-Type":      "application/json",
				"stream.Message-ID": "msg-2",
			},
			Time: time.Now(),
		},
		{
			Topic: topic,
			Data:  make([]byte, 1024), // Binary data
			Headers: map[string]string{
				"Content-Type":      "application/octet-stream",
				"stream.Message-ID": "msg-3",
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

	// Verify all messages received
	receivedMessages := make([]stream.Message, 0, len(testMessages))
	timeout := time.After(5 * time.Second)

	for i := range testMessages {
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
			if original.Headers["stream.Message-ID"] == received.Headers["stream.Message-ID"] {
				assert.Equal(t, original.Data, received.Data)
				assert.Equal(t, original.Headers["Content-Type"], received.Headers["Content-Type"])
				found = true
				break
			}
		}
		assert.True(t, found, "stream.Message %d not found in received messages", i)
	}
}

// TestIntegration_RequestReplyRoundTrip tests request-reply patterns
func TestIntegration_RequestReplyRoundTrip(t *testing.T) {
	s := helpers.CreateTestStream(t)
	requestTopic := stream.Topic("integration.request")

	// Set up responder
	responder := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		// Echo back with a response
		response := stream.Message{
			Topic: stream.Topic(msg.Headers["Reply-To"]),
			Data:  []byte("Response: " + string(msg.Data)),
			Headers: map[string]string{
				"Content-Type": "text/plain",
				"Request-ID":   msg.Headers["Request-ID"],
			},
			Time: time.Now(),
		}

		if replyTo := msg.Headers["Reply-To"]; replyTo != "" {
			return s.Publish(ctx, stream.Topic(replyTo), response)
		}
		return nil
	})

	subscription, err := s.Subscribe(requestTopic, responder)
	require.NoError(t, err)
	defer require.NoError(t, subscription.Stop())

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send request and wait for reply
	request := stream.Message{
		Topic: requestTopic,
		Data:  []byte("Hello from requester"),
		Headers: map[string]string{
			"Request-ID": "req-123",
		},
		Time: time.Now(),
	}

	response, err := s.Request(ctx, requestTopic, request, 5*time.Second)
	require.NoError(t, err)

	// Verify response
	assert.Contains(t, string(response.Data), "Response: Hello from requester")
	assert.Equal(t, "req-123", response.Headers["Request-ID"])
}

// TestIntegration_MultiStreamCommunication tests communication between multiple streams
func TestIntegration_MultiStreamCommunication(t *testing.T) {
	// Create two separate streams
	stream1 := helpers.CreateTestStream(t)
	stream2 := helpers.CreateTestStream(t)

	topic := stream.Topic("integration.multi.stream")

	// Set up subscriber on stream2
	received := make(chan stream.Message, 10)
	subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		received <- msg
		return nil
	})

	sub, err := stream2.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish from stream1
	msg := stream.Message{
		Topic: topic,
		Data:  []byte("cross-stream message"),
		Headers: map[string]string{
			"Source": "stream1",
		},
		Time: time.Now(),
	}

	err = stream1.Publish(ctx, topic, msg)
	require.NoError(t, err)

	// Verify message received on stream2
	select {
	case receivedMsg := <-received:
		assert.Equal(t, msg.Data, receivedMsg.Data)
		assert.Equal(t, "stream1", receivedMsg.Headers["Source"])
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for cross-stream message")
	}
}

// TestIntegration_StreamLifecycleWithActiveSubscriptions tests graceful lifecycle management
func TestIntegration_StreamLifecycleWithActiveSubscriptions(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("integration.lifecycle")

	// Set up subscriber
	received := make(chan stream.Message, 10)
	subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		received <- msg
		return nil
	})

	subscription, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish some messages
	for i := range 5 {
		msg := stream.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "lifecycle message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Drain subscription gracefully
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()

	err = subscription.Drain(drainCtx)
	assert.NoError(t, err)

	// Verify all messages were processed
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 5, len(received))

	// Close stream gracefully
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()

	err = s.Close(closeCtx)
	assert.NoError(t, err)
}

// TestIntegration_GracefulShutdownWithInFlightMessages tests shutdown behavior
func TestIntegration_GracefulShutdownWithInFlightMessages(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("integration.shutdown")

	processed := int64(0)

	// Slow subscriber to create in-flight messages
	subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		time.Sleep(50 * time.Millisecond) // Simulate processing time
		atomic.AddInt64(&processed, 1)
		return nil
	})

	subscription, err := s.Subscribe(topic, subscriber, sub.WithConcurrency(2))
	require.NoError(t, err)

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages quickly to create backlog
	numMessages := 10
	for i := range numMessages {
		msg := stream.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "shutdown message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Give some time for processing to start
	time.Sleep(100 * time.Millisecond)

	// Drain subscription (should wait for in-flight messages)
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer drainCancel()

	err = subscription.Drain(drainCtx)
	assert.NoError(t, err)

	// All messages should be processed
	finalProcessed := atomic.LoadInt64(&processed)
	assert.Equal(t, int64(numMessages), finalProcessed)
}

// ============================================================================
// Real-World Usage Scenarios
// ============================================================================

// TestIntegration_MicroserviceCommunication tests microservice patterns
func TestIntegration_MicroserviceCommunication(t *testing.T) {
	s := helpers.CreateTestStream(t)

	// Service A publishes events
	eventTopic := stream.Topic("microservice.events.user.created")
	commandTopic := stream.Topic("microservice.commands.send.email")

	emailsSent := make(chan string, 10)
	// Email service subscribes to commands
	emailSub, err := s.Subscribe(commandTopic, stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		emailsSent <- string(msg.Data)
		return nil
	}))
	require.NoError(t, err)
	defer func() {
		if err := emailSub.Stop(); err != nil {
			t.Logf("Error stopping email subscription: %v", err)
		}
	}()

	// Event processor subscribes to events and issues commands
	eventSub, err := s.Subscribe(eventTopic, stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		// Process user creation event and send email command
		command := stream.Message{
			Topic: commandTopic,
			Data:  []byte("Welcome email for " + string(msg.Data)),
			Headers: map[string]string{
				"Command-Type": "send-email",
			},
			Time: time.Now(),
		}
		return s.Publish(ctx, commandTopic, command)
	}))
	require.NoError(t, err)
	defer func() {
		if err := eventSub.Stop(); err != nil {
			t.Logf("Error stopping event subscription: %v", err)
		}
	}()

	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Service A publishes user creation event
	require.NoError(t, s.Publish(ctx, eventTopic, stream.Message{
		Topic: eventTopic,
		Data:  []byte("user123"),
		Headers: map[string]string{
			"Event-Type": "user-created",
		},
		Time: time.Now(),
	}))

	// Verify email command was sent
	select {
	case email := <-emailsSent:
		assert.Contains(t, email, "Welcome email for user123")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for email command")
	}
}

// TestIntegration_WorkQueuePattern tests work distribution patterns
func TestIntegration_WorkQueuePattern(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("integration.work.queue")

	// Track which worker processed each job
	processedJobs := make(map[string]string) // job ID -> worker ID
	mutex := &sync.Mutex{}

	numWorkers := 3
	workers := make([]stream.Subscription, numWorkers)

	// Start workers (same queue group for load balancing)
	for i := range numWorkers {
		workerID := fmt.Sprintf("worker-%d", i)

		worker := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
			jobID := string(msg.Data)
			time.Sleep(10 * time.Millisecond) // Simulate work

			mutex.Lock()
			processedJobs[jobID] = workerID
			mutex.Unlock()

			return nil
		})

		subscription, err := s.Subscribe(topic, worker, sub.WithQueueGroupName("work-queue"))
		require.NoError(t, err)
		workers[i] = subscription
		defer func() {
			if err := subscription.Stop(); err != nil {
				t.Logf("Error stopping worker subscription: %v", err)
			}
		}()
	}

	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Submit jobs
	numJobs := 30
	for i := range numJobs {
		require.NoError(t, s.Publish(ctx, topic, stream.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "job-%d", i),
			Time:  time.Now(),
		}))
	}

	// Wait for all jobs to be processed
	time.Sleep(2 * time.Second)

	mutex.Lock()
	defer mutex.Unlock()

	// Verify all jobs were processed and distributed among workers
	assert.Equal(t, numJobs, len(processedJobs))

	workerCounts := make(map[string]int)
	for _, workerID := range processedJobs {
		workerCounts[workerID]++
	}

	// Each worker should have processed some jobs
	assert.Equal(t, numWorkers, len(workerCounts))
	for workerID, count := range workerCounts {
		assert.Greater(t, count, 0, "Worker %s should have processed at least one job", workerID)
	}
}

// TestIntegration_FanOutFanInPattern tests fan-out/fan-in patterns
func TestIntegration_FanOutFanInPattern(t *testing.T) {
	s := helpers.CreateTestStream(t)

	inputTopic := stream.Topic("integration.fanout.input")
	processingTopic := stream.Topic("integration.fanout.processing")
	resultTopic := stream.Topic("integration.fanout.result")

	// Fan-out: Input processor splits work
	fanOut := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		// Split input into multiple tasks
		taskData := string(msg.Data)
		for i := range 3 {
			task := stream.Message{
				Topic: processingTopic,
				Data:  fmt.Appendf(nil, "%s-part-%d", taskData, i),
				Headers: map[string]string{
					"Original-stream.Message": taskData,
					"Part":                    fmt.Sprintf("%d", i),
				},
				Time: time.Now(),
			}

			if err := s.Publish(ctx, processingTopic, task); err != nil {
				return err
			}
		}
		return nil
	})

	fanOutSub, err := s.Subscribe(inputTopic, fanOut)
	require.NoError(t, err)
	defer func() {
		if err := fanOutSub.Stop(); err != nil {
			t.Logf("Error stopping fan-out subscription: %v", err)
		}
	}()

	// Processors handle individual tasks
	results := make(chan string, 20)

	processorSub, err := s.Subscribe(
		processingTopic,
		stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
			// Process task and send result
			result := stream.Message{
				Topic: resultTopic,
				Data:  []byte("processed-" + string(msg.Data)),
				Headers: map[string]string{
					"Original-stream.Message": msg.Headers["Original-stream.Message"],
					"Part":                    msg.Headers["Part"],
				},
				Time: time.Now(),
			}
			return s.Publish(ctx, resultTopic, result)
		}),
		sub.WithConcurrency(3))
	require.NoError(t, err)
	defer func() {
		if err := processorSub.Stop(); err != nil {
			t.Logf("Error stopping processor subscription: %v", err)
		}
	}()

	// Fan-in: Result collector
	collectorSub, err := s.Subscribe(resultTopic, stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		results <- string(msg.Data)
		return nil
	}))
	require.NoError(t, err)
	defer func() {
		if err := collectorSub.Stop(); err != nil {
			t.Logf("Error stopping collector subscription: %v", err)
		}
	}()

	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send input message
	require.NoError(t, s.Publish(ctx, inputTopic, stream.Message{
		Topic: inputTopic,
		Data:  []byte("input-data-1"),
		Time:  time.Now(),
	}))

	// Collect results (should receive 3 processed parts)
	var collectedResults []string
	timeout := time.After(5 * time.Second)

	for i := range 3 {
		select {
		case result := <-results:
			collectedResults = append(collectedResults, result)
		case <-timeout:
			t.Fatalf("Timeout waiting for result %d", i)
		}
	}

	// Verify all parts were processed
	assert.Len(t, collectedResults, 3)
	for i := range 3 {
		expected := fmt.Sprintf("processed-input-data-1-part-%d", i)
		assert.Contains(t, collectedResults, expected)
	}
}

// ============================================================================
// Multi-Component Integration
// ============================================================================

// TestIntegration_PublisherSubscriberStreamCoordination tests component coordination
func TestIntegration_PublisherSubscriberStreamCoordination(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("integration.coordination")

	// Create multiple subscriber groups with different configurations
	group1Messages := make(chan stream.Message, 10)
	group2Messages := make(chan stream.Message, 10)

	// Group 1: High concurrency, block backpressure
	sub1, err := s.Subscribe(
		topic,
		stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
			group1Messages <- msg
			return nil
		}),
		sub.WithQueueGroupName("group1"),
		sub.WithConcurrency(5),
		sub.WithBackpressure(sub.BackpressureBlock))
	require.NoError(t, err)
	defer func() {
		if err := sub1.Stop(); err != nil {
			t.Logf("Error stopping subscription 1: %v", err)
		}
	}()

	// Group 2: Low concurrency, drop newest backpressure
	sub2, err := s.Subscribe(topic,
		stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
			time.Sleep(100 * time.Millisecond) // Slower processing
			group2Messages <- msg
			return nil
		}),
		sub.WithQueueGroupName("group2"),
		sub.WithConcurrency(1),
		sub.WithBackpressure(sub.BackpressureDropNewest),
		sub.WithBufferSize(5))
	require.NoError(t, err)
	defer func() {
		if err := sub2.Stop(); err != nil {
			t.Logf("Error stopping subscription 2: %v", err)
		}
	}()

	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages rapidly
	numMessages := 20
	for i := range numMessages {
		msg := stream.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "coordination message %d", i),
			Headers: map[string]string{
				"stream.Message-ID": fmt.Sprintf("msg-%d", i),
			},
			Time: time.Now(),
		}
		err = s.Publish(ctx, topic, msg, pub.WithFlush(i%5 == 0)) // Flush every 5th message
		require.NoError(t, err)
	}

	// Allow time for processing
	time.Sleep(3 * time.Second)

	// Group 1 should receive all messages (different queue group)
	assert.Equal(t, numMessages, len(group1Messages))

	// Group 2 might drop some due to backpressure and slower processing
	group2Count := len(group2Messages)
	assert.Greater(t, group2Count, 0, "Group 2 should receive some messages")
	assert.LessOrEqual(t, group2Count, numMessages, "Group 2 shouldn't receive more than published")
}

// TestIntegration_CodecMessageTransportIntegration tests integration between components
func TestIntegration_CodecMessageTransportIntegration(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("integration.codec.transport")

	// Test data structure
	type TestData struct {
		ID      int       `json:"id"`
		Name    string    `json:"name"`
		Active  bool      `json:"active"`
		Created time.Time `json:"created"`
	}

	received := make(chan TestData, 5)

	// JSON subscriber
	sub, err := client.SubscribeJSON(s, topic, func(ctx context.Context, data TestData) error {
		received <- data
		return nil
	})
	require.NoError(t, err)
	defer func() {
		if err := sub.Stop(); err != nil {
			t.Logf("Error stopping subscription: %v", err)
		}
	}()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test data
	testData := []TestData{
		{ID: 1, Name: "Test User 1", Active: true, Created: time.Now()},
		{ID: 2, Name: "Test User 2", Active: false, Created: time.Now().Add(-time.Hour)},
		{ID: 3, Name: "Test User 3", Active: true, Created: time.Now().Add(-time.Minute)},
	}

	// Publish using JSON codec
	for i, data := range testData {
		require.NoError(t, s.PublishJSON(ctx, topic, data, pub.WithHeaders(map[string]string{"stream.Message-Sequence": fmt.Sprintf("%d", i)})))
	}

	// Verify all data received and properly decoded
	receivedData := make([]TestData, 0, len(testData))
	timeout := time.After(5 * time.Second)

	for i := range testData {
		select {
		case data := <-received:
			receivedData = append(receivedData, data)
		case <-timeout:
			t.Fatalf("Timeout waiting for data %d", i)
		}
	}

	// Verify data integrity
	assert.Equal(t, len(testData), len(receivedData))

	// Check that all original data was received (order might differ)
	for _, original := range testData {
		found := false
		for _, received := range receivedData {
			if original.ID == received.ID &&
				original.Name == received.Name &&
				original.Active == received.Active {
				found = true
				break
			}
		}
		assert.True(t, found, "Original data with ID %d not found in received data", original.ID)
	}
}

// ============================================================================
// Error Handling Across Component Boundaries
// ============================================================================

// TestIntegration_ErrorHandlingAcrossComponents tests error propagation
func TestIntegration_ErrorHandlingAcrossComponents(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("integration.error.handling")

	// Track errors and successful processing
	errors := make(chan error, 10)
	successful := make(chan stream.Message, 10)

	// Subscriber that fails on specific conditions
	subscription, err := s.Subscribe(topic,
		stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
			msgContent := string(msg.Data)

			// Simulate different error conditions
			if msgContent == "panic-message" {
				panic("simulated panic")
			}
			if msgContent == "error-message" {
				err := fmt.Errorf("simulated processing error")
				errors <- err
				return err
			}

			// Successful processing
			successful <- msg
			return nil
		}),
		sub.WithConcurrency(2))
	require.NoError(t, err)
	defer func() {
		if err := subscription.Stop(); err != nil {
			t.Logf("Error stopping subscription: %v", err)
		}
	}()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Send mix of successful and failing messages
	for i, msg := range []string{
		"normal-message-1",
		"error-message",
		"normal-message-2",
		"panic-message",
		"normal-message-3",
	} {
		msg := stream.Message{
			Topic: topic,
			Data:  []byte(msg),
			Headers: map[string]string{
				"stream.Message-ID": fmt.Sprintf("msg-%d", i),
			},
			Time: time.Now(),
		}
		require.NoError(t, s.Publish(ctx, topic, msg))
	}

	// Allow time for processing
	time.Sleep(2 * time.Second)

	// Verify error handling behavior
	// - Normal messages should be processed successfully
	// - Error messages should generate errors but not crash subscriber
	// - Panic messages should be recovered and not crash the system

	successCount := len(successful)
	errorCount := len(errors)

	// Should have processed 3 normal messages successfully
	assert.Equal(t, 3, successCount)

	// Should have captured 1 explicit error (panic is recovered internally)
	assert.Equal(t, 1, errorCount)

	// Verify the system is still functional by sending another message
	require.NoError(t, s.Publish(ctx, topic, stream.Message{
		Topic: topic,
		Data:  []byte("final-test-message"),
		Time:  time.Now(),
	}))

	// Should receive the final message
	select {
	case <-successful:
		// Success - system is still functional
	case <-time.After(2 * time.Second):
		t.Fatal("System appears to be non-functional after error handling")
	}
}

// ============================================================================
// Configuration Consistency Tests
// ============================================================================

// TestIntegration_ConfigurationConsistencyAcrossComponents tests config consistency
func TestIntegration_ConfigurationConsistencyAcrossComponents(t *testing.T) {
	// Test with custom configuration
	customConfig := []client.Option{
		client.WithHost("127.0.0.1"),
		client.WithPort(0), // Dynamic port
		client.WithConnectTimeout(3 * time.Second),
		client.WithDrainTimeout(2 * time.Second),
		client.WithServerReadyTimeout(10 * time.Second),
		client.WithDisableJetStream(),
	}

	s := helpers.CreateTestStream(t, customConfig...)
	topic := stream.Topic("integration.config.consistency")

	// Create multiple subscribers with different configurations
	subscribers := make([]stream.Subscription, 3)
	received := make([]chan stream.Message, 3)

	for i := range 3 {
		received[i] = make(chan stream.Message, 5)
		index := i // Capture for closure

		subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
			received[index] <- msg
			return nil
		})

		// Different subscriber configurations
		var opts []stream.SubscribeOption
		switch i {
		case 0:
			opts = []stream.SubscribeOption{
				sub.WithQueueGroupName("group-a"),
				sub.WithConcurrency(1),
				sub.WithBufferSize(10),
			}
		case 1:
			opts = []stream.SubscribeOption{
				sub.WithQueueGroupName("group-b"),
				sub.WithConcurrency(3),
				sub.WithBufferSize(50),
				sub.WithBackpressure(sub.BackpressureDropNewest),
			}
		case 2:
			opts = []stream.SubscribeOption{
				sub.WithQueueGroupName("group-c"),
				sub.WithConcurrency(2),
				sub.WithBufferSize(20),
				sub.WithBackpressure(sub.BackpressureBlock),
			}
		}

		subscription, err := s.Subscribe(topic, subscriber, opts...)
		require.NoError(t, err)
		subscribers[i] = subscription
		defer subscription.Stop()
	}

	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Publish messages with different options
	publishOptions := [][]stream.PublishOption{
		{}, // Default
		{pub.WithFlush(true)},
		{pub.WithHeaders(map[string]string{"Priority": "high"})},
		{pub.WithMessageID("test-msg-id"), pub.WithFlush(true)},
	}

	for i, opts := range publishOptions {
		require.NoError(t, s.Publish(ctx, topic, stream.Message{
			Topic:   topic,
			Data:    fmt.Appendf(nil, "config test message %d", i),
			Headers: map[string]string{"Test-Round": fmt.Sprintf("%d", i)},
			Time:    time.Now(),
		}, opts...))
	}

	// Allow time for processing
	time.Sleep(2 * time.Second)

	// Verify all subscriber groups received all messages (different queue groups)
	for i := range 3 {
		count := len(received[i])
		assert.Equal(t, len(publishOptions), count,
			"Subscriber group %d should receive all messages", i)

		// Verify configuration consistency by checking message delivery
		for j := range count {
			select {
			case msg := <-received[i]:
				assert.Equal(t, fmt.Sprintf("%d", j), msg.Headers["Test-Round"])
			default:
				// Already counted, this is just structure verification
			}
		}
	}

	// Test graceful shutdown with all configurations
	for i, sub := range subscribers {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := sub.Drain(drainCtx)
		drainCancel()
		assert.NoError(t, err, "Subscriber %d should drain gracefully", i)
	}
}

// ============================================================================
// Memory and Resource Integration Tests
// ============================================================================

// TestIntegration_MemoryUsageStability tests memory behavior over time
func TestIntegration_MemoryUsageStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping memory stability test in short mode")
	}

	s := helpers.CreateTestStream(t)
	topic := stream.Topic("integration.memory.stability")

	// Record initial memory stats
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)

	// Create subscriber that processes messages
	processed := int64(0)
	subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		atomic.AddInt64(&processed, 1)
		// Simulate some work
		_ = string(msg.Data)
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber, sub.WithConcurrency(3))
	require.NoError(t, err)
	defer func() {
		if err := sub.Stop(); err != nil {
			t.Logf("Error stopping subscription: %v", err)
		}
	}()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Send messages over time
	numRounds := 10
	messagesPerRound := 100

	for round := range numRounds {
		for i := range messagesPerRound {
			msg := stream.Message{
				Topic: topic,
				Data:  fmt.Appendf(nil, "round %d message %d with some data payload", round, i),
				Headers: map[string]string{
					"Round":             fmt.Sprintf("%d", round),
					"stream.Message-ID": fmt.Sprintf("r%d-m%d", round, i),
				},
				Time: time.Now(),
			}

			err = s.Publish(ctx, topic, msg)
			require.NoError(t, err)
		}

		// Wait between rounds
		time.Sleep(500 * time.Millisecond)

		// Force GC every few rounds
		if round%3 == 0 {
			runtime.GC()
		}
	}

	// Wait for all processing to complete
	time.Sleep(2 * time.Second)

	// Check final processed count
	finalProcessed := atomic.LoadInt64(&processed)
	expectedTotal := int64(numRounds * messagesPerRound)
	assert.Equal(t, expectedTotal, finalProcessed)

	// Check memory usage
	runtime.GC()
	runtime.ReadMemStats(&memAfter)

	memGrowth := memAfter.Alloc - memBefore.Alloc

	// Log memory statistics
	t.Logf("Memory before: %d bytes", memBefore.Alloc)
	t.Logf("Memory after: %d bytes", memAfter.Alloc)
	t.Logf("Memory growth: %d bytes", memGrowth)
	t.Logf("Messages processed: %d", finalProcessed)

	// Memory growth should be reasonable (less than 10MB for this test)
	assert.Less(t, memGrowth, uint64(10*1024*1024), // 10MB
		"Memory growth should be reasonable")
}

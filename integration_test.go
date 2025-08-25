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
// Basic Integration Tests
// ============================================================================

// TestIntegration_CompletePublishSubscribeFlow tests the complete publish-subscribe flow
func TestIntegration_CompletePublishSubscribeFlow(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("integration.complete.pubsub")
	
	received := make(chan Message, 100)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})
	
	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()
	
	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Publish test messages with different characteristics
	testMessages := []Message{
		{
			Topic: topic,
			Data:  []byte("simple text message"),
			Headers: map[string]string{
				"Content-Type": "text/plain",
				"Message-ID":   "msg-1",
			},
			Time: time.Now(),
		},
		{
			Topic: topic,
			Data:  []byte(`{"id": 123, "name": "JSON message"}`),
			Headers: map[string]string{
				"Content-Type": "application/json",
				"Message-ID":   "msg-2",
			},
			Time: time.Now(),
		},
		{
			Topic: topic,
			Data:  make([]byte, 1024), // Binary data
			Headers: map[string]string{
				"Content-Type": "application/octet-stream",
				"Message-ID":   "msg-3",
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
			if original.Headers["Message-ID"] == received.Headers["Message-ID"] {
				assert.Equal(t, original.Data, received.Data)
				assert.Equal(t, original.Headers["Content-Type"], received.Headers["Content-Type"])
				found = true
				break
			}
		}
		assert.True(t, found, "Message %d not found in received messages", i)
	}
}

// TestIntegration_RequestReplyRoundTrip tests request-reply patterns
func TestIntegration_RequestReplyRoundTrip(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("integration.request")
	
	// Set up responder
	responder := SubscriberFunc(func(ctx context.Context, msg Message) error {
		// Echo back with a response
		response := Message{
			Topic: Topic(msg.Headers["Reply-To"]),
			Data:  []byte("Response: " + string(msg.Data)),
			Headers: map[string]string{
				"Content-Type": "text/plain",
				"Request-ID":   msg.Headers["Request-ID"],
			},
			Time: time.Now(),
		}
		
		if replyTo := msg.Headers["Reply-To"]; replyTo != "" {
			return s.Publish(ctx, Topic(replyTo), response)
		}
		return nil
	})
	
	sub, err := s.Subscribe(requestTopic, responder)
	require.NoError(t, err)
	defer sub.Stop()
	
	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Send request and wait for reply
	request := Message{
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
	stream1 := CreateTestStream(t)
	stream2 := CreateTestStream(t)
	
	topic := Topic("integration.multi.stream")
	
	// Set up subscriber on stream2
	received := make(chan Message, 10)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
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
	msg := Message{
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
	s := CreateTestStream(t)
	topic := Topic("integration.lifecycle")
	
	// Set up subscriber
	received := make(chan Message, 10)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})
	
	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	
	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Publish some messages
	for i := 0; i < 5; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("lifecycle message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}
	
	// Drain subscription gracefully
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer drainCancel()
	
	err = sub.Drain(drainCtx)
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
	s := CreateTestStream(t)
	topic := Topic("integration.shutdown")
	
	processed := int64(0)
	
	// Slow subscriber to create in-flight messages
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		time.Sleep(50 * time.Millisecond) // Simulate processing time
		atomic.AddInt64(&processed, 1)
		return nil
	})
	
	sub, err := s.Subscribe(topic, subscriber, WithConcurrency(2))
	require.NoError(t, err)
	
	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Publish messages quickly to create backlog
	numMessages := 10
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("shutdown message %d", i)),
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
	
	err = sub.Drain(drainCtx)
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
	s := CreateTestStream(t)
	
	// Service A publishes events
	eventTopic := Topic("microservice.events.user.created")
	commandTopic := Topic("microservice.commands.send.email")
	
	emailsSent := make(chan string, 10)
	
	// Email service subscribes to commands
	emailService := SubscriberFunc(func(ctx context.Context, msg Message) error {
		emailsSent <- string(msg.Data)
		return nil
	})
	
	emailSub, err := s.Subscribe(commandTopic, emailService)
	require.NoError(t, err)
	defer emailSub.Stop()
	
	// Event processor subscribes to events and issues commands
	eventProcessor := SubscriberFunc(func(ctx context.Context, msg Message) error {
		// Process user creation event and send email command
		command := Message{
			Topic: commandTopic,
			Data:  []byte("Welcome email for " + string(msg.Data)),
			Headers: map[string]string{
				"Command-Type": "send-email",
			},
			Time: time.Now(),
		}
		return s.Publish(ctx, commandTopic, command)
	})
	
	eventSub, err := s.Subscribe(eventTopic, eventProcessor)
	require.NoError(t, err)
	defer eventSub.Stop()
	
	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Service A publishes user creation event
	userEvent := Message{
		Topic: eventTopic,
		Data:  []byte("user123"),
		Headers: map[string]string{
			"Event-Type": "user-created",
		},
		Time: time.Now(),
	}
	
	err = s.Publish(ctx, eventTopic, userEvent)
	require.NoError(t, err)
	
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
	s := CreateTestStream(t)
	topic := Topic("integration.work.queue")
	
	// Track which worker processed each job
	processedJobs := make(map[string]string) // job ID -> worker ID
	mutex := &sync.Mutex{}
	
	numWorkers := 3
	workers := make([]Subscription, numWorkers)
	
	// Start workers (same queue group for load balancing)
	for i := 0; i < numWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i)
		
		worker := SubscriberFunc(func(ctx context.Context, msg Message) error {
			jobID := string(msg.Data)
			time.Sleep(10 * time.Millisecond) // Simulate work
			
			mutex.Lock()
			processedJobs[jobID] = workerID
			mutex.Unlock()
			
			return nil
		})
		
		sub, err := s.Subscribe(topic, worker, WithQueueGroup("work-queue"))
		require.NoError(t, err)
		workers[i] = sub
		defer sub.Stop()
	}
	
	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Submit jobs
	numJobs := 30
	for i := 0; i < numJobs; i++ {
		job := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("job-%d", i)),
			Time:  time.Now(),
		}
		err := s.Publish(ctx, topic, job)
		require.NoError(t, err)
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
	s := CreateTestStream(t)
	
	inputTopic := Topic("integration.fanout.input")
	processingTopic := Topic("integration.fanout.processing")
	resultTopic := Topic("integration.fanout.result")
	
	// Fan-out: Input processor splits work
	fanOut := SubscriberFunc(func(ctx context.Context, msg Message) error {
		// Split input into multiple tasks
		taskData := string(msg.Data)
		for i := 0; i < 3; i++ {
			task := Message{
				Topic: processingTopic,
				Data:  []byte(fmt.Sprintf("%s-part-%d", taskData, i)),
				Headers: map[string]string{
					"Original-Message": taskData,
					"Part":             fmt.Sprintf("%d", i),
				},
				Time: time.Now(),
			}
			err := s.Publish(ctx, processingTopic, task)
			if err != nil {
				return err
			}
		}
		return nil
	})
	
	fanOutSub, err := s.Subscribe(inputTopic, fanOut)
	require.NoError(t, err)
	defer fanOutSub.Stop()
	
	// Processors handle individual tasks
	results := make(chan string, 20)
	
	processor := SubscriberFunc(func(ctx context.Context, msg Message) error {
		// Process task and send result
		result := Message{
			Topic: resultTopic,
			Data:  []byte("processed-" + string(msg.Data)),
			Headers: map[string]string{
				"Original-Message": msg.Headers["Original-Message"],
				"Part":             msg.Headers["Part"],
			},
			Time: time.Now(),
		}
		return s.Publish(ctx, resultTopic, result)
	})
	
	processorSub, err := s.Subscribe(processingTopic, processor, WithConcurrency(3))
	require.NoError(t, err)
	defer processorSub.Stop()
	
	// Fan-in: Result collector
	collector := SubscriberFunc(func(ctx context.Context, msg Message) error {
		results <- string(msg.Data)
		return nil
	})
	
	collectorSub, err := s.Subscribe(resultTopic, collector)
	require.NoError(t, err)
	defer collectorSub.Stop()
	
	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Send input message
	input := Message{
		Topic: inputTopic,
		Data:  []byte("input-data-1"),
		Time:  time.Now(),
	}
	
	err = s.Publish(ctx, inputTopic, input)
	require.NoError(t, err)
	
	// Collect results (should receive 3 processed parts)
	var collectedResults []string
	timeout := time.After(5 * time.Second)
	
	for i := 0; i < 3; i++ {
		select {
		case result := <-results:
			collectedResults = append(collectedResults, result)
		case <-timeout:
			t.Fatalf("Timeout waiting for result %d", i)
		}
	}
	
	// Verify all parts were processed
	assert.Len(t, collectedResults, 3)
	for i := 0; i < 3; i++ {
		expected := fmt.Sprintf("processed-input-data-1-part-%d", i)
		assert.Contains(t, collectedResults, expected)
	}
}

// ============================================================================
// Multi-Component Integration
// ============================================================================

// TestIntegration_PublisherSubscriberStreamCoordination tests component coordination
func TestIntegration_PublisherSubscriberStreamCoordination(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("integration.coordination")
	
	// Create multiple subscriber groups with different configurations
	group1Messages := make(chan Message, 10)
	group2Messages := make(chan Message, 10)
	
	// Group 1: High concurrency, block backpressure
	group1Sub := SubscriberFunc(func(ctx context.Context, msg Message) error {
		group1Messages <- msg
		return nil
	})
	
	sub1, err := s.Subscribe(topic, group1Sub, 
		WithQueueGroup("group1"),
		WithConcurrency(5),
		WithBackpressure(BackpressureBlock))
	require.NoError(t, err)
	defer sub1.Stop()
	
	// Group 2: Low concurrency, drop newest backpressure
	group2Sub := SubscriberFunc(func(ctx context.Context, msg Message) error {
		time.Sleep(100 * time.Millisecond) // Slower processing
		group2Messages <- msg
		return nil
	})
	
	sub2, err := s.Subscribe(topic, group2Sub,
		WithQueueGroup("group2"),
		WithConcurrency(1),
		WithBackpressure(BackpressureDropNewest),
		WithBufferSize(5))
	require.NoError(t, err)
	defer sub2.Stop()
	
	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Publish messages rapidly
	numMessages := 20
	for i := 0; i < numMessages; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("coordination message %d", i)),
			Headers: map[string]string{
				"Message-ID": fmt.Sprintf("msg-%d", i),
			},
			Time: time.Now(),
		}
		err = s.Publish(ctx, topic, msg, WithFlush(i%5 == 0)) // Flush every 5th message
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
	s := CreateTestStream(t)
	topic := Topic("integration.codec.transport")
	
	// Test data structure
	type TestData struct {
		ID      int    `json:"id"`
		Name    string `json:"name"`
		Active  bool   `json:"active"`
		Created time.Time `json:"created"`
	}
	
	received := make(chan TestData, 5)
	
	// JSON subscriber
	handler := func(ctx context.Context, data TestData) error {
		received <- data
		return nil
	}
	
	sub, err := SubscribeJSON(s, topic, handler)
	require.NoError(t, err)
	defer sub.Stop()
	
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
		err = s.PublishJSON(ctx, topic, data, 
			WithHeaders(map[string]string{
				"Message-Sequence": fmt.Sprintf("%d", i),
			}))
		require.NoError(t, err)
	}
	
	// Verify all data received and properly decoded
	receivedData := make([]TestData, 0, len(testData))
	timeout := time.After(5 * time.Second)
	
	for i := 0; i < len(testData); i++ {
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
	s := CreateTestStream(t)
	topic := Topic("integration.error.handling")
	
	// Track errors and successful processing
	errors := make(chan error, 10)
	successful := make(chan Message, 10)
	
	// Subscriber that fails on specific conditions
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
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
	})
	
	sub, err := s.Subscribe(topic, subscriber, WithConcurrency(2))
	require.NoError(t, err)
	defer sub.Stop()
	
	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Send mix of successful and failing messages
	messages := []string{
		"normal-message-1",
		"error-message",
		"normal-message-2",
		"panic-message",
		"normal-message-3",
	}
	
	for i, content := range messages {
		msg := Message{
			Topic: topic,
			Data:  []byte(content),
			Headers: map[string]string{
				"Message-ID": fmt.Sprintf("msg-%d", i),
			},
			Time: time.Now(),
		}
		err = s.Publish(ctx, topic, msg)
		require.NoError(t, err)
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
	finalMsg := Message{
		Topic: topic,
		Data:  []byte("final-test-message"),
		Time:  time.Now(),
	}
	
	err = s.Publish(ctx, topic, finalMsg)
	require.NoError(t, err)
	
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
	customConfig := []Option{
		WithHost("127.0.0.1"),
		WithPort(0), // Dynamic port
		WithConnectTimeout(3 * time.Second),
		WithDrainTimeout(2 * time.Second),
		WithServerReadyTimeout(10 * time.Second),
		WithDisableJetStream(),
	}
	
	s := CreateTestStream(t, customConfig...)
	topic := Topic("integration.config.consistency")
	
	// Create multiple subscribers with different configurations
	subscribers := make([]Subscription, 3)
	received := make([]chan Message, 3)
	
	for i := 0; i < 3; i++ {
		received[i] = make(chan Message, 5)
		index := i // Capture for closure
		
		subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
			received[index] <- msg
			return nil
		})
		
		// Different subscriber configurations
		var opts []SubscribeOption
		switch i {
		case 0:
			opts = []SubscribeOption{
				WithQueueGroup("group-a"),
				WithConcurrency(1),
				WithBufferSize(10),
			}
		case 1:
			opts = []SubscribeOption{
				WithQueueGroup("group-b"),
				WithConcurrency(3),
				WithBufferSize(50),
				WithBackpressure(BackpressureDropNewest),
			}
		case 2:
			opts = []SubscribeOption{
				WithQueueGroup("group-c"),
				WithConcurrency(2),
				WithBufferSize(20),
				WithBackpressure(BackpressureBlock),
			}
		}
		
		sub, err := s.Subscribe(topic, subscriber, opts...)
		require.NoError(t, err)
		subscribers[i] = sub
		defer sub.Stop()
	}
	
	// Wait for subscriptions to establish
	time.Sleep(200 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Publish messages with different options
	publishOptions := [][]PublishOption{
		{}, // Default
		{WithFlush(true)},
		{WithHeaders(map[string]string{"Priority": "high"})},
		{WithMessageID("test-msg-id"), WithFlush(true)},
	}
	
	for i, opts := range publishOptions {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("config test message %d", i)),
			Headers: map[string]string{
				"Test-Round": fmt.Sprintf("%d", i),
			},
			Time: time.Now(),
		}
		
		err := s.Publish(ctx, topic, msg, opts...)
		require.NoError(t, err)
	}
	
	// Allow time for processing
	time.Sleep(2 * time.Second)
	
	// Verify all subscriber groups received all messages (different queue groups)
	for i := 0; i < 3; i++ {
		count := len(received[i])
		assert.Equal(t, len(publishOptions), count, 
			"Subscriber group %d should receive all messages", i)
		
		// Verify configuration consistency by checking message delivery
		for j := 0; j < count; j++ {
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
	
	s := CreateTestStream(t)
	topic := Topic("integration.memory.stability")
	
	// Record initial memory stats
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	
	// Create subscriber that processes messages
	processed := int64(0)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		atomic.AddInt64(&processed, 1)
		// Simulate some work
		_ = string(msg.Data)
		return nil
	})
	
	sub, err := s.Subscribe(topic, subscriber, WithConcurrency(3))
	require.NoError(t, err)
	defer sub.Stop()
	
	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Send messages over time
	numRounds := 10
	messagesPerRound := 100
	
	for round := 0; round < numRounds; round++ {
		for i := 0; i < messagesPerRound; i++ {
			msg := Message{
				Topic: topic,
				Data:  []byte(fmt.Sprintf("round %d message %d with some data payload", round, i)),
				Headers: map[string]string{
					"Round":      fmt.Sprintf("%d", round),
					"Message-ID": fmt.Sprintf("r%d-m%d", round, i),
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
	maxAcceptableGrowth := uint64(10 * 1024 * 1024) // 10MB
	assert.Less(t, memGrowth, maxAcceptableGrowth, 
		"Memory growth should be reasonable")
}

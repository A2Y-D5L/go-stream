package stream

import (
	"context"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream"
	"github.com/a2y-d5l/go-stream/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExamples_BasicUsage tests the basic usage example from README
func TestExamples_BasicUsage(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("examples.basic.test")

	// Test basic publish/subscribe pattern
	received := make(chan stream.Message, 10)
	sub, err := s.Subscribe(topic, stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		received <- msg
		return nil
	}))
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Publish a simple message
	msg := stream.Message{
		Topic: topic,
		Data:  []byte("Hello, World!"),
		Headers: map[string]string{
			"Content-Type": "text/plain",
		},
		Time: time.Now(),
	}

	err = s.Publish(ctx, topic, msg)
	require.NoError(t, err)

	// Verify message was received
	select {
	case receivedMsg := <-received:
		assert.Equal(t, msg.Topic, receivedMsg.Topic)
		assert.Equal(t, msg.Data, receivedMsg.Data)
		assert.Equal(t, msg.Headers["Content-Type"], receivedMsg.Headers["Content-Type"])
	case <-time.After(5 * time.Second):
		t.Fatal("stream.Message not received within timeout")
	}
}

// TestExamples_RequestReplyPattern tests request-reply messaging pattern
func TestExamples_RequestReplyPattern(t *testing.T) {
	s := helpers.CreateTestStream(t)
	requestTopic := stream.Topic("examples.request")
	replyTopic := stream.Topic("examples.reply")

	// Set up a subscriber that listens for requests and sends replies
	reqSub, err := s.Subscribe(requestTopic, stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		if replyTo, exists := msg.Headers["Reply-To"]; exists && replyTo == string(replyTopic) {
			response := stream.Message{
				Topic: stream.Topic(replyTo),
				Data:  []byte("Response: " + string(msg.Data)),
				Headers: map[string]string{
					"Content-Type":  "text/plain",
					"Request-ID":    msg.Headers["Request-ID"],
				},
				Time: time.Now(),
			}
			return s.Publish(ctx, stream.Topic(replyTo), response)
		}
		return nil
	}))
	require.NoError(t, err)
	defer reqSub.Stop()

	// Set up reply receiver
	responses := make(chan stream.Message, 10)
	replyReceiver := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		responses <- msg
		return nil
	})

	replySub, err := s.Subscribe(replyTopic, replyReceiver)
	require.NoError(t, err)
	defer replySub.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Send request
	request := stream.Message{
		Topic: requestTopic,
		Data:  []byte("Hello, Service!"),
		Headers: map[string]string{
			"Content-Type": "text/plain",
			"Reply-To":     string(replyTopic),
			"Request-ID":   "req-001",
		},
		Time: time.Now(),
	}

	err = s.Publish(ctx, requestTopic, request)
	require.NoError(t, err)

	// Verify response
	select {
	case response := <-responses:
		assert.Equal(t, replyTopic, response.Topic)
		assert.Equal(t, []byte("Response: Hello, Service!"), response.Data)
		assert.Equal(t, "req-001", response.Headers["Request-ID"])
	case <-time.After(5 * time.Second):
		t.Fatal("Response not received within timeout")
	}
}

// TestExamples_WorkQueuePattern tests work queue distribution pattern
func TestExamples_WorkQueuePattern(t *testing.T) {
	s := helpers.CreateTestStream(t)
	workTopic := stream.Topic("examples.work.queue")

	// Track work distribution across multiple workers
	worker1Jobs := make(chan stream.Message, 10)
	worker2Jobs := make(chan stream.Message, 10)

	// Worker 1
	worker1 := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		worker1Jobs <- msg
		time.Sleep(10 * time.Millisecond) // Simulate work
		return nil
	})

	// Worker 2
	worker2 := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		worker2Jobs <- msg
		time.Sleep(10 * time.Millisecond) // Simulate work
		return nil
	})

	sub1, err := s.Subscribe(workTopic, worker1)
	require.NoError(t, err)
	defer sub1.Stop()

	sub2, err := s.Subscribe(workTopic, worker2)
	require.NoError(t, err)
	defer sub2.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Send work items
	jobCount := 10
	for i := range jobCount {
		job := stream.Message{
			Topic: workTopic,
			Data:  []byte("job-" + string(rune('0'+i))),
			Headers: map[string]string{
				"Job-ID": "job-" + string(rune('0'+i)),
			},
			Time: time.Now(),
		}
		err := s.Publish(ctx, workTopic, job)
		require.NoError(t, err)
	}

	// Wait for work distribution
	time.Sleep(2 * time.Second)

	// Verify work was distributed (both workers should have received jobs)
	worker1Count := len(worker1Jobs)
	worker2Count := len(worker2Jobs)

	t.Logf("Work distribution: Worker1=%d, Worker2=%d", worker1Count, worker2Count)
	
	assert.Greater(t, worker1Count, 0, "Worker 1 should receive some jobs")
	assert.Greater(t, worker2Count, 0, "Worker 2 should receive some jobs")
	assert.Equal(t, jobCount, worker1Count+worker2Count, "All jobs should be processed")
}

// TestExamples_EventSourcing tests event sourcing pattern
func TestExamples_EventSourcing(t *testing.T) {
	s := helpers.CreateTestStream(t)
	eventTopic := stream.Topic("examples.events")

	// Event store
	events := make([]stream.Message, 0)
	eventReceiver := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		events = append(events, msg)
		return nil
	})

	sub, err := s.Subscribe(eventTopic, eventReceiver)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Publish sequence of events
	eventSequence := []struct {
		eventType string
		data      string
	}{
		{"UserCreated", `{"userId": "123", "email": "user@example.com"}`},
		{"UserUpdated", `{"userId": "123", "email": "newemail@example.com"}`},
		{"UserDeleted", `{"userId": "123"}`},
	}

	for i, event := range eventSequence {
		msg := stream.Message{
			Topic: eventTopic,
			Data:  []byte(event.data),
			Headers: map[string]string{
				"Event-Type":   event.eventType,
				"Event-ID":     "evt-" + string(rune('0'+i)),
				"Content-Type": "application/json",
			},
			Time: time.Now(),
		}
		err := s.Publish(ctx, eventTopic, msg)
		require.NoError(t, err)
	}

	// Wait for events to be stored
	time.Sleep(1 * time.Second)

	// Verify event sequence
	require.Len(t, events, 3, "Should have received all events")
	
	assert.Equal(t, "UserCreated", events[0].Headers["Event-Type"])
	assert.Equal(t, "UserUpdated", events[1].Headers["Event-Type"])
	assert.Equal(t, "UserDeleted", events[2].Headers["Event-Type"])
}

// TestExamples_FanOutPattern tests fan-out messaging pattern
func TestExamples_FanOutPattern(t *testing.T) {
	s := helpers.CreateTestStream(t)
	broadcastTopic := stream.Topic("examples.broadcast")

	// Multiple services listening to the same events
	emailService := make(chan stream.Message, 10)
	smsService := make(chan stream.Message, 10)
	auditService := make(chan stream.Message, 10)

	// Email notification service
	emailSub := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		emailService <- msg
		return nil
	})

	// SMS notification service
	smsSub := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		smsService <- msg
		return nil
	})

	// Audit logging service
	auditSub := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		auditService <- msg
		return nil
	})

	sub1, err := s.Subscribe(broadcastTopic, emailSub)
	require.NoError(t, err)
	defer sub1.Stop()

	sub2, err := s.Subscribe(broadcastTopic, smsSub)
	require.NoError(t, err)
	defer sub2.Stop()

	sub3, err := s.Subscribe(broadcastTopic, auditSub)
	require.NoError(t, err)
	defer sub3.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Broadcast an event
	event := stream.Message{
		Topic: broadcastTopic,
		Data:  []byte(`{"eventType": "OrderPlaced", "orderId": "12345"}`),
		Headers: map[string]string{
			"Event-Type":   "OrderPlaced",
			"Content-Type": "application/json",
		},
		Time: time.Now(),
	}

	err = s.Publish(ctx, broadcastTopic, event)
	require.NoError(t, err)

	// Wait for fan-out
	time.Sleep(1 * time.Second)

	// Verify all services received the event
	assert.Len(t, emailService, 1, "Email service should receive the event")
	assert.Len(t, smsService, 1, "SMS service should receive the event")
	assert.Len(t, auditService, 1, "Audit service should receive the event")

	// Verify event content consistency
	emailEvent := <-emailService
	smsEvent := <-smsService
	auditEvent := <-auditService

	assert.Equal(t, event.Data, emailEvent.Data)
	assert.Equal(t, event.Data, smsEvent.Data)
	assert.Equal(t, event.Data, auditEvent.Data)
}

// TestExamples_CompositeIntegration tests multiple patterns working together
func TestExamples_CompositeIntegration(t *testing.T) {
	s := helpers.CreateTestStream(t)

	// Topics for different patterns
	orderTopic := stream.Topic("examples.orders")
	notificationTopic := stream.Topic("examples.notifications")
	auditTopic := stream.Topic("examples.audit")

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	// Order processor that fans out to notifications and audit
	orderProcessor := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		// Fan out to notification service
		notification := stream.Message{
			Topic: notificationTopic,
			Data:  []byte("Order processed: " + string(msg.Data)),
			Headers: map[string]string{
				"Source":       "OrderProcessor",
				"Content-Type": "text/plain",
			},
			Time: time.Now(),
		}
		if err := s.Publish(ctx, notificationTopic, notification); err != nil {
			return err
		}

		// Fan out to audit service
		auditLog := stream.Message{
			Topic: auditTopic,
			Data:  []byte("AUDIT: " + string(msg.Data)),
			Headers: map[string]string{
				"Source":       "OrderProcessor",
				"Content-Type": "text/plain",
			},
			Time: time.Now(),
		}
		return s.Publish(ctx, auditTopic, auditLog)
	})

	notifications := make(chan stream.Message, 10)
	notificationService := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		notifications <- msg
		return nil
	})

	audits := make(chan stream.Message, 10)
	auditService := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		audits <- msg
		return nil
	})

	// Set up subscriptions
	orderSub, err := s.Subscribe(orderTopic, orderProcessor)
	require.NoError(t, err)
	defer orderSub.Stop()

	notifSub, err := s.Subscribe(notificationTopic, notificationService)
	require.NoError(t, err)
	defer notifSub.Stop()

	auditSub, err := s.Subscribe(auditTopic, auditService)
	require.NoError(t, err)
	defer auditSub.Stop()

	// Wait for subscriptions to establish
	time.Sleep(100 * time.Millisecond)

	// Send an order
	order := stream.Message{
		Topic: orderTopic,
		Data:  []byte(`{"orderId": "ORD-123", "amount": 99.99}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
		Time: time.Now(),
	}

	err = s.Publish(ctx, orderTopic, order)
	require.NoError(t, err)

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify composite integration
	assert.Len(t, notifications, 1, "Should have notification")
	assert.Len(t, audits, 1, "Should have audit log")

	notification := <-notifications
	audit := <-audits

	assert.Contains(t, string(notification.Data), "Order processed")
	assert.Contains(t, string(audit.Data), "AUDIT:")
	assert.Equal(t, "OrderProcessor", notification.Headers["Source"])
	assert.Equal(t, "OrderProcessor", audit.Headers["Source"])
}

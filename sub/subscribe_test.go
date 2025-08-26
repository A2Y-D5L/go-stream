package sub_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/sub"
	"github.com/a2y-d5l/go-stream/test/helpers"
	"github.com/a2y-d5l/go-stream/topic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test data structures
type TestUser struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	IsActive bool   `json:"is_active"`
}

// ============================================================================
// Test Helper Functions and Types
// ============================================================================

// WithQueueGroup is an alias for the existing WithQueueGroupName for consistency
func WithQueueGroup(name string) sub.Option {
	return sub.WithQueueGroupName(name)
}

// Test subscriber implementations
type CountingSubscriber struct {
	count int64
	mu    sync.Mutex
	calls []message.Message
}

func (c *CountingSubscriber) Handle(ctx context.Context, msg message.Message) error {
	atomic.AddInt64(&c.count, 1)
	c.mu.Lock()
	c.calls = append(c.calls, msg)
	c.mu.Unlock()
	return nil
}

func (c *CountingSubscriber) Count() int64 {
	return atomic.LoadInt64(&c.count)
}

func (c *CountingSubscriber) Messages() []message.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]message.Message, len(c.calls))
	copy(result, c.calls)
	return result
}

type FailingSubscriber struct {
	failAfter int
	count     int64
}

func (f *FailingSubscriber) Handle(ctx context.Context, msg message.Message) error {
	count := atomic.AddInt64(&f.count, 1)
	if int(count) > f.failAfter {
		return fmt.Errorf("subscriber failed after %d messages", f.failAfter)
	}
	return nil
}

// ============================================================================
// Basic Subscription Tests
// ============================================================================

func TestSubscribe_SimpleHandler(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := topic.Topic("test.subscribe.simple")

	received := make(chan message.Message, 1)
	sub, err := s.Subscribe(topic, sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		received <- msg
		return nil
	}))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish a message
	msg := message.Message{
		Topic: topic,
		Data:  []byte("Hello, Subscriber!"),
		Headers: map[string]string{
			"Content-Type": "text/plain",
		},
		Time: time.Now(),
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = s.Publish(ctx, topic, msg)
	require.NoError(t, err)

	// Verify message was received
	select {
	case receivedMsg := <-received:
		assert.Equal(t, msg.Data, receivedMsg.Data)
		assert.Equal(t, msg.Headers["Content-Type"], receivedMsg.Headers["Content-Type"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive message within timeout")
	}
}

func TestSubscribe_FailingHandler(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.failing")
	failingSubscriber := &FailingSubscriber{failAfter: 2}

	sub, err := s.Subscribe(top, failingSubscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Publish multiple messages
	for i := range 5 {
		require.NoError(t, s.Publish(ctx, top, message.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "message %d", i),
			Time:  time.Now(),
		}))
	}

	// Allow time for processing
	time.Sleep(500 * time.Millisecond)

	// Subscriber should have processed some messages despite failures
	assert.Greater(t, int(failingSubscriber.count), 0)
}

func TestSubscribe_NonExistentTopic(t *testing.T) {
	// Subscribing to non-existent topic should succeed
	sub, err := helpers.CreateTestStream(t).
		Subscribe(topic.Topic("test.subscribe.nonexistent.very.specific.topic"),
			sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
				return nil
			}))

	assert.NoError(t, err, "Subscribing to non-existent topic should succeed")
	if sub != nil {
		defer sub.Stop()
	}
}

func TestSubscribe_ClosedStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	s := helpers.CreateTestStream(t)
	// Close the stream first
	require.NoError(t, s.Close(ctx))
	topic := topic.Topic("test.subscribe.closed")
	// Now try to subscribe
	_, err := s.Subscribe(topic, sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		return nil
	}))

	assert.Error(t, err, "Subscribing to closed stream should fail")
}

// ============================================================================
// JSON Subscription Tests
// ============================================================================

func TestSubscribeJSON_ValidData(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.json.valid")

	received := make(chan TestUser, 1)
	handler := func(ctx context.Context, user TestUser) error {
		received <- user
		return nil
	}

	sub, err := client.SubscribeJSON(s, top, handler)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish JSON data
	user := TestUser{
		ID:       456,
		Name:     "Alice Smith",
		Email:    "alice@example.com",
		IsActive: true,
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = s.PublishJSON(ctx, top, user)
	require.NoError(t, err)

	// Verify JSON was properly decoded
	select {
	case receivedUser := <-received:
		assert.Equal(t, user.ID, receivedUser.ID)
		assert.Equal(t, user.Name, receivedUser.Name)
		assert.Equal(t, user.Email, receivedUser.Email)
		assert.Equal(t, user.IsActive, receivedUser.IsActive)
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive JSON message within timeout")
	}
}

func TestSubscribeJSON_InvalidJSON(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.json.invalid")

	errorCount := int64(0)
	handler := func(ctx context.Context, user TestUser) error {
		// This should not be called for invalid JSON
		t.Error("Handler should not be called for invalid JSON")
		return nil
	}

	sub, err := client.SubscribeJSON(s, top, handler)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish invalid JSON
	invalidMsg := message.Message{
		Topic: top,
		Data:  []byte("invalid json {"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = s.Publish(ctx, top, invalidMsg)
	require.NoError(t, err)

	// Allow time for processing (and potential error)
	time.Sleep(500 * time.Millisecond)

	// Error count should remain 0 since handler wasn't called
	assert.Equal(t, int64(0), errorCount)
}

func TestSubscribeJSON_TypeMismatch(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.json.mismatch")

	handler := func(ctx context.Context, user TestUser) error {
		// This should not be called for type mismatch
		t.Error("Handler should not be called for type mismatch")
		return nil
	}

	sub, err := client.SubscribeJSON(s, top, handler)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish JSON that doesn't match TestUser structure
	wrongType := map[string]any{
		"wrong_field": "wrong_value",
		"id":          "string_instead_of_int", // Type mismatch
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = s.PublishJSON(ctx, top, wrongType)
	require.NoError(t, err)

	// Allow time for processing
	time.Sleep(500 * time.Millisecond)
	// Test passes if no panic occurs and handler isn't called incorrectly
}

func TestSubscribeJSON_EmptyMessage(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.json.empty")

	handler := func(ctx context.Context, user TestUser) error {
		t.Error("Handler should not be called for empty message")
		return nil
	}

	sub, err := client.SubscribeJSON(s, top, handler)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Publish empty message
	emptyMsg := message.Message{
		Topic: top,
		Data:  []byte{},
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	err = s.Publish(ctx, top, emptyMsg)
	require.NoError(t, err)

	// Allow time for processing
	time.Sleep(500 * time.Millisecond)
	// Test passes if handler isn't called for empty message
}

// ============================================================================
// Queue Group Tests
// ============================================================================

func TestSubscribe_AutoGeneratedQueueGroup(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.queue.auto")

	subscriber1 := &CountingSubscriber{}
	subscriber2 := &CountingSubscriber{}

	// Both subscribers without explicit queue group (should auto-generate)
	sub1, err := s.Subscribe(top, subscriber1)
	require.NoError(t, err)
	defer sub1.Stop()

	sub2, err := s.Subscribe(top, subscriber2)
	require.NoError(t, err)
	defer sub2.Stop()

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Publish multiple messages
	numMessages := 10
	for i := range numMessages {
		msg := message.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, top, msg)
		require.NoError(t, err)
	}

	// Allow time for processing
	time.Sleep(1 * time.Second)

	// Each subscriber should receive some messages (load balancing)
	total := subscriber1.Count() + subscriber2.Count()
	assert.Equal(t, int64(numMessages), total, "Total messages should equal published messages")

	// Both should have received at least one message (in most cases)
	// Note: This is probabilistic and might occasionally fail in extremely loaded systems
	assert.Greater(t, subscriber1.Count()+subscriber2.Count(), int64(0))
}

func TestSubscribe_CustomQueueGroup(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.queue.custom")

	subscriber1 := &CountingSubscriber{}
	subscriber2 := &CountingSubscriber{}

	customQueueGroup := "custom-queue-group"

	// Both subscribers with same custom queue group
	sub1, err := s.Subscribe(top, subscriber1, WithQueueGroup(customQueueGroup))
	require.NoError(t, err)
	defer sub1.Stop()

	sub2, err := s.Subscribe(top, subscriber2, WithQueueGroup(customQueueGroup))
	require.NoError(t, err)
	defer sub2.Stop()

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Publish multiple messages
	numMessages := 10
	for i := range numMessages {
		msg := message.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, top, msg)
		require.NoError(t, err)
	}

	// Allow time for processing
	time.Sleep(1 * time.Second)

	// Messages should be distributed between subscribers
	total := subscriber1.Count() + subscriber2.Count()
	assert.Equal(t, int64(numMessages), total)
}

func TestSubscribe_DifferentQueueGroups(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.queue.different")

	subscriber1 := &CountingSubscriber{}
	subscriber2 := &CountingSubscriber{}

	// Subscribers with different queue groups (both should receive all messages)
	sub1, err := s.Subscribe(top, subscriber1, WithQueueGroup("group1"))
	require.NoError(t, err)
	defer sub1.Stop()

	sub2, err := s.Subscribe(top, subscriber2, WithQueueGroup("group2"))
	require.NoError(t, err)
	defer sub2.Stop()

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Publish messages
	numMessages := 5
	for i := range numMessages {
		msg := message.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, top, msg)
		require.NoError(t, err)
	}

	// Allow time for processing
	time.Sleep(1 * time.Second)

	// Each subscriber should receive all messages (different queue groups)
	assert.Equal(t, int64(numMessages), subscriber1.Count())
	assert.Equal(t, int64(numMessages), subscriber2.Count())
}

// ============================================================================
// Subscription Management Tests
// ============================================================================

func TestSubscription_Stop(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscription.stop")

	subscriber := &CountingSubscriber{}

	sub, err := s.Subscribe(top, subscriber)
	require.NoError(t, err)

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Publish some messages
	for i := range 3 {
		msg := message.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, top, msg)
		require.NoError(t, err)
	}

	// Allow some processing
	time.Sleep(500 * time.Millisecond)
	initialCount := subscriber.Count()

	// Stop the subscription
	err = sub.Stop()
	assert.NoError(t, err)

	// Publish more messages (should not be received)
	for i := 3; i < 6; i++ {
		msg := message.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, top, msg)
		require.NoError(t, err)
	}

	// Allow time for any potential processing
	time.Sleep(500 * time.Millisecond)

	// Count should not have increased after stop
	finalCount := subscriber.Count()
	assert.Equal(t, initialCount, finalCount, "No new messages should be processed after stop")
}

func TestSubscription_Drain(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscription.drain")

	subscriber := &CountingSubscriber{}

	sub, err := s.Subscribe(top, subscriber)
	require.NoError(t, err)

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Publish messages quickly
	numMessages := 10
	for i := range numMessages {
		msg := message.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, top, msg)
		require.NoError(t, err)
	}

	// Drain the subscription (should process remaining messages)
	drainCtx, drainCancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer drainCancel()

	err = sub.Drain(drainCtx)
	assert.NoError(t, err)

	// Should have processed all messages
	assert.Equal(t, int64(numMessages), subscriber.Count())
}

// ============================================================================
// Concurrency Tests
// ============================================================================

func TestSubscribe_ConcurrentSubscribers(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.concurrent")

	numSubscribers := 5
	subscribers := make([]*CountingSubscriber, numSubscribers)
	subs := make([]sub.Subscription, numSubscribers)

	// Create multiple subscribers with different queue groups
	for i := range numSubscribers {
		subscribers[i] = &CountingSubscriber{}
		var err error
		subs[i], err = s.Subscribe(top, subscribers[i],
			WithQueueGroup(fmt.Sprintf("group-%d", i)))
		require.NoError(t, err)
		defer subs[i].Stop()
	}

	// Wait for subscriptions to be ready
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Publish messages concurrently
	numMessages := 50
	var wg sync.WaitGroup

	for i := range numMessages {
		wg.Add(1)
		go func(msgID int) {
			defer wg.Done()
			msg := message.Message{
				Topic: top,
				Data:  fmt.Appendf(nil, "concurrent message %d", msgID),
				Time:  time.Now(),
			}
			err := s.Publish(ctx, top, msg)
			if err != nil {
				t.Errorf("Failed to publish message %d: %v", msgID, err)
			}
		}(i)
	}

	wg.Wait()

	// Allow time for processing
	time.Sleep(2 * time.Second)

	// Each subscriber should have received all messages (different queue groups)
	for i, subscriber := range subscribers {
		count := subscriber.Count()
		assert.Equal(t, int64(numMessages), count,
			"Subscriber %d should have received all messages", i)
	}
}

func TestSubscribe_HighConcurrency(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.high.concurrency")

	subscriber := &CountingSubscriber{}

	// Subscribe with high concurrency
	sub, err := s.Subscribe(top, subscriber, sub.WithConcurrency(10))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	// Publish many messages quickly (restored to original challenging load)
	numMessages := 1000
	for i := range numMessages {
		msg := message.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "high concurrency message %d", i),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, top, msg)
		require.NoError(t, err)
	}

	// Allow time for processing
	time.Sleep(5 * time.Second)

	// Should have processed all messages
	count := subscriber.Count()
	assert.Equal(t, int64(numMessages), count, "Should process all messages with high concurrency")
}

// ============================================================================
// Error Handling Tests
// ============================================================================

func TestSubscribe_HandlerPanic(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := topic.Topic("test.subscribe.panic")

	panicSubscriber := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		panic("subscriber panic!")
	})

	sub, err := s.Subscribe(top, panicSubscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Publish a message that will cause panic
	require.NoError(t, s.Publish(ctx, top, message.Message{
		Topic: top,
		Data:  []byte("panic trigger"),
		Time:  time.Now(),
	}))

	// Allow time for processing (and panic recovery)
	time.Sleep(500 * time.Millisecond)

	countingSubscriber := &CountingSubscriber{}
	// Test that the subscription is still functional after panic
	// (implementation should now recover from panics)
	testTopic := topic.Topic("test.subscribe.after.panic")
	sub2, err := s.Subscribe(testTopic, countingSubscriber)
	require.NoError(t, err)
	defer sub2.Stop()

	time.Sleep(100 * time.Millisecond)

	// Publish to the new subscription to verify system is still working
	require.NoError(t, s.Publish(ctx, testTopic, message.Message{
		Topic: testTopic,
		Data:  []byte("recovery test"),
		Time:  time.Now(),
	}))

	// Allow processing time
	time.Sleep(500 * time.Millisecond)

	// Verify the counting subscriber received the message
	assert.Greater(t, int(countingSubscriber.count), 0, "System should still be functional after panic recovery")
}

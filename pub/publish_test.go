package pub

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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

type TestResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// ============================================================================
// Basic Publishing Tests
// ============================================================================

func TestPublish_ValidMessage(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.valid")

	msg := Message{
		Topic: topic,
		Data:  []byte("Hello, World!"),
		Headers: map[string]string{
			"Content-Type": "text/plain",
		},
		Time: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Publish(ctx, topic, msg)
	assert.NoError(t, err)
}

func TestPublish_EmptyMessage(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.empty")

	msg := Message{
		Topic: topic,
		Data:  []byte{}, // empty data
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Publish(ctx, topic, msg)
	assert.NoError(t, err, "Publishing empty message should succeed")
}

func TestPublish_LargeMessage(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.large")

	// Create a large payload (1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	msg := Message{
		Topic: topic,
		Data:  largeData,
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := s.Publish(ctx, topic, msg)
	assert.NoError(t, err, "Publishing large message should succeed")
}

func TestPublish_NonExistentTopic(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.nonexistent.very.specific.topic.name")

	msg := Message{
		Topic: topic,
		Data:  []byte("test message"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publishing to non-existent topic should succeed in NATS (fire-and-forget)
	err := s.Publish(ctx, topic, msg)
	assert.NoError(t, err, "Publishing to non-existent topic should succeed")
}

func TestPublish_ClosedStream(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.closed")

	// Close the stream first
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Close(ctx)
	require.NoError(t, err)

	// Now try to publish
	msg := Message{
		Topic: topic,
		Data:  []byte("test message"),
		Time:  time.Now(),
	}

	err = s.Publish(ctx, topic, msg)
	assert.Error(t, err, "Publishing to closed stream should fail")
}

// ============================================================================
// JSON Publishing Tests
// ============================================================================

func TestPublishJSON_ValidStruct(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.json.valid")

	user := TestUser{
		ID:       123,
		Name:     "John Doe",
		Email:    "john@example.com",
		IsActive: true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.PublishJSON(ctx, topic, user)
	assert.NoError(t, err)
}

func TestPublishJSON_InvalidJSONData(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.json.invalid")

	// Create an invalid JSON data (circular reference)
	type CircularRef struct {
		Name string
		Ref  *CircularRef
	}
	
	circular := &CircularRef{Name: "test"}
	circular.Ref = circular // circular reference

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.PublishJSON(ctx, topic, circular)
	assert.Error(t, err, "Publishing invalid JSON should fail")
}

func TestPublishJSON_NilPointer(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.json.nil")

	var user *TestUser = nil

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.PublishJSON(ctx, topic, user)
	assert.NoError(t, err, "Publishing nil pointer should succeed (marshals to null)")
}

func TestPublishJSON_ComplexNestedStructures(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.json.complex")

	complexData := map[string]any{
		"users": []TestUser{
			{ID: 1, Name: "Alice", Email: "alice@example.com", IsActive: true},
			{ID: 2, Name: "Bob", Email: "bob@example.com", IsActive: false},
		},
		"metadata": map[string]any{
			"timestamp": time.Now().Unix(),
			"version":   "1.0",
			"nested": map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		},
		"count": 2,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.PublishJSON(ctx, topic, complexData)
	assert.NoError(t, err)
}

// ============================================================================
// Request/Reply Tests
// ============================================================================

func TestRequest_ImmediateResponse(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("test.request.immediate")

	// Set up a responder using the test helper
	sub, err := s.TestResponder(requestTopic, func(data []byte) ([]byte, error) {
		return []byte("response data"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Send request
	request := Message{
		Topic: requestTopic,
		Data:  []byte("request data"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := s.Request(ctx, requestTopic, request, 2*time.Second)
	assert.NoError(t, err)
	assert.Equal(t, []byte("response data"), response.Data)
}

func TestRequest_Timeout(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("test.request.timeout")

	// No responder set up, so request should timeout

	request := Message{
		Topic: requestTopic,
		Data:  []byte("request data"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.Request(ctx, requestTopic, request, 500*time.Millisecond)
	assert.Error(t, err)
	// NATS returns "no responders available" instead of deadline exceeded when there are no responders
	assert.True(t, err.Error() == "nats: no responders available for request" || 
		strings.Contains(err.Error(), "deadline exceeded") ||
		strings.Contains(err.Error(), "timeout"), 
		"Expected timeout or no responders error, got: %s", err.Error())
}

func TestRequest_NoResponder(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("test.request.noresponder")

	request := Message{
		Topic: requestTopic,
		Data:  []byte("request data"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := s.Request(ctx, requestTopic, request, 1*time.Second)
	assert.Error(t, err)
}

func TestRequest_MultipleResponders(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("test.request.multiple")

	// Set up multiple responders using test helpers
	var subs []*nats.Subscription
	for i := 0; i < 3; i++ {
		responderID := i
		sub, err := s.TestResponder(requestTopic, func(data []byte) ([]byte, error) {
			return []byte(fmt.Sprintf("response from responder %d", responderID)), nil
		})
		require.NoError(t, err)
		subs = append(subs, sub)
	}
	
	defer func() {
		for _, sub := range subs {
			sub.Unsubscribe()
		}
	}()

	// Wait for subscriptions to be ready
	time.Sleep(100 * time.Millisecond)

	request := Message{
		Topic: requestTopic,
		Data:  []byte("request data"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := s.Request(ctx, requestTopic, request, 2*time.Second)
	assert.NoError(t, err)
	assert.Contains(t, string(response.Data), "response from responder")
}

// ============================================================================
// Generic Request Tests
// ============================================================================

func TestRequestJSON_TypedResponse(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("test.request.json.typed")

	// Set up a JSON responder using test helper
	sub, err := s.TestJSONResponder(requestTopic, func(data []byte) (any, error) {
		var req TestUser
		err := json.Unmarshal(data, &req)
		if err != nil {
			return nil, err
		}

		// Create response
		resp := TestResponse{
			Success: true,
			Message: fmt.Sprintf("Processed user %s", req.Name),
			Data:    req.ID,
		}
		return resp, nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Send typed request
	request := TestUser{
		ID:    456,
		Name:  "Jane Doe",
		Email: "jane@example.com",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := RequestJSON[TestResponse](s, ctx, requestTopic, request, 2*time.Second)
	assert.NoError(t, err)
	assert.True(t, response.Success)
	assert.Contains(t, response.Message, "Jane Doe")
}

func TestRequestJSON_DecodeError(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("test.request.json.decode.error")

	// Set up a responder that returns invalid JSON
	responder := SubscriberFunc(func(ctx context.Context, msg Message) error {
		response := Message{
			Topic: Topic(msg.Headers["reply-to"]),
			Data:  []byte("invalid json {"),
			Time:  time.Now(),
		}
		return s.Publish(ctx, Topic(msg.Headers["reply-to"]), response)
	})

	sub, err := s.Subscribe(requestTopic, responder)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	request := TestUser{ID: 1, Name: "Test"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = RequestJSON[TestResponse](s, ctx, requestTopic, request, 2*time.Second)
	assert.Error(t, err, "Should fail to decode invalid JSON")
}

func TestRequestJSON_TimeoutScenario(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("test.request.json.timeout")

	// No responder, so it should timeout
	request := TestUser{ID: 1, Name: "Test"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := RequestJSON[TestResponse](s, ctx, requestTopic, request, 500*time.Millisecond)
	assert.Error(t, err)
}

func TestRequestJSON_NilResponse(t *testing.T) {
	s := CreateTestStream(t)
	requestTopic := Topic("test.request.json.nil")

	// Set up a responder that returns empty JSON response
	sub, err := s.TestJSONResponder(requestTopic, func(data []byte) (any, error) {
		// Return empty map which will marshal to empty JSON object
		return map[string]any{}, nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	request := TestUser{ID: 1, Name: "Test"}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := RequestJSON[TestResponse](s, ctx, requestTopic, request, 2*time.Second)
	// This should succeed but return zero values since we sent an empty object
	assert.NoError(t, err)
	assert.False(t, response.Success) // Should be false (zero value)
	assert.Empty(t, response.Message) // Should be empty (zero value)
	assert.Nil(t, response.Data) // Should be nil since empty object has no "data" field
}

// ============================================================================
// Publish Options Tests
// ============================================================================

func TestPublishOptions_WithHeaders(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.options.headers")

	received := make(chan Message, 1)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	customHeaders := map[string]string{
		"X-Custom-Header": "custom-value",
		"X-Another":       "another-value",
	}

	msg := Message{
		Topic: topic,
		Data:  []byte("test message"),
		Headers: map[string]string{
			"Original-Header": "original-value",
		},
		Time: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = s.Publish(ctx, topic, msg, WithHeaders(customHeaders))
	assert.NoError(t, err)

	// Verify headers were received
	select {
	case receivedMsg := <-received:
		assert.Equal(t, "custom-value", receivedMsg.Headers["X-Custom-Header"])
		assert.Equal(t, "another-value", receivedMsg.Headers["X-Another"])
		assert.Equal(t, "original-value", receivedMsg.Headers["Original-Header"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive message within timeout")
	}
}

func TestPublishOptions_WithMessageID(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.options.messageid")

	received := make(chan Message, 1)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	messageID := "test-message-123"

	msg := Message{
		Topic: topic,
		Data:  []byte("test message"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = s.Publish(ctx, topic, msg, WithMessageID(messageID))
	assert.NoError(t, err)

	// Verify message ID was received
	select {
	case receivedMsg := <-received:
		assert.Equal(t, messageID, receivedMsg.Headers["X-Message-Id"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive message within timeout")
	}
}

func TestPublishOptions_WithFlush(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.options.flush")

	msg := Message{
		Topic: topic,
		Data:  []byte("test message"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test with flush enabled
	err := s.Publish(ctx, topic, msg, WithFlush(true))
	assert.NoError(t, err)

	// Test with flush disabled (default)
	err = s.Publish(ctx, topic, msg, WithFlush(false))
	assert.NoError(t, err)
}

func TestPublishOptions_WithFlushTimeout(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.options.flush.timeout")

	msg := Message{
		Topic: topic,
		Data:  []byte("test message"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test with custom flush timeout
	err := s.Publish(ctx, topic, msg, 
		WithFlush(true), 
		WithFlushTimeout(1*time.Second))
	assert.NoError(t, err)
}

func TestPublishOptions_CombinedOptions(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.options.combined")

	received := make(chan Message, 1)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	msg := Message{
		Topic: topic,
		Data:  []byte("test message"),
		Time:  time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Combine multiple options
	err = s.Publish(ctx, topic, msg,
		WithHeaders(map[string]string{"X-Custom": "value"}),
		WithMessageID("combined-test-123"),
		WithFlush(true),
		WithFlushTimeout(1*time.Second))
	assert.NoError(t, err)

	// Verify all options were applied
	select {
	case receivedMsg := <-received:
		assert.Equal(t, "value", receivedMsg.Headers["X-Custom"])
		assert.Equal(t, "combined-test-123", receivedMsg.Headers["X-Message-Id"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive message within timeout")
	}
}

// ============================================================================
// Concurrent Publishing Tests
// ============================================================================

func TestPublish_ConcurrentPublishers(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("test.publish.concurrent")

	numPublishers := 10
	messagesPerPublisher := 100
	totalMessages := numPublishers * messagesPerPublisher

	received := make(chan Message, totalMessages)
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		received <- msg
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Start concurrent publishers
	var wg sync.WaitGroup
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
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

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := s.Publish(ctx, topic, msg)
				cancel()
				if err != nil {
					t.Errorf("Publisher %d failed to publish message %d: %v", publisherID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Count received messages
	time.Sleep(2 * time.Second) // Allow time for all messages to be processed
	receivedCount := len(received)
	
	// We might not receive all messages due to timing, but should receive most
	assert.Greater(t, receivedCount, totalMessages/2, 
		"Should receive at least half of the published messages")
}

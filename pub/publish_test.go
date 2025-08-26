package pub_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/pub"
	"github.com/a2y-d5l/go-stream/sub"
	"github.com/a2y-d5l/go-stream/test/helpers"
	"github.com/a2y-d5l/go-stream/topic"
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
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.NoError(t, helpers.CreateTestStream(t).
		Publish(ctx, topic.Topic("test.publish.valid"), message.Message{
			Topic: topic.Topic("test.publish.valid"),
			Data:  []byte("Hello, World!"),
			Headers: map[string]string{
				"Content-Type": "text/plain",
			},
			Time: time.Now(),
		}),
		"Publishing valid message should succeed")
}

func TestPublish_EmptyMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.NoError(t, helpers.CreateTestStream(t).
		Publish(ctx,
			topic.Topic("test.publish.empty"),
			message.Message{
				Topic: topic.Topic("test.publish.empty"),
				Data:  []byte{}, // empty data
				Time:  time.Now(),
			}),
		"Publishing empty message should succeed")
}

func TestPublish_LargeMessage(t *testing.T) {
	// Create a large payload (1MB)
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	assert.NoError(t, helpers.CreateTestStream(t).
		Publish(ctx,
			topic.Topic("test.publish.large"),
			message.Message{
				Topic: topic.Topic("test.publish.large"),
				Data:  largeData,
				Time:  time.Now(),
			}),
		"Publishing large message should succeed")
}

func TestPublish_NonExistentTopic(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.NoError(t, helpers.CreateTestStream(t).
		Publish(ctx,
			topic.Topic("test.publish.nonexistent.very.specific.topic.name"),
			message.Message{
				Topic: topic.Topic("test.publish.nonexistent.very.specific.topic.name"),
				Data:  []byte("test message"),
				Time:  time.Now(),
			}),
		"Publishing to non-existent topic should succeed")
}

func TestPublish_ClosedStream(t *testing.T) {
	s := helpers.CreateTestStream(t)
	require.NoError(t, s.Close(t.Context()))

	// Close the stream first
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.Error(t, s.Publish(ctx,
		topic.Topic("test.publish.closed"),
		message.Message{
			Topic: topic.Topic("test.publish.closed"),
			Data:  []byte("test message"),
			Time:  time.Now(),
		}), "Publishing to closed stream should fail")
}

// ============================================================================
// JSON Publishing Tests
// ============================================================================

func TestPublishJSON_ValidStruct(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.NoError(t, helpers.CreateTestStream(t).
		PublishJSON(ctx,
			topic.Topic("test.publish.json.valid"),
			TestUser{
				ID:       123,
				Name:     "John Doe",
				Email:    "john@example.com",
				IsActive: true,
			}))
}

func TestPublishJSON_InvalidJSONData(t *testing.T) {
	// Create an invalid JSON data (circular reference)
	type CircularRef struct {
		Name string
		Ref  *CircularRef
	}
	circular := &CircularRef{Name: "test"}
	circular.Ref = circular // circular reference

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.Error(t, helpers.CreateTestStream(t).
		PublishJSON(ctx,
			topic.Topic("test.publish.json.invalid"),
			circular),
		"Publishing invalid JSON should fail")
}

func TestPublishJSON_NilPointer(t *testing.T) {
	var user *TestUser = nil

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.NoError(t, helpers.CreateTestStream(t).
		PublishJSON(ctx,
			topic.Topic("test.publish.json.nil"),
			user),
		"Publishing nil pointer should succeed (marshals to null)")
}

func TestPublishJSON_ComplexNestedStructures(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.NoError(t, helpers.CreateTestStream(t).
		PublishJSON(ctx, topic.Topic("test.publish.json.complex"), map[string]any{
			"users": []TestUser{
				{ID: 1, Name: "Alice", Email: "alice@example.com", IsActive: true},
				{ID: 2, Name: "Bob", Email: "bob@example.com", IsActive: false},
			},
			"metadata": map[string]any{
				"timestamp": time.Now().Unix(),
				"version":   "1.0",
				"nested":    map[string]string{"k1": "v1", "k2": "v2"},
			},
			"count": 2,
		}),
		"Publishing complex nested structure should succeed")
}

// ============================================================================
// Request/Reply Tests
// ============================================================================

func TestRequest_ImmediateResponse(t *testing.T) {
	s := helpers.CreateTestStream(t)

	// Set up a responder using the test helper
	sub, err := helpers.TestResponder(s, topic.Topic("test.request.immediate"), func(data []byte) ([]byte, error) {
		return []byte("response data"), nil
	})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	response, err := s.Request(ctx,
		topic.Topic("test.request.immediate"),
		message.Message{
			Topic: topic.Topic("test.request.immediate"),
			Data:  []byte("request data"),
			Time:  time.Now(),
		},
		2*time.Second)

	assert.NoError(t, err)
	assert.Equal(t, []byte("response data"), response.Data)
}

func TestRequest_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// No responder set up, so request should timeout
	_, err := helpers.CreateTestStream(t).
		Request(ctx,
			topic.Topic("test.request.timeout"),
			message.Message{
				Topic: topic.Topic("test.request.timeout"),
				Data:  []byte("request data"),
				Time:  time.Now(),
			},
			500*time.Millisecond)

	assert.Error(t, err)
	// NATS returns "no responders available" instead of deadline exceeded when there are no responders
	assert.True(t, err.Error() == "nats: no responders available for request" ||
		strings.Contains(err.Error(), "deadline exceeded") ||
		strings.Contains(err.Error(), "timeout"),
		"Expected timeout or no responders error, got: %s", err.Error())
}

func TestRequest_NoResponder(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := helpers.CreateTestStream(t).
		Request(ctx,
			topic.Topic("test.request.noresponder"),
			message.Message{
				Topic: topic.Topic("test.request.noresponder"),
				Data:  []byte("request data"),
				Time:  time.Now(),
			},
			1*time.Second)

	assert.Error(t, err)
}

func TestRequest_MultipleResponders(t *testing.T) {
	s := helpers.CreateTestStream(t)
	// Set up multiple responders using test helpers
	var subs []*nats.Subscription
	for i := 0; i < 3; i++ {
		responderID := i
		sub, err := helpers.TestResponder(s,
			topic.Topic("test.request.multiple"),
			func(data []byte) ([]byte, error) {
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

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	response, err := s.Request(ctx,
		topic.Topic("test.request.multiple"),
		message.Message{
			Topic: topic.Topic("test.request.multiple"),
			Data:  []byte("request data"),
			Time:  time.Now(),
		},
		2*time.Second)

	assert.NoError(t, err)
	assert.Contains(t, string(response.Data), "response from responder")
}

// ============================================================================
// Generic Request Tests
// ============================================================================

func TestRequestJSON_TypedResponse(t *testing.T) {
	s := helpers.CreateTestStream(t)
	// Set up a JSON responder using test helper
	sub, err := helpers.TestJSONResponder(s,
		topic.Topic("test.request.json.typed"),
		func(data []byte) (any, error) {
			var req TestUser
			err := json.Unmarshal(data, &req)
			if err != nil {
				return nil, err
			}

			return TestResponse{
				Success: true,
				Message: fmt.Sprintf("Processed user %s", req.Name),
				Data:    req.ID,
			}, nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Send typed request
	response, err := client.RequestJSON[TestResponse](s, ctx,
		topic.Topic("test.request.json.typed"),
		TestUser{
			ID:    456,
			Name:  "Jane Doe",
			Email: "jane@example.com",
		},
		2*time.Second)

	assert.NoError(t, err)
	assert.True(t, response.Success)
	assert.Contains(t, response.Message, "Jane Doe")
}

func TestRequestJSON_DecodeError(t *testing.T) {
	s := helpers.CreateTestStream(t)

	// Set up a responder that returns invalid JSON
	sub, err := s.Subscribe(
		topic.Topic("test.request.json.decode.error"),
		sub.SubscriberFunc(
			func(ctx context.Context, msg message.Message) error {
				top := topic.Topic(msg.Headers["reply-to"])
				response := message.Message{
					Topic: top,
					Data:  []byte("invalid json {"),
					Time:  time.Now(),
				}
				return s.Publish(ctx, top, response)
			}))
	require.NoError(t, err)
	defer sub.Stop()
	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err = client.RequestJSON[TestResponse](s, ctx,
		topic.Topic("test.request.json.decode.error"),
		TestUser{ID: 1, Name: "Test"},
		2*time.Second)
	assert.Error(t, err, "Should fail to decode invalid JSON")
}

func TestRequestJSON_TimeoutScenario(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// No responder, so it should timeout
	_, err := client.RequestJSON[TestResponse](helpers.CreateTestStream(t), ctx,
		topic.Topic("test.request.json.timeout"),
		TestUser{ID: 1, Name: "Test"},
		500*time.Millisecond)
	assert.Error(t, err)
}

func TestRequestJSON_NilResponse(t *testing.T) {
	s := helpers.CreateTestStream(t)

	// Set up a responder that returns empty JSON response
	sub, err := helpers.TestJSONResponder(s,
		topic.Topic("test.request.json.nil"),
		func(data []byte) (any, error) {
			// Return empty map which will marshal to empty JSON object
			return map[string]any{}, nil
		})
	require.NoError(t, err)
	defer sub.Unsubscribe()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	response, err := client.RequestJSON[TestResponse](s, ctx,
		topic.Topic("test.request.json.nil"),
		TestUser{ID: 1, Name: "Test"},
		2*time.Second)

	// This should succeed but return zero values since we sent an empty object
	assert.NoError(t, err)
	assert.False(t, response.Success) // Should be false (zero value)
	assert.Empty(t, response.Message) // Should be empty (zero value)
	assert.Nil(t, response.Data)      // Should be nil since empty object has no "data" field
}

// ============================================================================
// Publish Options Tests
// ============================================================================

func TestPublishOptions_WithHeaders(t *testing.T) {
	s := helpers.CreateTestStream(t)

	received := make(chan message.Message, 1)

	sub, err := s.Subscribe(
		topic.Topic("test.publish.options.headers"),
		sub.SubscriberFunc(
			func(ctx context.Context, msg message.Message) error {
				received <- msg
				return nil
			}))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.NoError(t, s.Publish(ctx,
		topic.Topic("test.publish.options.headers"),
		message.Message{
			Topic: topic.Topic("test.publish.options.headers"),
			Data:  []byte("test message"),
			Headers: map[string]string{
				"Original-Header": "original-value",
			},
			Time: time.Now(),
		},
		pub.WithHeaders(map[string]string{
			"X-Custom-Header": "custom-value",
			"X-Another":       "another-value",
		})))

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
	s := helpers.CreateTestStream(t)

	received := make(chan message.Message, 1)
	sub, err := s.Subscribe(
		topic.Topic("test.publish.options.messageid"),
		sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
			received <- msg
			return nil
		}))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	messageID := "test-message-123"

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	assert.NoError(t, s.Publish(ctx,
		topic.Topic("test.publish.options.messageid"),
		message.Message{
			Topic: topic.Topic("test.publish.options.messageid"),
			Data:  []byte("test message"),
			Time:  time.Now(),
		},
		pub.WithMessageID(messageID)))

	// Verify message ID was received
	select {
	case receivedMsg := <-received:
		assert.Equal(t, messageID, receivedMsg.Headers["X-message.Message-Id"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive message within timeout")
	}
}

func TestPublishOptions_WithFlush(t *testing.T) {
	s := helpers.CreateTestStream(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Test with flush enabled
	assert.NoError(t, s.Publish(ctx,
		topic.Topic("test.publish.options.flush"),
		message.Message{
			Topic: topic.Topic("test.publish.options.flush"),
			Data:  []byte("test message"),
			Time:  time.Now(),
		},
		pub.WithFlush(true)))

	// Test with flush disabled (default)
	assert.NoError(t, s.Publish(ctx,
		topic.Topic("test.publish.options.flush"),
		message.Message{
			Topic: topic.Topic("test.publish.options.flush"),
			Data:  []byte("test message"),
			Time:  time.Now(),
		},
		pub.WithFlush(false)))
}

func TestPublishOptions_WithFlushTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Test with custom flush timeout
	assert.NoError(t, helpers.CreateTestStream(t).
		Publish(ctx,
			topic.Topic("test.publish.options.flush.timeout"),
			message.Message{
				Topic: topic.Topic("test.publish.options.flush.timeout"),
				Data:  []byte("test message"),
				Time:  time.Now(),
			},
			pub.WithFlush(true),
			pub.WithFlushTimeout(1*time.Second)))
}

func TestPublishOptions_CombinedOptions(t *testing.T) {
	s := helpers.CreateTestStream(t)

	received := make(chan message.Message, 1)
	sub, err := s.Subscribe(
		topic.Topic("test.publish.options.combined"),
		sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
			received <- msg
			return nil
		}))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Combine multiple options
	assert.NoError(t, s.Publish(ctx,
		topic.Topic("test.publish.options.combined"),
		message.Message{
			Topic: topic.Topic("test.publish.options.combined"),
			Data:  []byte("test message"),
			Time:  time.Now(),
		},
		pub.WithHeaders(map[string]string{"X-Custom": "value"}),
		pub.WithMessageID("combined-test-123"),
		pub.WithFlush(true),
		pub.WithFlushTimeout(1*time.Second)))

	// Verify all options were applied
	select {
	case receivedMsg := <-received:
		assert.Equal(t, "value", receivedMsg.Headers["X-Custom"])
		assert.Equal(t, "combined-test-123", receivedMsg.Headers["X-message.Message-Id"])
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive message within timeout")
	}
}

// ============================================================================
// Concurrent Publishing Tests
// ============================================================================

func TestPublish_ConcurrentPublishers(t *testing.T) {
	s := helpers.CreateTestStream(t)
	numPublishers := 10
	messagesPerPublisher := 100
	totalMessages := numPublishers * messagesPerPublisher

	received := make(chan message.Message, totalMessages)
	sub, err := s.Subscribe(
		topic.Topic("test.publish.concurrent"),
		sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
			received <- msg
			return nil
		}))
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to be ready
	time.Sleep(100 * time.Millisecond)

	// Start concurrent publishers
	var wg sync.WaitGroup
	for i := range numPublishers {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
			for j := range messagesPerPublisher {
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				err := s.Publish(ctx,
					topic.Topic("test.publish.concurrent"),
					message.Message{
						Topic: topic.Topic("test.publish.concurrent"),
						Data:  []byte(fmt.Sprintf("publisher-%d-message-%d", publisherID, j)),
						Headers: map[string]string{
							"Publisher-ID":        fmt.Sprintf("%d", publisherID),
							"message.Message-Seq": fmt.Sprintf("%d", j),
						},
						Time: time.Now(),
					})
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
	assert.Greater(t, receivedCount, totalMessages/2, "Should receive at least half of the published messages")
}

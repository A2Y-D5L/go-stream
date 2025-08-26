package helpers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/topic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StreamInterface defines the minimal interface needed for test helpers.
type StreamInterface interface {
	GetNATSConnection() *nats.Conn
}

// TestResponder is a helper for testing request/reply functionality
// It sets up a proper NATS responder that can handle reply-to automatically
func TestResponder(s StreamInterface, t topic.Topic, handler func(data []byte) ([]byte, error)) (*nats.Subscription, error) {
	nc := s.GetNATSConnection()
	return nc.Subscribe(string(t), func(msg *nats.Msg) {
		if msg.Reply != "" {
			response, err := handler(msg.Data)
			if err != nil {
				// Send error response
				nc.Publish(msg.Reply, []byte("error: "+err.Error()))
				return
			}
			nc.Publish(msg.Reply, response)
		}
	})
}

// TestJSONResponder is a helper for testing JSON request/reply functionality
func TestJSONResponder(s StreamInterface, t topic.Topic, handler func(data []byte) (any, error)) (*nats.Subscription, error) {
	nc := s.GetNATSConnection()
	return nc.Subscribe(string(t), func(msg *nats.Msg) {
		if msg.Reply != "" {
			response, err := handler(msg.Data)
			if err != nil {
				// Send error response
				nc.Publish(msg.Reply, []byte("error: "+err.Error()))
				return
			}

			// Marshal response to JSON
			jsonData, err := json.Marshal(response)
			if err != nil {
				nc.Publish(msg.Reply, []byte("error: json marshal failed"))
				return
			}

			// Create response message with headers
			respMsg := &nats.Msg{
				Subject: msg.Reply,
				Data:    jsonData,
				Header:  nats.Header{},
			}
			respMsg.Header.Set("Content-Type", "application/json")
			nc.PublishMsg(respMsg)
		}
	})
}

// CreateTestStream creates a Stream instance for testing with appropriate timeouts.
func CreateTestStream(t *testing.T, opts ...client.Option) *client.Stream {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use shorter timeouts since connection-based readiness is more reliable
	defaultOpts := []client.Option{
		client.WithPort(-1), // dynamic port
		client.WithServerReadyTimeout(5 * time.Second),
		client.WithDrainTimeout(1 * time.Second), // Add proper drain timeout for tests
		client.WithDisableJetStream(),            // Disable JetStream for faster startup
	}

	allOpts := append(defaultOpts, opts...)
	s, err := client.New(ctx, allOpts...)
	require.NoError(t, err, "Failed to create test stream")

	t.Cleanup(func() {
		CleanupStream(t, s)
	})

	return s
}

// CleanupStream properly closes a test stream and handles cleanup
func CleanupStream(t *testing.T, s *client.Stream) {
	t.Helper()

	if s == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := s.Close(ctx)
	if err != nil {
		t.Logf("Warning: Error closing test stream: %v", err)
	}
}

// AssertStreamHealthy verifies that a stream is in a healthy state
func AssertStreamHealthy(t *testing.T, s *client.Stream) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := s.Healthy(ctx)
	assert.NoError(t, err, "Stream should be healthy")
}

// WithTestTimeout creates a context with a test-appropriate timeout
func WithTestTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

// GenerateTestTopic creates a unique topic name for testing
func GenerateTestTopic(prefix string) topic.Topic {
	return topic.Topic(prefix + "." + time.Now().Format("20060102150405.000000"))
}

// GenerateTestMessage creates a test message with the given topic and content
func GenerateTestMessage(top string, content string) message.Message {
	return message.Message{
		Topic:   topic.Topic(top),
		Data:    []byte(content),
		Headers: make(map[string]string),
		ID:      "",
		Time:    time.Now(),
	}
}

// GenerateTestPayload creates test data of the specified size
func GenerateTestPayload(size int) []byte {
	if size <= 0 {
		return []byte{}
	}

	data := make([]byte, size)
	for i := range size {
		data[i] = byte('A' + (i % 26))
	}
	return data
}

// GenerateRandomHeaders creates test headers
func GenerateRandomHeaders(count int) map[string]string {
	headers := make(map[string]string, count)
	for i := range count {
		headers[string(rune('A'+i))] = string(rune('a' + i))
	}
	return headers
}

// AssertMessageEqual compares two messages for equality
func AssertMessageEqual(t *testing.T, expected, actual message.Message) {
	t.Helper()

	assert.Equal(t, expected.Topic, actual.Topic, "message.Message topics should match")
	assert.Equal(t, expected.Data, actual.Data, "message.Message data should match")
	assert.Equal(t, expected.Headers, actual.Headers, "message.Message headers should match")
	// Note: ID and Time may be auto-generated, so we don't compare them unless explicitly set
	if expected.ID != "" {
		assert.Equal(t, expected.ID, actual.ID, "message.Message IDs should match")
	}
}

// WaitForCondition waits for a condition to become true within the timeout
func WaitForCondition(condition func() bool, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		if condition() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// Continue checking
		}
	}
}

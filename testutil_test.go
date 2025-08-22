package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test utilities and helpers for consistent testing across the package

// CreateTestStream creates a Stream instance for testing with appropriate timeouts.
func CreateTestStream(t *testing.T, opts ...Option) *Stream {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use shorter timeouts since connection-based readiness is more reliable
	defaultOpts := []Option{
		WithPort(-1), // dynamic port
		WithServerReadyTimeout(5 * time.Second),
		WithDisableJetStream(), // Disable JetStream for faster startup
	}
	
	allOpts := append(defaultOpts, opts...)
	s, err := New(ctx, allOpts...)
	require.NoError(t, err, "Failed to create test stream")
	
	t.Cleanup(func() {
		CleanupStream(t, s)
	})
	
	return s
}

// CleanupStream properly closes a test stream and handles cleanup
func CleanupStream(t *testing.T, s *Stream) {
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
func AssertStreamHealthy(t *testing.T, s *Stream) {
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
func GenerateTestTopic(prefix string) Topic {
	return Topic(prefix + "." + time.Now().Format("20060102150405.000000"))
}

// GenerateTestMessage creates a test message with the given topic and content
func GenerateTestMessage(topic string, content string) Message {
	return Message{
		Topic:   Topic(topic),
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
	for i := 0; i < size; i++ {
		data[i] = byte('A' + (i % 26))
	}
	return data
}

// GenerateRandomHeaders creates test headers
func GenerateRandomHeaders(count int) map[string]string {
	headers := make(map[string]string, count)
	for i := 0; i < count; i++ {
		headers[string(rune('A'+i))] = string(rune('a' + i))
	}
	return headers
}

// AssertMessageEqual compares two messages for equality
func AssertMessageEqual(t *testing.T, expected, actual Message) {
	t.Helper()

	assert.Equal(t, expected.Topic, actual.Topic, "Message topics should match")
	assert.Equal(t, expected.Data, actual.Data, "Message data should match")
	assert.Equal(t, expected.Headers, actual.Headers, "Message headers should match")
	// Note: ID and Time may be auto-generated, so we don't compare them unless explicitly set
	if expected.ID != "" {
		assert.Equal(t, expected.ID, actual.ID, "Message IDs should match")
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

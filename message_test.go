package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMessage_Creation(t *testing.T) {
	t.Run("basic message creation", func(t *testing.T) {
		topic := Topic("test.topic")
		data := []byte("test data")
		headers := map[string]string{
			"Content-Type": "application/json",
			"X-Request-Id": "test-123",
		}
		id := "msg-123"
		timestamp := time.Now()

		msg := Message{
			Topic:   topic,
			Data:    data,
			Headers: headers,
			ID:      id,
			Time:    timestamp,
		}

		assert.Equal(t, topic, msg.Topic)
		assert.Equal(t, data, msg.Data)
		assert.Equal(t, headers, msg.Headers)
		assert.Equal(t, id, msg.ID)
		assert.Equal(t, timestamp, msg.Time)
	})

	t.Run("message with empty data", func(t *testing.T) {
		msg := Message{
			Topic:   Topic("test.topic"),
			Data:    []byte{},
			Headers: make(map[string]string),
		}

		assert.Equal(t, Topic("test.topic"), msg.Topic)
		assert.Empty(t, msg.Data)
		assert.NotNil(t, msg.Headers)
		assert.Empty(t, msg.Headers)
	})

	t.Run("message with nil headers", func(t *testing.T) {
		msg := Message{
			Topic:   Topic("test.topic"),
			Data:    []byte("data"),
			Headers: nil,
		}

		assert.Equal(t, Topic("test.topic"), msg.Topic)
		assert.Equal(t, []byte("data"), msg.Data)
		assert.Nil(t, msg.Headers)
	})

	t.Run("message with large payload", func(t *testing.T) {
		largeData := GenerateTestPayload(1024 * 1024) // 1MB

		msg := Message{
			Topic:   Topic("test.large"),
			Data:    largeData,
			Headers: make(map[string]string),
		}

		assert.Equal(t, len(largeData), len(msg.Data))
		assert.Equal(t, largeData, msg.Data)
	})
}

func TestMessage_HeaderManagement(t *testing.T) {
	t.Run("header setting and getting", func(t *testing.T) {
		msg := Message{
			Topic:   Topic("test.topic"),
			Data:    []byte("test data"),
			Headers: make(map[string]string),
		}

		// Set headers
		msg.Headers["Content-Type"] = "application/json"
		msg.Headers["X-Request-Id"] = "test-123"
		msg.Headers["Custom-Header"] = "custom-value"

		// Verify headers
		assert.Equal(t, "application/json", msg.Headers["Content-Type"])
		assert.Equal(t, "test-123", msg.Headers["X-Request-Id"])
		assert.Equal(t, "custom-value", msg.Headers["Custom-Header"])
		assert.Len(t, msg.Headers, 3)
	})

	t.Run("header overrides", func(t *testing.T) {
		msg := Message{
			Topic: Topic("test.topic"),
			Data:  []byte("test data"),
			Headers: map[string]string{
				"Content-Type": "text/plain",
				"X-Request-Id": "original-123",
			},
		}

		// Override headers
		msg.Headers["Content-Type"] = "application/json"
		msg.Headers["X-Request-Id"] = "new-456"

		assert.Equal(t, "application/json", msg.Headers["Content-Type"])
		assert.Equal(t, "new-456", msg.Headers["X-Request-Id"])
	})

	t.Run("empty header values", func(t *testing.T) {
		msg := Message{
			Topic:   Topic("test.topic"),
			Data:    []byte("test data"),
			Headers: make(map[string]string),
		}

		msg.Headers["Empty-Header"] = ""
		msg.Headers["Normal-Header"] = "value"

		assert.Equal(t, "", msg.Headers["Empty-Header"])
		assert.Equal(t, "value", msg.Headers["Normal-Header"])
		assert.Len(t, msg.Headers, 2)
	})

	t.Run("headers with special characters", func(t *testing.T) {
		msg := Message{
			Topic:   Topic("test.topic"),
			Data:    []byte("test data"),
			Headers: make(map[string]string),
		}

		msg.Headers["Unicode-Header"] = "Hello, 世界! 🌍"
		msg.Headers["Special-Chars"] = "!@#$%^&*()_+-=[]{}|;:,.<>?"

		assert.Equal(t, "Hello, 世界! 🌍", msg.Headers["Unicode-Header"])
		assert.Equal(t, "!@#$%^&*()_+-=[]{}|;:,.<>?", msg.Headers["Special-Chars"])
	})
}

func TestMessage_Timestamps(t *testing.T) {
	t.Run("timestamp handling", func(t *testing.T) {
		before := time.Now()
		time.Sleep(1 * time.Millisecond) // Ensure different timestamp

		msg := Message{
			Topic: Topic("test.topic"),
			Data:  []byte("test data"),
			Time:  time.Now(),
		}

		time.Sleep(1 * time.Millisecond)
		after := time.Now()

		assert.True(t, msg.Time.After(before))
		assert.True(t, msg.Time.Before(after))
	})

	t.Run("zero timestamp", func(t *testing.T) {
		msg := Message{
			Topic: Topic("test.topic"),
			Data:  []byte("test data"),
			Time:  time.Time{}, // Zero time
		}

		assert.True(t, msg.Time.IsZero())
	})
}

func TestMessage_IDHandling(t *testing.T) {
	t.Run("explicit message ID", func(t *testing.T) {
		id := "explicit-id-123"
		msg := Message{
			Topic: Topic("test.topic"),
			Data:  []byte("test data"),
			ID:    id,
		}

		assert.Equal(t, id, msg.ID)
	})

	t.Run("empty message ID", func(t *testing.T) {
		msg := Message{
			Topic: Topic("test.topic"),
			Data:  []byte("test data"),
			ID:    "",
		}

		assert.Empty(t, msg.ID)
	})

	t.Run("message ID uniqueness", func(t *testing.T) {
		msg1 := Message{
			Topic: Topic("test.topic"),
			Data:  []byte("test data 1"),
			ID:    "id-1",
		}

		msg2 := Message{
			Topic: Topic("test.topic"),
			Data:  []byte("test data 2"),
			ID:    "id-2",
		}

		assert.NotEqual(t, msg1.ID, msg2.ID)
	})
}

func TestMessage_Validation(t *testing.T) {
	t.Run("valid message", func(t *testing.T) {
		msg := Message{
			Topic:   Topic("valid.topic"),
			Data:    []byte("valid data"),
			Headers: map[string]string{"Content-Type": "application/json"},
			ID:      "valid-id",
			Time:    time.Now(),
		}

		// Basic validation - all fields are set
		assert.NotEmpty(t, msg.Topic)
		assert.NotNil(t, msg.Data)
		assert.NotNil(t, msg.Headers)
		assert.NotEmpty(t, msg.ID)
		assert.False(t, msg.Time.IsZero())
	})

	t.Run("minimal valid message", func(t *testing.T) {
		msg := Message{
			Topic: Topic("minimal.topic"),
			Data:  []byte("data"),
		}

		// Minimal validation - topic and data are required
		assert.NotEmpty(t, msg.Topic)
		assert.NotNil(t, msg.Data)
	})
}

func TestMessage_BinaryData(t *testing.T) {
	t.Run("binary data handling", func(t *testing.T) {
		binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}

		msg := Message{
			Topic: Topic("binary.topic"),
			Data:  binaryData,
		}

		assert.Equal(t, binaryData, msg.Data)
		assert.Len(t, msg.Data, 6)
	})

	t.Run("large binary data", func(t *testing.T) {
		// Create 1MB of binary data
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		msg := Message{
			Topic: Topic("large.binary"),
			Data:  largeData,
		}

		assert.Equal(t, largeData, msg.Data)
		assert.Len(t, msg.Data, 1024*1024)
	})
}

func TestMessage_Equality(t *testing.T) {
	t.Run("identical messages", func(t *testing.T) {
		timestamp := time.Now()
		headers := map[string]string{"key": "value"}

		msg1 := Message{
			Topic:   Topic("test.topic"),
			Data:    []byte("test data"),
			Headers: headers,
			ID:      "id-123",
			Time:    timestamp,
		}

		msg2 := Message{
			Topic:   Topic("test.topic"),
			Data:    []byte("test data"),
			Headers: headers,
			ID:      "id-123",
			Time:    timestamp,
		}

		AssertMessageEqual(t, msg1, msg2)
	})

	t.Run("different topics", func(t *testing.T) {
		msg1 := Message{
			Topic: Topic("topic1"),
			Data:  []byte("data"),
		}

		msg2 := Message{
			Topic: Topic("topic2"),
			Data:  []byte("data"),
		}

		assert.NotEqual(t, msg1.Topic, msg2.Topic)
	})

	t.Run("different data", func(t *testing.T) {
		msg1 := Message{
			Topic: Topic("topic"),
			Data:  []byte("data1"),
		}

		msg2 := Message{
			Topic: Topic("topic"),
			Data:  []byte("data2"),
		}

		assert.NotEqual(t, msg1.Data, msg2.Data)
	})
}

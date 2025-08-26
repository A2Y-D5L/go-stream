package message

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/a2y-d5l/go-stream/topic"
)

// --------------------- Additional Test Data Types ---------------------

type MessageTestData struct {
	ID    int    `json:"id"`
	Value string `json:"value"`
}

// --------------------- Enhanced Message Serialization Tests ---------------------

func TestMessage_JSONSerialization(t *testing.T) {
	now := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("serialize complete message", func(t *testing.T) {
		msg := Message{
			Topic: "user.created",
			Data:  []byte(`{"id": 123, "name": "John"}`),
			Headers: map[string]string{
				"Content-Type": "application/json",
				"X-Request-Id": "req-123",
			},
			ID:   "msg-456",
			Time: now,
		}

		// Serialize to JSON
		data, err := json.Marshal(msg)
		require.NoError(t, err)
		assert.Contains(t, string(data), "user.created")
		assert.Contains(t, string(data), "msg-456")

		// Deserialize back
		var restored Message
		err = json.Unmarshal(data, &restored)
		require.NoError(t, err)

		assert.Equal(t, msg.Topic, restored.Topic)
		assert.Equal(t, msg.Data, restored.Data)
		assert.Equal(t, msg.Headers, restored.Headers)
		assert.Equal(t, msg.ID, restored.ID)
		assert.True(t, msg.Time.Equal(restored.Time))
	})

	t.Run("serialize message with nil headers", func(t *testing.T) {
		msg := Message{
			Topic: "test.topic",
			Data:  []byte("test data"),
			Time:  now,
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		var restored Message
		err = json.Unmarshal(data, &restored)
		require.NoError(t, err)

		assert.Equal(t, msg.Topic, restored.Topic)
		assert.Equal(t, msg.Data, restored.Data)
		assert.Nil(t, restored.Headers) // nil headers should remain nil
	})

	t.Run("serialize message with empty data", func(t *testing.T) {
		msg := Message{
			Topic: "empty.data",
			Data:  []byte{},
			Time:  now,
		}

		data, err := json.Marshal(msg)
		require.NoError(t, err)

		var restored Message
		err = json.Unmarshal(data, &restored)
		require.NoError(t, err)

		assert.Equal(t, msg.Topic, restored.Topic)
		assert.Equal(t, []byte{}, restored.Data)
	})
}

// --------------------- NATS Message Conversion Tests ---------------------

func TestMessage_NATSConversion(t *testing.T) {
	t.Run("convert from NATS message", func(t *testing.T) {
		natsMsg := &nats.Msg{
			Subject: "user.updated",
			Data:    []byte(`{"id": 456, "status": "active"}`),
			Reply:   "response.topic",
			Header: nats.Header{
				"Content-Type":  []string{"application/json"},
				"X-Request-Id":  []string{"req-789"},
				"Authorization": []string{"Bearer secret"},
			},
		}

		// Convert to our Message type
		msg := Message{
			Topic:   topic.Topic(natsMsg.Subject),
			Data:    natsMsg.Data,
			Headers: make(map[string]string),
			Time:    time.Now(),
		}

		// Convert headers
		for k, vv := range natsMsg.Header {
			if len(vv) > 0 {
				msg.Headers[k] = vv[0]
			}
		}

		assert.Equal(t, topic.Topic("user.updated"), msg.Topic)
		assert.Equal(t, natsMsg.Data, msg.Data)
		assert.Equal(t, "application/json", msg.Headers["Content-Type"])
		assert.Equal(t, "req-789", msg.Headers["X-Request-Id"])
		assert.Equal(t, "Bearer secret", msg.Headers["Authorization"])
	})

	t.Run("convert to NATS message", func(t *testing.T) {
		msg := Message{
			Topic: "order.processed",
			Data:  []byte(`{"order_id": "ORD-123", "amount": 99.99}`),
			Headers: map[string]string{
				"Content-Type": "application/json",
				"X-User-Id":    "user-456",
			},
			ID:   "msg-789",
			Time: time.Now(),
		}

		// Convert to NATS message
		natsMsg := &nats.Msg{
			Subject: string(msg.Topic),
			Data:    msg.Data,
			Header:  make(nats.Header),
		}

		// Convert headers
		for k, v := range msg.Headers {
			natsMsg.Header.Set(k, v)
		}

		assert.Equal(t, "order.processed", natsMsg.Subject)
		assert.Equal(t, msg.Data, natsMsg.Data)
		assert.Equal(t, "application/json", natsMsg.Header.Get("Content-Type"))
		assert.Equal(t, "user-456", natsMsg.Header.Get("X-User-Id"))
	})

	t.Run("convert with empty headers", func(t *testing.T) {
		msg := Message{
			Topic: "simple.message",
			Data:  []byte("simple data"),
			Time:  time.Now(),
		}

		natsMsg := &nats.Msg{
			Subject: string(msg.Topic),
			Data:    msg.Data,
			Header:  make(nats.Header),
		}

		assert.Equal(t, "simple.message", natsMsg.Subject)
		assert.Equal(t, msg.Data, natsMsg.Data)
		assert.Empty(t, natsMsg.Header)
	})
}

// --------------------- Message Header Operations Tests ---------------------

func TestMessage_HeaderOperations(t *testing.T) {
	t.Run("header case sensitivity", func(t *testing.T) {
		msg := Message{
			Topic: "test.headers",
			Data:  []byte("data"),
			Headers: map[string]string{
				"Content-Type": "application/json",
				"content-type": "text/plain", // Different case
			},
			Time: time.Now(),
		}

		// Headers should be case-sensitive in our implementation
		assert.Equal(t, "application/json", msg.Headers["Content-Type"])
		assert.Equal(t, "text/plain", msg.Headers["content-type"])
		assert.Equal(t, 2, len(msg.Headers))
	})

	t.Run("header modification", func(t *testing.T) {
		msg := Message{
			Topic:   "test.modify",
			Data:    []byte("data"),
			Headers: map[string]string{"Initial": "value"},
			Time:    time.Now(),
		}

		// Modify headers
		msg.Headers["New-Header"] = "new-value"
		msg.Headers["Initial"] = "updated-value"

		assert.Equal(t, "updated-value", msg.Headers["Initial"])
		assert.Equal(t, "new-value", msg.Headers["New-Header"])
		assert.Equal(t, 2, len(msg.Headers))
	})

	t.Run("nil headers map", func(t *testing.T) {
		msg := Message{
			Topic:   "test.nil",
			Data:    []byte("data"),
			Headers: nil,
			Time:    time.Now(),
		}

		assert.Nil(t, msg.Headers)

		// Initialize headers map
		msg.Headers = make(map[string]string)
		msg.Headers["Added"] = "after-init"

		assert.Equal(t, "after-init", msg.Headers["Added"])
		assert.Equal(t, 1, len(msg.Headers))
	})
}

// --------------------- Message Data Handling Tests ---------------------

func TestMessage_DataHandling(t *testing.T) {
	t.Run("text data", func(t *testing.T) {
		text := "Hello, World!"
		msg := Message{
			Topic: "text.message",
			Data:  []byte(text),
			Time:  time.Now(),
		}

		assert.Equal(t, text, string(msg.Data))
	})

	t.Run("json data", func(t *testing.T) {
		testData := MessageTestData{ID: 123, Value: "test"}
		jsonData, err := json.Marshal(testData)
		require.NoError(t, err)

		msg := Message{
			Topic:   "json.message",
			Data:    jsonData,
			Headers: map[string]string{"Content-Type": "application/json"},
			Time:    time.Now(),
		}

		var decoded MessageTestData
		err = json.Unmarshal(msg.Data, &decoded)
		require.NoError(t, err)
		assert.Equal(t, testData, decoded)
	})

	t.Run("binary data", func(t *testing.T) {
		binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
		msg := Message{
			Topic:   "binary.message",
			Data:    binaryData,
			Headers: map[string]string{"Content-Type": "application/octet-stream"},
			Time:    time.Now(),
		}

		assert.Equal(t, binaryData, msg.Data)
		assert.Equal(t, len(binaryData), len(msg.Data))
	})

	t.Run("large data", func(t *testing.T) {
		// Create 1MB of data
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		msg := Message{
			Topic: "large.message",
			Data:  largeData,
			Time:  time.Now(),
		}

		assert.Equal(t, 1024*1024, len(msg.Data))
		assert.Equal(t, largeData, msg.Data)
	})
}

// --------------------- Message Topic Handling Tests ---------------------

func TestMessage_TopicHandling(t *testing.T) {
	testCases := []struct {
		name      string
		topic     topic.Topic
		expectStr string
	}{
		{"simple topic", "user", "user"},
		{"dotted topic", "user.created", "user.created"},
		{"hierarchical topic", "api.v1.users.created", "api.v1.users.created"},
		{"wildcard topic", "users.*", "users.*"},
		{"empty topic", "", ""},
		{"topic with special chars", "user-events_2024", "user-events_2024"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := Message{
				Topic: tc.topic,
				Data:  []byte("test"),
				Time:  time.Now(),
			}

			assert.Equal(t, tc.topic, msg.Topic)
			assert.Equal(t, tc.expectStr, string(msg.Topic))
		})
	}
}

// --------------------- Message Time Handling Tests ---------------------

func TestMessage_TimeHandling(t *testing.T) {
	t.Run("current time", func(t *testing.T) {
		before := time.Now()
		msg := Message{
			Topic: "time.test",
			Data:  []byte("data"),
			Time:  time.Now(),
		}
		after := time.Now()

		assert.True(t, msg.Time.After(before) || msg.Time.Equal(before))
		assert.True(t, msg.Time.Before(after) || msg.Time.Equal(after))
	})

	t.Run("specific time", func(t *testing.T) {
		specificTime := time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC)
		msg := Message{
			Topic: "scheduled.message",
			Data:  []byte("scheduled data"),
			Time:  specificTime,
		}

		assert.Equal(t, specificTime, msg.Time)
		assert.Equal(t, 2024, msg.Time.Year())
		assert.Equal(t, time.June, msg.Time.Month())
		assert.Equal(t, 15, msg.Time.Day())
	})

	t.Run("zero time", func(t *testing.T) {
		msg := Message{
			Topic: "zero.time",
			Data:  []byte("data"),
			Time:  time.Time{},
		}

		assert.True(t, msg.Time.IsZero())
	})

	t.Run("time zone handling", func(t *testing.T) {
		utc := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		est := utc.In(time.FixedZone("EST", -5*3600))

		msgUTC := Message{Topic: "utc", Data: []byte("data"), Time: utc}
		msgEST := Message{Topic: "est", Data: []byte("data"), Time: est}

		assert.True(t, msgUTC.Time.Equal(msgEST.Time))                 // Same instant
		assert.NotEqual(t, msgUTC.Time.String(), msgEST.Time.String()) // Different string representation
	})
}

// --------------------- Message Performance Benchmarks ---------------------

func BenchmarkMessage_Creation(b *testing.B) {
	headers := map[string]string{
		"Content-Type": "application/json",
		"X-Request-Id": "req-123",
	}
	data := []byte(`{"id": 123, "name": "test"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Message{
			Topic:   "benchmark.topic",
			Data:    data,
			Headers: headers,
			ID:      "msg-456",
			Time:    time.Now(),
		}
	}
}

func BenchmarkMessage_JSONMarshal(b *testing.B) {
	msg := Message{
		Topic: "benchmark.json",
		Data:  []byte(`{"id": 123, "value": "test data"}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
			"X-Request-Id": "req-456",
		},
		ID:   "msg-789",
		Time: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessage_JSONUnmarshal(b *testing.B) {
	msg := Message{
		Topic: "benchmark.json",
		Data:  []byte(`{"id": 123, "value": "test data"}`),
		Headers: map[string]string{
			"Content-Type": "application/json",
			"X-Request-Id": "req-456",
		},
		ID:   "msg-789",
		Time: time.Now(),
	}
	data, _ := json.Marshal(msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var restored Message
		err := json.Unmarshal(data, &restored)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessage_HeaderOperations(b *testing.B) {
	msg := Message{
		Topic:   "benchmark.headers",
		Data:    []byte("data"),
		Headers: make(map[string]string),
		Time:    time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.Headers["Key"] = "Value"
		_ = msg.Headers["Key"]
		delete(msg.Headers, "Key")
	}
}

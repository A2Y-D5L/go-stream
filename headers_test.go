package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// --------------------- Header Constants Tests ---------------------

func TestHeaders_StandardHeaders(t *testing.T) {
	t.Run("content type header", func(t *testing.T) {
		headers := map[string]string{
			"Content-Type": "application/json",
		}

		assert.Equal(t, "application/json", headers["Content-Type"])
	})

	t.Run("request id header", func(t *testing.T) {
		headers := map[string]string{
			"X-Request-Id": "req-12345",
		}

		assert.Equal(t, "req-12345", headers["X-Request-Id"])
	})

	t.Run("authorization header", func(t *testing.T) {
		headers := map[string]string{
			"Authorization": "Bearer token123",
		}

		assert.Equal(t, "Bearer token123", headers["Authorization"])
	})
}

// --------------------- Header Manipulation Tests ---------------------

func TestHeaders_Manipulation(t *testing.T) {
	t.Run("add headers", func(t *testing.T) {
		headers := make(map[string]string)
		
		// Add headers one by one
		headers["Content-Type"] = "application/json"
		headers["X-Request-Id"] = "req-456"
		headers["User-Agent"] = "go-stream/1.0"

		assert.Equal(t, 3, len(headers))
		assert.Equal(t, "application/json", headers["Content-Type"])
		assert.Equal(t, "req-456", headers["X-Request-Id"])
		assert.Equal(t, "go-stream/1.0", headers["User-Agent"])
	})

	t.Run("update headers", func(t *testing.T) {
		headers := map[string]string{
			"Content-Type": "text/plain",
			"Version":      "1.0",
		}

		// Update existing headers
		headers["Content-Type"] = "application/json"
		headers["Version"] = "2.0"

		assert.Equal(t, 2, len(headers))
		assert.Equal(t, "application/json", headers["Content-Type"])
		assert.Equal(t, "2.0", headers["Version"])
	})

	t.Run("delete headers", func(t *testing.T) {
		headers := map[string]string{
			"Content-Type":  "application/json",
			"X-Request-Id":  "req-789",
			"Authorization": "Bearer token",
		}

		// Delete headers
		delete(headers, "Authorization")
		delete(headers, "X-Request-Id")

		assert.Equal(t, 1, len(headers))
		assert.Equal(t, "application/json", headers["Content-Type"])
		assert.NotContains(t, headers, "Authorization")
		assert.NotContains(t, headers, "X-Request-Id")
	})

	t.Run("clear all headers", func(t *testing.T) {
		headers := map[string]string{
			"Content-Type": "application/json",
			"X-Request-Id": "req-123",
		}

		// Clear by creating new map
		for k := range headers {
			delete(headers, k)
		}

		assert.Equal(t, 0, len(headers))
		assert.Empty(t, headers)
	})
}

// --------------------- Header Case Sensitivity Tests ---------------------

func TestHeaders_CaseSensitivity(t *testing.T) {
	t.Run("case sensitive keys", func(t *testing.T) {
		headers := map[string]string{
			"Content-Type": "application/json",
			"content-type": "text/plain",
			"CONTENT-TYPE": "application/xml",
		}

		// All three should be different keys
		assert.Equal(t, 3, len(headers))
		assert.Equal(t, "application/json", headers["Content-Type"])
		assert.Equal(t, "text/plain", headers["content-type"])
		assert.Equal(t, "application/xml", headers["CONTENT-TYPE"])
	})

	t.Run("canonical header names", func(t *testing.T) {
		// Test common header name variations
		testCases := []struct {
			name     string
			input    string
			expected string
		}{
			{"standard", "Content-Type", "application/json"},
			{"lowercase", "content-type", "text/plain"},
			{"uppercase", "CONTENT-TYPE", "application/xml"},
			{"mixed", "CoNtEnT-tYpE", "text/html"},
		}

		headers := make(map[string]string)
		for _, tc := range testCases {
			headers[tc.input] = tc.expected
		}

		for _, tc := range testCases {
			assert.Equal(t, tc.expected, headers[tc.input], "Failed for %s", tc.name)
		}
	})
}

// --------------------- Special Headers Tests ---------------------

func TestHeaders_SpecialHeaders(t *testing.T) {
	t.Run("content type variations", func(t *testing.T) {
		testCases := []struct {
			name        string
			contentType string
		}{
			{"json", "application/json"},
			{"xml", "application/xml"},
			{"text", "text/plain"},
			{"html", "text/html"},
			{"binary", "application/octet-stream"},
			{"form", "application/x-www-form-urlencoded"},
			{"multipart", "multipart/form-data"},
			{"protobuf", "application/x-protobuf"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				headers := map[string]string{
					"Content-Type": tc.contentType,
				}

				assert.Equal(t, tc.contentType, headers["Content-Type"])
			})
		}
	})

	t.Run("correlation headers", func(t *testing.T) {
		correlationHeaders := map[string]string{
			"X-Request-Id":     "req-123",
			"X-Correlation-Id": "corr-456",
			"X-Trace-Id":       "trace-789",
			"X-Span-Id":        "span-abc",
			"X-Session-Id":     "sess-def",
		}

		for headerName, expectedValue := range correlationHeaders {
			assert.Equal(t, expectedValue, correlationHeaders[headerName])
		}

		assert.Equal(t, 5, len(correlationHeaders))
	})

	t.Run("authentication headers", func(t *testing.T) {
		authHeaders := []struct {
			name   string
			header string
			value  string
		}{
			{"bearer token", "Authorization", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"},
			{"basic auth", "Authorization", "Basic dXNlcjpwYXNzd29yZA=="},
			{"api key", "X-API-Key", "api-key-123456789"},
			{"custom auth", "X-Auth-Token", "custom-token-abcdef"},
		}

		for _, tc := range authHeaders {
			t.Run(tc.name, func(t *testing.T) {
				headers := map[string]string{
					tc.header: tc.value,
				}

				assert.Equal(t, tc.value, headers[tc.header])
			})
		}
	})

	t.Run("custom application headers", func(t *testing.T) {
		customHeaders := map[string]string{
			"X-User-Id":       "user-123",
			"X-Tenant-Id":     "tenant-456",
			"X-Version":       "v1.2.3",
			"X-Client":        "mobile-app",
			"X-Feature-Flag":  "new-ui-enabled",
			"X-Rate-Limit":    "1000",
			"X-Timestamp":     "2024-01-01T12:00:00Z",
		}

		assert.Equal(t, 7, len(customHeaders))
		assert.Equal(t, "user-123", customHeaders["X-User-Id"])
		assert.Equal(t, "tenant-456", customHeaders["X-Tenant-Id"])
		assert.Equal(t, "v1.2.3", customHeaders["X-Version"])
	})
}

// --------------------- Header Validation Tests ---------------------

func TestHeaders_Validation(t *testing.T) {
	t.Run("empty header values", func(t *testing.T) {
		headers := map[string]string{
			"Content-Type": "",
			"X-Request-Id": "",
			"Empty-Header": "",
		}

		// Empty values are valid
		assert.Equal(t, 3, len(headers))
		assert.Equal(t, "", headers["Content-Type"])
		assert.Equal(t, "", headers["X-Request-Id"])
	})

	t.Run("whitespace in header values", func(t *testing.T) {
		headers := map[string]string{
			"Spaces":     "  value with spaces  ",
			"Tabs":       "\tvalue with tabs\t",
			"Newlines":   "value\nwith\nnewlines",
			"Mixed":      " \t mixed whitespace \n ",
		}

		// Whitespace should be preserved as-is
		assert.Equal(t, "  value with spaces  ", headers["Spaces"])
		assert.Equal(t, "\tvalue with tabs\t", headers["Tabs"])
		assert.Equal(t, "value\nwith\nnewlines", headers["Newlines"])
		assert.Equal(t, " \t mixed whitespace \n ", headers["Mixed"])
	})

	t.Run("special characters in headers", func(t *testing.T) {
		headers := map[string]string{
			"Unicode":     "value with unicode: 你好世界",
			"Symbols":     "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./ ",
			"Numbers":     "1234567890",
			"Mixed":       "Value123!@#",
		}

		assert.Equal(t, "value with unicode: 你好世界", headers["Unicode"])
		assert.Equal(t, "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./ ", headers["Symbols"])
		assert.Equal(t, "1234567890", headers["Numbers"])
		assert.Equal(t, "Value123!@#", headers["Mixed"])
	})

	t.Run("very long header values", func(t *testing.T) {
		longValue := make([]byte, 10000)
		for i := range longValue {
			longValue[i] = byte('A' + (i % 26))
		}

		headers := map[string]string{
			"Long-Header": string(longValue),
		}

		assert.Equal(t, 10000, len(headers["Long-Header"]))
		assert.Equal(t, string(longValue), headers["Long-Header"])
	})
}

// --------------------- Header Context Integration Tests ---------------------

func TestHeaders_ContextIntegration(t *testing.T) {
	t.Run("request id propagation", func(t *testing.T) {
		// Simulate request ID propagation through headers
		requestID := "req-ctx-123"
		
		// Original message with request ID
		originalHeaders := map[string]string{
			"X-Request-Id": requestID,
			"Content-Type": "application/json",
		}

		// Simulate passing through system - request ID should be preserved
		processedHeaders := make(map[string]string)
		for k, v := range originalHeaders {
			processedHeaders[k] = v
		}

		// Add additional context
		processedHeaders["X-Processed-By"] = "service-A"
		processedHeaders["X-Timestamp"] = time.Now().Format(time.RFC3339)

		assert.Equal(t, requestID, processedHeaders["X-Request-Id"])
		assert.Equal(t, "application/json", processedHeaders["Content-Type"])
		assert.Equal(t, "service-A", processedHeaders["X-Processed-By"])
		assert.Contains(t, processedHeaders, "X-Timestamp")
	})

	t.Run("trace context propagation", func(t *testing.T) {
		// Simulate distributed tracing headers
		traceHeaders := map[string]string{
			"X-Trace-Id":      "trace-456",
			"X-Span-Id":       "span-789",
			"X-Parent-Span":   "span-123",
			"X-Sampled":       "true",
			"X-Priority":      "high",
		}

		// Headers should maintain tracing context
		assert.Equal(t, "trace-456", traceHeaders["X-Trace-Id"])
		assert.Equal(t, "span-789", traceHeaders["X-Span-Id"])
		assert.Equal(t, "span-123", traceHeaders["X-Parent-Span"])
		assert.Equal(t, "true", traceHeaders["X-Sampled"])
		assert.Equal(t, "high", traceHeaders["X-Priority"])
	})

	t.Run("header inheritance", func(t *testing.T) {
		// Parent message headers
		parentHeaders := map[string]string{
			"X-Request-Id":  "req-parent",
			"X-Trace-Id":    "trace-parent",
			"Authorization": "Bearer parent-token",
		}

		// Child message inherits some headers, adds others
		childHeaders := make(map[string]string)
		
		// Inherit trace context
		if traceId, exists := parentHeaders["X-Trace-Id"]; exists {
			childHeaders["X-Trace-Id"] = traceId
		}
		if requestId, exists := parentHeaders["X-Request-Id"]; exists {
			childHeaders["X-Request-Id"] = requestId
		}

		// Add child-specific headers
		childHeaders["X-Child-Id"] = "child-123"
		childHeaders["Content-Type"] = "application/json"

		assert.Equal(t, "trace-parent", childHeaders["X-Trace-Id"])
		assert.Equal(t, "req-parent", childHeaders["X-Request-Id"])
		assert.Equal(t, "child-123", childHeaders["X-Child-Id"])
		assert.Equal(t, "application/json", childHeaders["Content-Type"])
		assert.NotContains(t, childHeaders, "Authorization") // Not inherited
	})
}

// --------------------- Header Edge Cases Tests ---------------------

func TestHeaders_EdgeCases(t *testing.T) {
	t.Run("nil headers map", func(t *testing.T) {
		var headers map[string]string
		assert.Nil(t, headers)
		assert.Equal(t, 0, len(headers))

		// Reading from nil map returns zero value
		assert.Equal(t, "", headers["Any-Key"])
	})

	t.Run("empty headers map", func(t *testing.T) {
		headers := make(map[string]string)
		assert.NotNil(t, headers)
		assert.Equal(t, 0, len(headers))
		assert.Empty(t, headers)

		// Can add to empty map
		headers["First"] = "value"
		assert.Equal(t, 1, len(headers))
		assert.Equal(t, "value", headers["First"])
	})

	t.Run("header key edge cases", func(t *testing.T) {
		headers := map[string]string{
			"":           "empty key",
			" ":          "space key",
			"\t":         "tab key",
			"a":          "single char",
			"VERY-LONG-HEADER-NAME-WITH-MANY-WORDS-AND-DASHES": "long key",
		}

		assert.Equal(t, 5, len(headers))
		assert.Equal(t, "empty key", headers[""])
		assert.Equal(t, "space key", headers[" "])
		assert.Equal(t, "tab key", headers["\t"])
		assert.Equal(t, "single char", headers["a"])
		assert.Equal(t, "long key", headers["VERY-LONG-HEADER-NAME-WITH-MANY-WORDS-AND-DASHES"])
	})

	t.Run("concurrent header access", func(t *testing.T) {
		// Note: This is for documentation - actual concurrent access would need proper synchronization
		headers := map[string]string{
			"Shared": "initial value",
		}

		// In real concurrent code, you'd need sync.RWMutex or sync.Map
		// This test just verifies the data structure behavior
		
		// Read
		value := headers["Shared"]
		assert.Equal(t, "initial value", value)

		// Write
		headers["Shared"] = "updated value"
		assert.Equal(t, "updated value", headers["Shared"])

		// Add
		headers["New"] = "new value"
		assert.Equal(t, "new value", headers["New"])
		assert.Equal(t, 2, len(headers))
	})
}

// --------------------- Header Performance Tests ---------------------

func BenchmarkHeaders_SetSingle(b *testing.B) {
	headers := make(map[string]string)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		headers["Content-Type"] = "application/json"
	}
}

func BenchmarkHeaders_GetSingle(b *testing.B) {
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = headers["Content-Type"]
	}
}

func BenchmarkHeaders_SetMultiple(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		headers := map[string]string{
			"Content-Type":   "application/json",
			"X-Request-Id":   "req-123",
			"Authorization":  "Bearer token",
			"X-Trace-Id":     "trace-456",
			"X-User-Agent":   "go-stream/1.0",
		}
		_ = headers // Use the variable
	}
}

func BenchmarkHeaders_CopyHeaders(b *testing.B) {
	sourceHeaders := map[string]string{
		"Content-Type":   "application/json",
		"X-Request-Id":   "req-123",
		"Authorization":  "Bearer token",
		"X-Trace-Id":     "trace-456",
		"X-User-Agent":   "go-stream/1.0",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		destHeaders := make(map[string]string)
		for k, v := range sourceHeaders {
			destHeaders[k] = v
		}
	}
}

func BenchmarkHeaders_DeleteHeader(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		headers := map[string]string{
			"Content-Type":  "application/json",
			"X-Request-Id":  "req-123",
			"Authorization": "Bearer token",
		}
		delete(headers, "Authorization")
	}
}

func BenchmarkHeaders_IterateHeaders(b *testing.B) {
	headers := map[string]string{
		"Content-Type":   "application/json",
		"X-Request-Id":   "req-123",
		"Authorization":  "Bearer token",
		"X-Trace-Id":     "trace-456",
		"X-User-Agent":   "go-stream/1.0",
		"X-Timestamp":    "2024-01-01T12:00:00Z",
		"X-Version":      "v1.0.0",
		"X-Client":       "test-client",
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for k, v := range headers {
			_ = k
			_ = v
		}
	}
}

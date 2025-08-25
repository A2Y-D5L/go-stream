package stream

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Context key types for safe context values
type contextKey string

const (
	requestIDKey contextKey = "request-id"
	traceIDKey   contextKey = "trace-id"
	userIDKey    contextKey = "user-id"
	tenantIDKey  contextKey = "tenant-id"
)

// --------------------- Context Value Tests ---------------------

func TestContext_RequestIDPropagation(t *testing.T) {
	t.Run("request ID in context", func(t *testing.T) {
		requestID := "req-ctx-123"
		ctx := context.WithValue(context.Background(), requestIDKey, requestID)

		// Retrieve request ID from context
		retrievedID := ctx.Value(requestIDKey)
		assert.Equal(t, requestID, retrievedID)

		// Type assertion
		if id, ok := retrievedID.(string); ok {
			assert.Equal(t, requestID, id)
		} else {
			t.Fatal("Request ID is not a string")
		}
	})

	t.Run("request ID propagation through call chain", func(t *testing.T) {
		originalID := "req-original-456"

		// Start with context containing request ID
		ctx := context.WithValue(context.Background(), requestIDKey, originalID)

		// Simulate passing through multiple function calls
		ctx = simulateServiceA(ctx)
		ctx = simulateServiceB(ctx)
		ctx = simulateServiceC(ctx)

		// Request ID should still be available
		finalID := ctx.Value(requestIDKey)
		assert.Equal(t, originalID, finalID)
	})

	t.Run("missing request ID", func(t *testing.T) {
		ctx := context.Background()

		// Should return nil for missing key
		requestID := ctx.Value(requestIDKey)
		assert.Nil(t, requestID)
	})

	t.Run("override request ID", func(t *testing.T) {
		originalID := "req-original"
		newID := "req-new"

		ctx := context.WithValue(context.Background(), requestIDKey, originalID)

		// Override with new value
		ctx = context.WithValue(ctx, requestIDKey, newID)

		retrievedID := ctx.Value(requestIDKey)
		assert.Equal(t, newID, retrievedID)
		assert.NotEqual(t, originalID, retrievedID)
	})
}

func TestContext_TraceContext(t *testing.T) {
	t.Run("full trace context", func(t *testing.T) {
		traceID := "trace-789"
		spanID := "span-abc"
		parentSpan := "span-parent-def"

		ctx := context.Background()
		ctx = context.WithValue(ctx, traceIDKey, traceID)
		ctx = context.WithValue(ctx, "span-id", spanID)
		ctx = context.WithValue(ctx, "parent-span", parentSpan)

		assert.Equal(t, traceID, ctx.Value(traceIDKey))
		assert.Equal(t, spanID, ctx.Value("span-id"))
		assert.Equal(t, parentSpan, ctx.Value("parent-span"))
	})

	t.Run("trace context inheritance", func(t *testing.T) {
		parentTraceID := "trace-parent-123"
		childSpanID := "span-child-456"

		// Parent context with trace ID
		parentCtx := context.WithValue(context.Background(), traceIDKey, parentTraceID)

		// Child context inherits trace ID, adds span ID
		childCtx := context.WithValue(parentCtx, "span-id", childSpanID)

		// Both values should be available in child context
		assert.Equal(t, parentTraceID, childCtx.Value(traceIDKey))
		assert.Equal(t, childSpanID, childCtx.Value("span-id"))

		// Parent should not have child's span ID
		assert.Nil(t, parentCtx.Value("span-id"))
	})
}

func TestContext_UserContext(t *testing.T) {
	t.Run("user authentication context", func(t *testing.T) {
		userID := "user-123"
		tenantID := "tenant-456"
		roles := []string{"admin", "user"}

		ctx := context.Background()
		ctx = context.WithValue(ctx, userIDKey, userID)
		ctx = context.WithValue(ctx, tenantIDKey, tenantID)
		ctx = context.WithValue(ctx, "roles", roles)

		assert.Equal(t, userID, ctx.Value(userIDKey))
		assert.Equal(t, tenantID, ctx.Value(tenantIDKey))
		assert.Equal(t, roles, ctx.Value("roles"))
	})

	t.Run("anonymous user context", func(t *testing.T) {
		ctx := context.Background()

		// No user information should be present
		assert.Nil(t, ctx.Value(userIDKey))
		assert.Nil(t, ctx.Value(tenantIDKey))
		assert.Nil(t, ctx.Value("roles"))
	})
}

// --------------------- Context Timeout Tests ---------------------

func TestContext_Timeouts(t *testing.T) {
	t.Run("context with timeout", func(t *testing.T) {
		timeout := 100 * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		// Context should not be done immediately
		select {
		case <-ctx.Done():
			t.Fatal("Context should not be done immediately")
		default:
			// Expected
		}

		// Wait for timeout
		select {
		case <-ctx.Done():
			assert.Equal(t, context.DeadlineExceeded, ctx.Err())
		case <-time.After(timeout + 50*time.Millisecond):
			t.Fatal("Context should have timed out")
		}
	})

	t.Run("context with deadline", func(t *testing.T) {
		deadline := time.Now().Add(50 * time.Millisecond)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()

		// Check deadline
		ctxDeadline, ok := ctx.Deadline()
		assert.True(t, ok)
		assert.True(t, deadline.Equal(ctxDeadline))

		// Wait for deadline
		<-ctx.Done()
		assert.Equal(t, context.DeadlineExceeded, ctx.Err())
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		// Context should not be done initially
		select {
		case <-ctx.Done():
			t.Fatal("Context should not be done initially")
		default:
			// Expected
		}

		// Cancel context
		cancel()

		// Context should be done now
		select {
		case <-ctx.Done():
			assert.Equal(t, context.Canceled, ctx.Err())
		case <-time.After(10 * time.Millisecond):
			t.Fatal("Context should be canceled")
		}
	})

	t.Run("nested context cancellation", func(t *testing.T) {
		parentCtx, parentCancel := context.WithCancel(context.Background())
		childCtx, childCancel := context.WithCancel(parentCtx)
		defer childCancel()

		// Cancel parent
		parentCancel()

		// Child should also be canceled
		select {
		case <-childCtx.Done():
			assert.Equal(t, context.Canceled, childCtx.Err())
		case <-time.After(10 * time.Millisecond):
			t.Fatal("Child context should be canceled when parent is canceled")
		}
	})
}

// --------------------- Context Message Integration Tests ---------------------

func TestContext_MessageIntegration(t *testing.T) {
	t.Run("context to headers conversion", func(t *testing.T) {
		// Create context with values
		ctx := context.Background()
		ctx = context.WithValue(ctx, requestIDKey, "req-123")
		ctx = context.WithValue(ctx, traceIDKey, "trace-456")
		ctx = context.WithValue(ctx, userIDKey, "user-789")

		// Convert context values to headers
		headers := contextToHeaders(ctx)

		assert.Equal(t, "req-123", headers["X-Request-Id"])
		assert.Equal(t, "trace-456", headers["X-Trace-Id"])
		assert.Equal(t, "user-789", headers["X-User-Id"])
	})

	t.Run("headers to context conversion", func(t *testing.T) {
		headers := map[string]string{
			"X-Request-Id": "req-from-headers",
			"X-Trace-Id":   "trace-from-headers",
			"X-User-Id":    "user-from-headers",
			"Content-Type": "application/json", // Should be ignored
		}

		// Convert headers to context
		ctx := headersToContext(context.Background(), headers)

		assert.Equal(t, "req-from-headers", ctx.Value(requestIDKey))
		assert.Equal(t, "trace-from-headers", ctx.Value(traceIDKey))
		assert.Equal(t, "user-from-headers", ctx.Value(userIDKey))
		assert.Nil(t, ctx.Value("content-type")) // Should not be in context
	})

	t.Run("round trip context preservation", func(t *testing.T) {
		// Original context
		originalCtx := context.Background()
		originalCtx = context.WithValue(originalCtx, requestIDKey, "req-roundtrip")
		originalCtx = context.WithValue(originalCtx, traceIDKey, "trace-roundtrip")

		// Convert to headers and back
		headers := contextToHeaders(originalCtx)
		restoredCtx := headersToContext(context.Background(), headers)

		// Values should be preserved
		assert.Equal(t, originalCtx.Value(requestIDKey), restoredCtx.Value(requestIDKey))
		assert.Equal(t, originalCtx.Value(traceIDKey), restoredCtx.Value(traceIDKey))
	})
}

// --------------------- Context Best Practices Tests ---------------------

func TestContext_BestPractices(t *testing.T) {
	t.Run("context should not store nil values", func(t *testing.T) {
		ctx := context.WithValue(context.Background(), requestIDKey, nil)

		// Nil values should be avoided, but are technically valid
		value := ctx.Value(requestIDKey)
		assert.Nil(t, value)
	})

	t.Run("context key type safety", func(t *testing.T) {
		// Using typed keys prevents collisions
		const stringKey = "request-id"

		ctx := context.Background()
		ctx = context.WithValue(ctx, requestIDKey, "typed-value")
		ctx = context.WithValue(ctx, stringKey, "string-value")

		// Should be different values
		assert.Equal(t, "typed-value", ctx.Value(requestIDKey))
		assert.Equal(t, "string-value", ctx.Value(stringKey))
		assert.NotEqual(t, ctx.Value(requestIDKey), ctx.Value(stringKey))
	})

	t.Run("context value immutability", func(t *testing.T) {
		originalValue := "original"
		ctx1 := context.WithValue(context.Background(), requestIDKey, originalValue)

		// Create new context with different value
		newValue := "new"
		ctx2 := context.WithValue(ctx1, requestIDKey, newValue)

		// Original context should be unchanged
		assert.Equal(t, originalValue, ctx1.Value(requestIDKey))
		assert.Equal(t, newValue, ctx2.Value(requestIDKey))
	})

	t.Run("context inheritance", func(t *testing.T) {
		// Parent context with multiple values
		parentCtx := context.Background()
		parentCtx = context.WithValue(parentCtx, requestIDKey, "parent-req")
		parentCtx = context.WithValue(parentCtx, traceIDKey, "parent-trace")

		// Child inherits all parent values
		childCtx := context.WithValue(parentCtx, userIDKey, "child-user")

		// Child should have all values
		assert.Equal(t, "parent-req", childCtx.Value(requestIDKey))
		assert.Equal(t, "parent-trace", childCtx.Value(traceIDKey))
		assert.Equal(t, "child-user", childCtx.Value(userIDKey))

		// Parent should not have child's value
		assert.Nil(t, parentCtx.Value(userIDKey))
	})
}

// --------------------- Helper Functions ---------------------

// simulateServiceA simulates passing context through service A
func simulateServiceA(ctx context.Context) context.Context {
	// Service A might add its own values but preserves existing ones
	return context.WithValue(ctx, "service-a-processed", time.Now())
}

// simulateServiceB simulates passing context through service B
func simulateServiceB(ctx context.Context) context.Context {
	// Service B might add its own values but preserves existing ones
	return context.WithValue(ctx, "service-b-processed", time.Now())
}

// simulateServiceC simulates passing context through service C
func simulateServiceC(ctx context.Context) context.Context {
	// Service C might add its own values but preserves existing ones
	return context.WithValue(ctx, "service-c-processed", time.Now())
}

// contextToHeaders extracts relevant context values into headers
func contextToHeaders(ctx context.Context) map[string]string {
	headers := make(map[string]string)

	if requestID := ctx.Value(requestIDKey); requestID != nil {
		if id, ok := requestID.(string); ok {
			headers["X-Request-Id"] = id
		}
	}

	if traceID := ctx.Value(traceIDKey); traceID != nil {
		if id, ok := traceID.(string); ok {
			headers["X-Trace-Id"] = id
		}
	}

	if userID := ctx.Value(userIDKey); userID != nil {
		if id, ok := userID.(string); ok {
			headers["X-User-Id"] = id
		}
	}

	return headers
}

// headersToContext extracts relevant headers into context values
func headersToContext(ctx context.Context, headers map[string]string) context.Context {
	if requestID, exists := headers["X-Request-Id"]; exists {
		ctx = context.WithValue(ctx, requestIDKey, requestID)
	}

	if traceID, exists := headers["X-Trace-Id"]; exists {
		ctx = context.WithValue(ctx, traceIDKey, traceID)
	}

	if userID, exists := headers["X-User-Id"]; exists {
		ctx = context.WithValue(ctx, userIDKey, userID)
	}

	return ctx
}

// --------------------- Performance Tests ---------------------

func BenchmarkContext_WithValue(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ctx = context.WithValue(ctx, requestIDKey, "req-123")
	}
}

func BenchmarkContext_Value(b *testing.B) {
	ctx := context.WithValue(context.Background(), requestIDKey, "req-123")

	for b.Loop() {
		_ = ctx.Value(requestIDKey)
	}
}

func BenchmarkContext_MultipleValues(b *testing.B) {

	for b.Loop() {
		ctx := context.Background()
		ctx = context.WithValue(ctx, requestIDKey, "req-123")
		ctx = context.WithValue(ctx, traceIDKey, "trace-456")
		ctx = context.WithValue(ctx, userIDKey, "user-789")
		_ = context.WithValue(ctx, tenantIDKey, "tenant-abc")
	}
}

func BenchmarkContext_ContextToHeaders(b *testing.B) {
	ctx := context.Background()
	ctx = context.WithValue(ctx, requestIDKey, "req-123")
	ctx = context.WithValue(ctx, traceIDKey, "trace-456")
	ctx = context.WithValue(ctx, userIDKey, "user-789")

	for b.Loop() {
		_ = contextToHeaders(ctx)
	}
}

func BenchmarkContext_HeadersToContext(b *testing.B) {
	headers := map[string]string{
		"X-Request-Id": "req-123",
		"X-Trace-Id":   "trace-456",
		"X-User-Id":    "user-789",
	}

	for b.Loop() {
		_ = headersToContext(context.Background(), headers)
	}
}

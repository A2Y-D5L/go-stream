package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name   string
		config LoggerConfig
	}{
		{
			name: "JSON format",
			config: LoggerConfig{
				Level:  slog.LevelDebug,
				Format: JSON,
			},
		},
		{
			name: "Text format",
			config: LoggerConfig{
				Level:  slog.LevelInfo,
				Format: Text,
			},
		},
		{
			name: "With sampling",
			config: LoggerConfig{
				Level:  slog.LevelInfo,
				Format: JSON,
				Sampling: &SamplingConfig{
					Enabled:      true,
					Rate:         0.5,
					MaxPerSecond: 10,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			tt.config.Output = &buf

			logger := NewLogger(tt.config)
			if logger == nil {
				t.Error("Expected logger to be created")
			}

			// Test that we can log
			logger.Info("test message", slog.String("key", "value"))

			output := buf.String()
			if output == "" {
				t.Error("Expected log output")
			}

			// Verify format
			if tt.config.Format == JSON {
				// Should be valid JSON
				var logEntry map[string]any
				if err := json.Unmarshal([]byte(output), &logEntry); err != nil {
					t.Errorf("Expected valid JSON output, got error: %v", err)
				}
			}
		})
	}
}

func TestLoggerLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelWarn,
		Format: JSON,
		Output: &buf,
	})

	// These should not be logged (below threshold)
	logger.Debug("debug message")
	logger.Info("info message")

	// These should be logged
	logger.Warn("warn message")
	logger.Error("error message")

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")

	// Should only have 2 lines (warn and error)
	if len(lines) != 2 {
		t.Errorf("Expected 2 log lines, got %d", len(lines))
	}

	// Verify content
	if !strings.Contains(output, "warn message") {
		t.Error("Expected warn message in output")
	}
	if !strings.Contains(output, "error message") {
		t.Error("Expected error message in output")
	}
	if strings.Contains(output, "debug message") {
		t.Error("Debug message should not be in output")
	}
	if strings.Contains(output, "info message") {
		t.Error("Info message should not be in output")
	}
}

func TestLoggerWith(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelInfo,
		Format: JSON,
		Output: &buf,
	})

	// Create logger with additional fields
	childLogger := logger.With(
		slog.String("service", "test"),
		slog.String("version", "1.0"),
	)

	childLogger.Info("test message", slog.String("extra", "field"))

	output := buf.String()
	if !strings.Contains(output, "service") {
		t.Error("Expected service field in output")
	}
	if !strings.Contains(output, "test") {
		t.Error("Expected service value in output")
	}
	if !strings.Contains(output, "version") {
		t.Error("Expected version field in output")
	}
	if !strings.Contains(output, "extra") {
		t.Error("Expected extra field in output")
	}
}

func TestLoggerWithContext(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelInfo,
		Format: JSON,
		Output: &buf,
	})

	// Create context with values
	ctx := context.Background()
	ctx = context.WithValue(ctx, "request_id", "req-123")
	ctx = context.WithValue(ctx, "trace_id", "trace-456")
	ctx = context.WithValue(ctx, "operation", "test-op")

	// Create logger with context
	contextLogger := logger.WithContext(ctx)
	contextLogger.Info("test message")

	output := buf.String()
	if !strings.Contains(output, "request_id") {
		t.Error("Expected request_id field in output")
	}
	if !strings.Contains(output, "req-123") {
		t.Error("Expected request_id value in output")
	}
	if !strings.Contains(output, "trace_id") {
		t.Error("Expected trace_id field in output")
	}
	if !strings.Contains(output, "trace-456") {
		t.Error("Expected trace_id value in output")
	}
	if !strings.Contains(output, "operation") {
		t.Error("Expected operation field in output")
	}
	if !strings.Contains(output, "test-op") {
		t.Error("Expected operation value in output")
	}
}

func TestLoggerLogMethod(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelDebug,
		Format: JSON,
		Output: &buf,
	})

	ctx := context.Background()
	logger.Log(ctx, slog.LevelInfo, "test log method",
		slog.String("key", "value"),
		slog.Int("number", 42),
	)

	output := buf.String()
	if !strings.Contains(output, "test log method") {
		t.Error("Expected log message in output")
	}
	if !strings.Contains(output, "key") {
		t.Error("Expected key field in output")
	}
	if !strings.Contains(output, "value") {
		t.Error("Expected value in output")
	}
	if !strings.Contains(output, "number") {
		t.Error("Expected number field in output")
	}
	if !strings.Contains(output, "42") {
		t.Error("Expected number value in output")
	}
}

func TestSampling(t *testing.T) {
	tests := []struct {
		name   string
		config *SamplingConfig
	}{
		{
			name: "Rate sampling",
			config: &SamplingConfig{
				Enabled: true,
				Rate:    0.5, // 50% sampling
			},
		},
		{
			name: "Per-second limit",
			config: &SamplingConfig{
				Enabled:      true,
				Rate:         1.0, // No rate limiting
				MaxPerSecond: 5,   // Max 5 per second
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := NewLogger(LoggerConfig{
				Level:    slog.LevelInfo,
				Format:   JSON,
				Output:   &buf,
				Sampling: tt.config,
			})

			// Log many messages
			for i := range 100 {
				logger.Info("test message", slog.Int("iteration", i))
			}

			output := buf.String()
			lines := strings.Split(strings.TrimSpace(output), "\n")

			// Should have fewer than 100 lines due to sampling
			if len(lines) >= 100 {
				t.Errorf("Expected sampling to reduce log count, got %d lines", len(lines))
			}

			// Errors should never be sampled
			buf.Reset()
			for i := range 10 {
				logger.Error("error message", slog.Int("iteration", i))
			}

			errorOutput := buf.String()
			errorLines := strings.Split(strings.TrimSpace(errorOutput), "\n")
			if len(errorLines) != 10 {
				t.Errorf("Expected all error messages to be logged, got %d lines", len(errorLines))
			}
		})
	}
}

func TestDefaultLogger(t *testing.T) {
	originalLogger := Default()
	defer SetDefaultLogger(originalLogger) // Restore after test

	var buf bytes.Buffer
	testLogger := NewLogger(LoggerConfig{
		Level:  slog.LevelInfo,
		Format: JSON,
		Output: &buf,
	})

	SetDefaultLogger(testLogger)

	// Test package-level functions
	Info("test info", slog.String("key", "value"))
	Warn("test warn")
	Error("test error")

	output := buf.String()
	if !strings.Contains(output, "test info") {
		t.Error("Expected info message in output")
	}
	if !strings.Contains(output, "test warn") {
		t.Error("Expected warn message in output")
	}
	if !strings.Contains(output, "test error") {
		t.Error("Expected error message in output")
	}
}

func TestNATSFieldHelpers(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelInfo,
		Format: JSON,
		Output: &buf,
	})

	logger.Info("NATS operation",
		NATSSubject("test.subject"),
		NATSMsgID("msg-123"),
		NATSConnection("nats://localhost:4222"),
		NATSConsumer("test-consumer"),
		NATSStream("test-stream"),
		Duration("elapsed", 100*time.Millisecond),
		Operation("publish"),
		RequestID("req-456"),
		TraceID("trace-789"),
		WorkerCount(5),
		QueueDepth(10),
		RetryAttempt(2),
		BytesProcessed(1024),
		MessageCount(42),
	)

	output := buf.String()

	expectedFields := []string{
		"nats.subject", "test.subject",
		"nats.message_id", "msg-123",
		"nats.connection", "nats://localhost:4222",
		"nats.consumer", "test-consumer",
		"nats.stream", "test-stream",
		"elapsed",
		"operation", "publish",
		"request_id", "req-456",
		"trace_id", "trace-789",
		"worker_count",
		"queue_depth",
		"retry_attempt",
		"bytes_processed",
		"message_count",
	}

	for _, field := range expectedFields {
		if !strings.Contains(output, field) {
			t.Errorf("Expected field %s in output", field)
		}
	}
}

func TestErrorField(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelInfo,
		Format: JSON,
		Output: &buf,
	})

	testErr := errors.New("test error message")
	logger.Error("operation failed", ErrorField(testErr))

	output := buf.String()
	if !strings.Contains(output, "test error message") {
		t.Error("Expected error message in output")
	}
	if !strings.Contains(output, "error") {
		t.Error("Expected error field in output")
	}
}

func TestSamplerShouldLog(t *testing.T) {
	sampler := newSampler(&SamplingConfig{
		Enabled: true,
		Rate:    0.5,
	})

	// Errors should always be logged
	if !sampler.shouldLog(slog.LevelError) {
		t.Error("Expected error level to always be logged")
	}

	// Warnings should always be logged
	if !sampler.shouldLog(slog.LevelWarn) {
		t.Error("Expected warn level to always be logged")
	}

	// Test rate limiting over many calls
	infoCount := 0
	for range 1000 {
		if sampler.shouldLog(slog.LevelInfo) {
			infoCount++
		}
	}

	// Should be roughly 50% due to rate limiting (allow some variance)
	if infoCount < 450 || infoCount > 550 {
		t.Errorf("Expected roughly 500 info logs (±50), got %d", infoCount)
	}
}

func BenchmarkLogger_Info(b *testing.B) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelInfo,
		Format: JSON,
		Output: &buf,
	})

	for i := 0; b.Loop(); i++ {
		logger.Info("benchmark message",
			slog.String("key", "value"),
			slog.Int("iteration", i),
		)
	}
}

func BenchmarkLogger_InfoWithSampling(b *testing.B) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelInfo,
		Format: JSON,
		Output: &buf,
		Sampling: &SamplingConfig{
			Enabled: true,
			Rate:    0.1, // 10% sampling
		},
	})

	for i := 0; b.Loop(); i++ {
		logger.Info("benchmark message",
			slog.String("key", "value"),
			slog.Int("iteration", i),
		)
	}
}

func BenchmarkLogger_With(b *testing.B) {
	var buf bytes.Buffer
	logger := NewLogger(LoggerConfig{
		Level:  slog.LevelInfo,
		Format: JSON,
		Output: &buf,
	})

	for i := 0; b.Loop(); i++ {
		childLogger := logger.With(
			slog.String("service", "test"),
			slog.Int("instance", i),
		)
		childLogger.Info("benchmark message")
	}
}

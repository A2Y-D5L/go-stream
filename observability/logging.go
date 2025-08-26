package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"time"
)

// LogFormat represents the output format for logs
type LogFormat int

const (
	// JSON format outputs structured JSON logs
	JSON LogFormat = iota
	// Text format outputs human-readable text logs
	Text
)

// Logger interface defines the logging contract for the go-stream library
type Logger interface {
	Debug(msg string, fields ...slog.Attr)
	Info(msg string, fields ...slog.Attr)
	Warn(msg string, fields ...slog.Attr)
	Error(msg string, fields ...slog.Attr)
	With(fields ...slog.Attr) Logger
	WithContext(ctx context.Context) Logger
	Log(ctx context.Context, level slog.Level, msg string, fields ...slog.Attr)
}

// LoggerConfig holds configuration for creating a logger
type LoggerConfig struct {
	Level    slog.Level
	Format   LogFormat
	Output   io.Writer
	Sampling *SamplingConfig
}

// SamplingConfig controls log sampling to reduce volume in high-throughput scenarios
type SamplingConfig struct {
	Enabled      bool
	Rate         float64 // 0.0-1.0, percentage of logs to keep
	MaxPerSecond int     // Maximum logs per second
}

// defaultLogger is a package-level logger instance
var defaultLogger Logger = NewLogger(LoggerConfig{
	Level:  slog.LevelInfo,
	Format: Text,
	Output: os.Stderr,
})

// SetDefaultLogger sets the package-level default logger
func SetDefaultLogger(logger Logger) {
	defaultLogger = logger
}

// Default returns the package-level default logger
func Default() Logger {
	return defaultLogger
}

// Convenience functions using the default logger
func Debug(msg string, fields ...slog.Attr) {
	defaultLogger.Debug(msg, fields...)
}

func Info(msg string, fields ...slog.Attr) {
	defaultLogger.Info(msg, fields...)
}

func Warn(msg string, fields ...slog.Attr) {
	defaultLogger.Warn(msg, fields...)
}

func Error(msg string, fields ...slog.Attr) {
	defaultLogger.Error(msg, fields...)
}

// logger implements the Logger interface
type logger struct {
	slogger  *slog.Logger
	sampling *sampler
	attrs    []slog.Attr
}

// NewLogger creates a new logger with the given configuration
func NewLogger(config LoggerConfig) Logger {
	// Set default output if not provided
	if config.Output == nil {
		config.Output = os.Stderr
	}

	// Create handler based on format
	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level: config.Level,
	}

	switch config.Format {
	case JSON:
		handler = slog.NewJSONHandler(config.Output, opts)
	default:
		handler = slog.NewTextHandler(config.Output, opts)
	}

	// Create sampler if sampling is enabled
	var s *sampler
	if config.Sampling != nil && config.Sampling.Enabled {
		s = newSampler(config.Sampling)
	}

	return &logger{
		slogger:  slog.New(handler),
		sampling: s,
	}
}

// Debug logs a debug message with optional structured fields
func (l *logger) Debug(msg string, fields ...slog.Attr) {
	l.log(slog.LevelDebug, msg, fields...)
}

// Info logs an info message with optional structured fields
func (l *logger) Info(msg string, fields ...slog.Attr) {
	l.log(slog.LevelInfo, msg, fields...)
}

// Warn logs a warning message with optional structured fields
func (l *logger) Warn(msg string, fields ...slog.Attr) {
	l.log(slog.LevelWarn, msg, fields...)
}

// Error logs an error message with optional structured fields
func (l *logger) Error(msg string, fields ...slog.Attr) {
	l.log(slog.LevelError, msg, fields...)
}

// With creates a new logger with additional structured fields
func (l *logger) With(fields ...slog.Attr) Logger {
	// Combine existing attrs with new ones
	allAttrs := make([]slog.Attr, 0, len(l.attrs)+len(fields))
	allAttrs = append(allAttrs, l.attrs...)
	allAttrs = append(allAttrs, fields...)

	// Create new slogger with additional attributes
	args := make([]any, len(fields)*2)
	for i, attr := range fields {
		args[i*2] = attr.Key
		args[i*2+1] = attr.Value
	}

	return &logger{
		slogger:  l.slogger.With(args...),
		sampling: l.sampling,
		attrs:    allAttrs,
	}
}

// WithContext creates a new logger with context-aware fields
func (l *logger) WithContext(ctx context.Context) Logger {
	var fields []slog.Attr

	// Extract request ID from context if available
	if reqID := ctx.Value("request_id"); reqID != nil {
		if id, ok := reqID.(string); ok {
			fields = append(fields, slog.String("request_id", id))
		}
	}

	// Extract trace ID from context if available
	if traceID := ctx.Value("trace_id"); traceID != nil {
		if id, ok := traceID.(string); ok {
			fields = append(fields, slog.String("trace_id", id))
		}
	}

	// Extract operation from context if available
	if op := ctx.Value("operation"); op != nil {
		if operation, ok := op.(string); ok {
			fields = append(fields, slog.String("operation", operation))
		}
	}

	if len(fields) == 0 {
		return l
	}

	return l.With(fields...)
}

// Log logs a message at the specified level with optional structured fields
func (l *logger) Log(ctx context.Context, level slog.Level, msg string, fields ...slog.Attr) {
	// Check if sampling is enabled and should skip this log
	if l.sampling != nil && !l.sampling.shouldLog(level) {
		return
	}

	// Convert slog.Attr to args for slogger
	args := make([]any, len(fields)*2)
	for i, attr := range fields {
		args[i*2] = attr.Key
		args[i*2+1] = attr.Value
	}

	// Log using the underlying slogger
	l.slogger.Log(ctx, level, msg, args...)
}

// log is the internal logging method that handles sampling and actual logging
func (l *logger) log(level slog.Level, msg string, fields ...slog.Attr) {
	l.Log(context.Background(), level, msg, fields...)
}

// sampler implements log sampling to control high-volume logging
type sampler struct {
	config   *SamplingConfig
	counter  atomic.Uint64
	lastSec  atomic.Int64
	secCount atomic.Uint64
}

// newSampler creates a new log sampler
func newSampler(config *SamplingConfig) *sampler {
	return &sampler{
		config: config,
	}
}

// shouldLog determines if a log entry should be written based on sampling rules
func (s *sampler) shouldLog(level slog.Level) bool {
	// Always log errors and warnings regardless of sampling
	if level >= slog.LevelWarn {
		return true
	}

	// Check per-second limit first if enabled
	if s.config.MaxPerSecond > 0 {
		now := time.Now().Unix()
		lastSec := s.lastSec.Load()

		if now != lastSec {
			// New second, reset counter
			if s.lastSec.CompareAndSwap(lastSec, now) {
				s.secCount.Store(1)
				// Don't return early, still check rate sampling
			}
		} else {
			// Same second, increment and check limit
			count := s.secCount.Add(1)
			if int(count) > s.config.MaxPerSecond {
				return false
			}
		}
	}

	// Check rate-based sampling
	if s.config.Rate < 1.0 {
		count := s.counter.Add(1)
		// Simple deterministic sampling based on counter
		if float64(count%100)/100.0 >= s.config.Rate {
			return false
		}
	}

	return true
}

// NATS-specific field helpers for consistent logging

// NATSSubject creates a subject field
func NATSSubject(subject string) slog.Attr {
	return slog.String("nats.subject", subject)
}

// NATSMsgID creates a message ID field
func NATSMsgID(msgID string) slog.Attr {
	return slog.String("nats.message_id", msgID)
}

// NATSConnection creates a connection field
func NATSConnection(url string) slog.Attr {
	return slog.String("nats.connection", url)
}

// NATSConsumer creates a consumer field
func NATSConsumer(consumer string) slog.Attr {
	return slog.String("nats.consumer", consumer)
}

// NATSStream creates a stream field
func NATSStream(stream string) slog.Attr {
	return slog.String("nats.stream", stream)
}

// Duration creates a duration field with appropriate precision
func Duration(key string, d time.Duration) slog.Attr {
	return slog.Duration(key, d)
}

// Operation creates an operation field
func Operation(op string) slog.Attr {
	return slog.String("operation", op)
}

// Error creates an error field
func ErrorField(err error) slog.Attr {
	return slog.String("error", err.Error())
}

// RequestID creates a request ID field
func RequestID(reqID string) slog.Attr {
	return slog.String("request_id", reqID)
}

// TraceID creates a trace ID field
func TraceID(traceID string) slog.Attr {
	return slog.String("trace_id", traceID)
}

// WorkerCount creates a worker count field
func WorkerCount(count int) slog.Attr {
	return slog.Int("worker_count", count)
}

// QueueDepth creates a queue depth field
func QueueDepth(depth int) slog.Attr {
	return slog.Int("queue_depth", depth)
}

// RetryAttempt creates a retry attempt field
func RetryAttempt(attempt int) slog.Attr {
	return slog.Int("retry_attempt", attempt)
}

// BytesProcessed creates a bytes processed field
func BytesProcessed(bytes int64) slog.Attr {
	return slog.Int64("bytes_processed", bytes)
}

// MessageCount creates a message count field
func MessageCount(count int64) slog.Attr {
	return slog.Int64("message_count", count)
}

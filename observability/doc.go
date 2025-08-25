// Package observability provides comprehensive observability capabilities for go-stream,
// including structured logging, metrics collection, and distributed tracing.
//
// # Structured Logging
//
// The observability package provides a production-ready structured logging solution
// built on Go's standard slog package. It offers enhanced features specifically
// designed for NATS-based messaging systems:
//
//	logger := observability.NewLogger(observability.LoggerConfig{
//		Level:  slog.LevelInfo,
//		Format: observability.JSON,
//		Output: os.Stdout,
//	})
//
//	logger.Info("message published",
//		observability.NATSSubject("orders.created"),
//		observability.Duration("publish_time", time.Since(start)),
//	)
//
// # Context-Aware Logging
//
// The logger automatically extracts correlation fields from context:
//
//	ctx := context.WithValue(ctx, "request_id", "req-123")
//	ctx = context.WithValue(ctx, "trace_id", "trace-456")
//
//	ctxLogger := logger.WithContext(ctx)
//	ctxLogger.Info("processing request") // Includes request_id and trace_id
//
// # High-Volume Sampling
//
// For production systems with high message volumes, sampling can prevent log overload:
//
//	logger := observability.NewLogger(observability.LoggerConfig{
//		Level:  slog.LevelInfo,
//		Format: observability.JSON,
//		Output: os.Stdout,
//		Sampling: &observability.SamplingConfig{
//			Enabled:      true,
//			Rate:         0.1, // Sample 10% of messages
//			MaxPerSecond: 100, // Limit to 100 logs per second
//		},
//	})
//
// # NATS-Specific Field Helpers
//
// Specialized field helpers for NATS operations:
//
//	logger.Info("subscriber processing",
//		observability.NATSSubject("user.events"),
//		observability.NATSMsgID("msg-789"),
//		observability.NATSConnection("primary"),
//		observability.MessageCount(processed),
//		observability.BytesProcessed(totalBytes),
//	)
//
// # Integration with go-stream
//
// The observability package integrates seamlessly with go-stream operations.
// See the examples directory for comprehensive usage patterns and the
// INTEGRATION.md file for production deployment guidance.
package observability

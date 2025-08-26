package observability

import (
	"context"
	"log/slog"
	"time"
)

// StreamLogger provides observability-enhanced logging for stream operations
type StreamLogger struct {
	Logger
	streamID string
}

// NewStreamLogger creates a logger specifically for stream operations
func NewStreamLogger(logger Logger, streamID string) *StreamLogger {
	return &StreamLogger{
		Logger:   logger.With(slog.String("stream_id", streamID)),
		streamID: streamID,
	}
}

// LogConnection logs stream connection events
func (sl *StreamLogger) LogConnection(ctx context.Context, serverURL string, connected bool) {
	if connected {
		sl.WithContext(ctx).Info("stream connected",
			NATSConnection(serverURL),
			slog.Bool("connected", true),
			Operation("stream-connect"),
		)
	} else {
		sl.WithContext(ctx).Warn("stream disconnected",
			NATSConnection(serverURL),
			slog.Bool("connected", false),
			Operation("stream-disconnect"),
		)
	}
}

// LogPublish logs message publishing with performance metrics
func (sl *StreamLogger) LogPublish(ctx context.Context, subject string, msgSize int, duration time.Duration, err error) {
	logger := sl.WithContext(ctx).With(
		NATSSubject(subject),
		BytesProcessed(int64(msgSize)),
		Duration("publish_duration", duration),
		Operation("publish"),
	)

	if err != nil {
		logger.Error("message publish failed",
			ErrorField(err),
			slog.String("failure_reason", "publish_error"),
		)
	} else {
		logger.Info("message published successfully")
	}
}

// LogSubscribe logs subscription events
func (sl *StreamLogger) LogSubscribe(ctx context.Context, subject string, queueGroup string, err error) {
	logger := sl.WithContext(ctx).With(
		NATSSubject(subject),
		Operation("subscribe"),
	)

	if queueGroup != "" {
		logger = logger.With(slog.String("queue_group", queueGroup))
	}

	if err != nil {
		logger.Error("subscription failed",
			ErrorField(err),
			slog.String("failure_reason", "subscribe_error"),
		)
	} else {
		logger.Info("subscription established")
	}
}

// LogMessageReceived logs received messages with processing context
func (sl *StreamLogger) LogMessageReceived(ctx context.Context, subject string, msgID string, msgSize int) {
	sl.WithContext(ctx).Info("message received",
		NATSSubject(subject),
		NATSMsgID(msgID),
		BytesProcessed(int64(msgSize)),
		Operation("message-received"),
	)
}

// LogMessageProcessed logs completed message processing
func (sl *StreamLogger) LogMessageProcessed(ctx context.Context, subject string, msgID string, duration time.Duration, err error) {
	logger := sl.WithContext(ctx).With(
		NATSSubject(subject),
		NATSMsgID(msgID),
		Duration("processing_duration", duration),
		Operation("message-processed"),
	)

	if err != nil {
		logger.Error("message processing failed",
			ErrorField(err),
			slog.String("failure_reason", "processing_error"),
		)
	} else {
		logger.Info("message processed successfully")
	}
}

// PublisherLogger provides observability for publisher operations
type PublisherLogger struct {
	*StreamLogger
	publisherID string
}

// NewPublisherLogger creates a logger for publisher operations
func NewPublisherLogger(streamLogger *StreamLogger, publisherID string) *PublisherLogger {
	return &PublisherLogger{
		StreamLogger: &StreamLogger{
			Logger:   streamLogger.With(slog.String("publisher_id", publisherID)),
			streamID: streamLogger.streamID,
		},
		publisherID: publisherID,
	}
}

// SubscriberLogger provides observability for subscriber operations
type SubscriberLogger struct {
	*StreamLogger
	subscriberID string
}

// NewSubscriberLogger creates a logger for subscriber operations
func NewSubscriberLogger(streamLogger *StreamLogger, subscriberID string) *SubscriberLogger {
	return &SubscriberLogger{
		StreamLogger: &StreamLogger{
			Logger:   streamLogger.With(slog.String("subscriber_id", subscriberID)),
			streamID: streamLogger.streamID,
		},
		subscriberID: subscriberID,
	}
}

// LogBatchProcessing logs batch message processing
func (sub *SubscriberLogger) LogBatchProcessing(ctx context.Context, batchSize int, totalBytes int64) {
	sub.WithContext(ctx).Info("processing message batch",
		slog.Int("batch_size", batchSize),
		BytesProcessed(totalBytes),
		Operation("batch-processing"),
	)
}

// LogSubscriberHealth logs subscriber health metrics
func (sub *SubscriberLogger) LogSubscriberHealth(ctx context.Context, queueDepth int, processingRate float64) {
	sub.WithContext(ctx).Info("subscriber health check",
		QueueDepth(queueDepth),
		slog.Float64("processing_rate_per_sec", processingRate),
		Operation("health-check"),
	)
}

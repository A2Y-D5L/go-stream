package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/a2y-d5l/go-stream"
	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/observability"
	"github.com/a2y-d5l/go-stream/sub"
)

// This example demonstrates the observability package being used in the context it was designed for:
// real go-stream operations with actual Stream, Publisher, and Subscriber instances.

type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	UserID     string    `json:"user_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
	RequestID  string    `json:"request_id"`
}

type PaymentCommand struct {
	OrderID   string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	RequestID string  `json:"request_id"`
}

func main() {
	println("=== go-stream Observability Integration Demo ===")
	println("Showing observability logging in real Stream, Publisher, and Subscriber operations")
	
	ctx := context.Background()
	
	// Initialize observability
	baseLogger := initObservabilityLogger()
	
	// Create stream with observability integration
	stream, streamLogger := createObservabilityEnabledStream(ctx, baseLogger)
	defer stream.Close(ctx)
	
	// Demonstrate real-world microservices scenario
	demonstrateECommerceFlow(ctx, stream, streamLogger)
}

func initObservabilityLogger() observability.Logger {
	return observability.NewLogger(observability.LoggerConfig{
		Level:  slog.LevelInfo,
		Format: observability.JSON,
		Output: os.Stdout,
		Sampling: &observability.SamplingConfig{
			Enabled:      true,
			Rate:         0.8, // Sample 80% for demo visibility
			MaxPerSecond: 50,
		},
	})
}

func createObservabilityEnabledStream(ctx context.Context, baseLogger observability.Logger) (*stream.Stream, *observability.StreamLogger) {
	// Create stream logger for this specific stream instance
	streamLogger := observability.NewStreamLogger(baseLogger, "ecommerce-stream")
	
	// Convert observability logger to slog.Logger for go-stream
	slogLogger := convertToSlog(streamLogger.Logger)
	
	// Create stream with observability integration
	s, err := stream.New(ctx, 
		client.WithLogger(slogLogger),
		client.WithHost("127.0.0.1"),
		client.WithPort(0), // dynamic port
	)
	
	if err != nil {
		streamLogger.Error("failed to create stream", observability.ErrorField(err))
		panic(err)
	}
	
	// Log successful stream creation with connection details
	streamLogger.LogConnection(ctx, "embedded://127.0.0.1", true)
	
	return s, streamLogger
}

func demonstrateECommerceFlow(ctx context.Context, s *stream.Stream, streamLogger *observability.StreamLogger) {
	println("\n=== E-Commerce Order Processing Flow ===")
	
	// Set up publishers and subscribers with observability
	setupOrderProcessingPipeline(ctx, s, streamLogger)
	
	// Simulate order flow with different scenarios
	simulateOrderScenarios(ctx, s, streamLogger)
	
	// Wait for processing to complete
	time.Sleep(2 * time.Second)
}

func setupOrderProcessingPipeline(ctx context.Context, s *stream.Stream, streamLogger *observability.StreamLogger) {
	// Order Processing Subscriber
	orderProcessor := &OrderProcessor{
		logger: observability.NewSubscriberLogger(streamLogger, "order-processor"),
		stream: s,
	}
	
	_, err := s.Subscribe("orders.created", orderProcessor, sub.WithQueueGroupName("order-processors"))
	streamLogger.LogSubscribe(ctx, "orders.created", "order-processors", err)
	
	// Payment Processing Subscriber  
	paymentProcessor := &PaymentProcessor{
		logger: observability.NewSubscriberLogger(streamLogger, "payment-processor"),
		stream: s,
	}

	_, err = s.Subscribe("payments.process", paymentProcessor, sub.WithQueueGroupName("payment-processors"))
	streamLogger.LogSubscribe(ctx, "payments.process", "payment-processors", err)
	
	// Notification Subscriber
	notificationService := &NotificationService{
		logger: observability.NewSubscriberLogger(streamLogger, "notification-service"),
	}
	
	_, err = s.Subscribe("orders.completed", notificationService)
	streamLogger.LogSubscribe(ctx, "orders.completed", "", err)
}

func simulateOrderScenarios(ctx context.Context, s *stream.Stream, streamLogger *observability.StreamLogger) {
	// Create publisher logger
	publisherLogger := observability.NewPublisherLogger(streamLogger, "api-gateway")
	
	scenarios := []struct {
		name      string
		requestID string
		userID    string
		amount    float64
		shouldFail bool
	}{
		{"successful-order", "req-001", "user-123", 99.99, false},
		{"high-value-order", "req-002", "user-456", 2500.00, false}, 
		{"failed-payment", "req-003", "user-789", 150.00, true},
		{"quick-order", "req-004", "user-321", 25.50, false},
	}
	
	for _, scenario := range scenarios {
		processOrderScenario(ctx, s, publisherLogger, scenario.name, scenario.requestID, 
			scenario.userID, scenario.amount, scenario.shouldFail)
		time.Sleep(200 * time.Millisecond) // Allow processing
	}
}

func processOrderScenario(ctx context.Context, s *stream.Stream, publisherLogger *observability.PublisherLogger, 
	scenarioName, requestID, userID string, amount float64, shouldFail bool) {
	
	// Add request correlation to context
	ctx = context.WithValue(ctx, "request_id", requestID)
	ctx = context.WithValue(ctx, "user_id", userID)
	ctx = context.WithValue(ctx, "scenario", scenarioName)
	
	orderEvent := OrderEvent{
		OrderID:   fmt.Sprintf("order-%s", requestID[4:]), // order-001, order-002, etc.
		UserID:    userID,
		Amount:    amount,
		Status:    "created",
		Timestamp: time.Now(),
		RequestID: requestID,
	}
	
	data, _ := json.Marshal(orderEvent)
	
	startTime := time.Now()
	err := s.Publish(ctx, "orders.created", stream.Message{Data: data})
	duration := time.Since(startTime)
	
	// Log the publish operation with observability context
	publisherLogger.LogPublish(ctx, "orders.created", len(data), duration, err)
	
	if shouldFail {
		// Simulate a failure scenario for demonstration
		publisherLogger.WithContext(ctx).Warn("order processing expected to fail",
			slog.String("failure_reason", "payment_gateway_unavailable"),
			observability.Operation("order-simulation"),
		)
	}
}

// OrderProcessor handles order creation events
type OrderProcessor struct {
	logger *observability.SubscriberLogger
	stream *stream.Stream
}

func (op *OrderProcessor) Handle(ctx context.Context, msg stream.Message) error {
	startTime := time.Now()
	
	// Extract message info for logging
	var order OrderEvent
	if err := json.Unmarshal(msg.Data, &order); err != nil {
		op.logger.LogMessageProcessed(ctx, "orders.created", "", time.Since(startTime), err)
		return err
	}
	
	// Add order correlation to context
	ctx = context.WithValue(ctx, "request_id", order.RequestID)
	ctx = context.WithValue(ctx, "order_id", order.OrderID)
	
	op.logger.LogMessageReceived(ctx, "orders.created", order.OrderID, len(msg.Data))
	
	// Simulate order validation processing
	time.Sleep(30 * time.Millisecond)
	
	// Validate order (simulate some failing)
	if order.Amount > 2000 {
		op.logger.WithContext(ctx).Warn("high value order requires manual review",
			slog.Float64("amount", order.Amount),
			slog.Float64("threshold", 2000.0),
			observability.Operation("order-validation"),
		)
	}
	
	// Trigger payment processing
	paymentCmd := PaymentCommand{
		OrderID:   order.OrderID,
		Amount:    order.Amount,
		RequestID: order.RequestID,
	}
	
	paymentData, _ := json.Marshal(paymentCmd)
	
	pubStart := time.Now()
	err := op.stream.Publish(ctx, "payments.process", stream.Message{Data: paymentData})
	pubDuration := time.Since(pubStart)
	
	// Log the downstream publish
	pubLogger := observability.NewPublisherLogger(
		&observability.StreamLogger{Logger: op.logger.Logger}, 
		"order-processor",
	)
	pubLogger.LogPublish(ctx, "payments.process", len(paymentData), pubDuration, err)
	
	op.logger.LogMessageProcessed(ctx, "orders.created", order.OrderID, time.Since(startTime), err)
	
	return err
}

// PaymentProcessor handles payment processing
type PaymentProcessor struct {
	logger *observability.SubscriberLogger
	stream *stream.Stream
}

func (pp *PaymentProcessor) Handle(ctx context.Context, msg stream.Message) error {
	startTime := time.Now()
	
	var payment PaymentCommand
	if err := json.Unmarshal(msg.Data, &payment); err != nil {
		pp.logger.LogMessageProcessed(ctx, "payments.process", "", time.Since(startTime), err)
		return err
	}
	
	ctx = context.WithValue(ctx, "request_id", payment.RequestID)
	ctx = context.WithValue(ctx, "order_id", payment.OrderID)
	
	pp.logger.LogMessageReceived(ctx, "payments.process", payment.OrderID, len(msg.Data))
	
	// Simulate payment processing
	time.Sleep(50 * time.Millisecond)
	
	// Simulate payment failure for demonstration
	var err error
	if payment.RequestID == "req-003" {
		err = errors.New("payment_gateway_timeout")
		pp.logger.WithContext(ctx).Error("payment processing failed",
			observability.ErrorField(err),
			slog.Float64("amount", payment.Amount),
			slog.String("gateway", "stripe"),
			observability.Operation("payment-processing"),
		)
	} else {
		pp.logger.WithContext(ctx).Info("payment processed successfully",
			slog.Float64("amount", payment.Amount),
			slog.String("gateway", "stripe"),
			observability.Operation("payment-processing"),
		)
		
		// Trigger order completion
		completionData, _ := json.Marshal(map[string]interface{}{
			"order_id":   payment.OrderID,
			"request_id": payment.RequestID,
			"status":     "completed",
		})
		
		pubStart := time.Now()
		pubErr := pp.stream.Publish(ctx, "orders.completed", stream.Message{Data: completionData})
		pubDuration := time.Since(pubStart)
		
		pubLogger := observability.NewPublisherLogger(
			&observability.StreamLogger{Logger: pp.logger.Logger}, 
			"payment-processor",
		)
		pubLogger.LogPublish(ctx, "orders.completed", len(completionData), pubDuration, pubErr)
	}
	
	pp.logger.LogMessageProcessed(ctx, "payments.process", payment.OrderID, time.Since(startTime), err)
	
	return err
}

// NotificationService handles order completion notifications
type NotificationService struct {
	logger *observability.SubscriberLogger
}

func (ns *NotificationService) Handle(ctx context.Context, msg stream.Message) error {
	startTime := time.Now()
	
	var completion map[string]interface{}
	if err := json.Unmarshal(msg.Data, &completion); err != nil {
		ns.logger.LogMessageProcessed(ctx, "orders.completed", "", time.Since(startTime), err)
		return err
	}
	
	orderID := completion["order_id"].(string)
	requestID := completion["request_id"].(string)
	
	ctx = context.WithValue(ctx, "request_id", requestID)
	ctx = context.WithValue(ctx, "order_id", orderID)
	
	ns.logger.LogMessageReceived(ctx, "orders.completed", orderID, len(msg.Data))
	
	// Simulate notification sending
	time.Sleep(15 * time.Millisecond)
	
	ns.logger.WithContext(ctx).Info("order completion notification sent",
		slog.String("notification_type", "email"),
		slog.String("status", "delivered"),
		observability.Operation("notification-delivery"),
	)
	
	ns.logger.LogMessageProcessed(ctx, "orders.completed", orderID, time.Since(startTime), nil)
	
	return nil
}

// Helper to convert observability logger to slog.Logger
func convertToSlog(obsLogger observability.Logger) *slog.Logger {
	// In a real implementation, you might extract the underlying slog.Logger
	// For this demo, we create a compatible logger
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

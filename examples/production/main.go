package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	stream "github.com/a2y-d5l/go-stream"
	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/observability"
	"github.com/a2y-d5l/go-stream/sub"
)

// OrderEvent represents an order event in the system
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
}

// ProductionStreamService demonstrates a production-ready streaming service
type ProductionStreamService struct {
	stream  *stream.Stream
	logger  observability.Logger
	metrics *observability.StreamMetrics
	server  *http.Server
}

func main() {
	ctx := context.Background()
	
	// Configure structured logging
	logger := observability.NewLogger(observability.LoggerConfig{
		Level:  slog.LevelInfo,
		Format: observability.JSON,
		Output: os.Stdout,
	})
	
	// Configure metrics collection
	metricsCollector := observability.NewInMemoryMetricsCollector()
	if err := metricsCollector.Start(ctx); err != nil {
		log.Fatal("Failed to start metrics collector:", err)
	}
	defer metricsCollector.Stop(ctx)
	
	// Initialize stream with production configuration
	s, err := stream.New(ctx,
		client.WithHost("0.0.0.0"),
		client.WithPort(4222),
		client.WithConnectTimeout(time.Second*10),
		client.WithReconnectWait(time.Second*2),
		client.WithDrainTimeout(time.Second*10),
		client.WithServerReadyTimeout(time.Second*30),
	)
	if err != nil {
		logger.Error("Failed to create stream", slog.String("error", err.Error()))
		os.Exit(1)
	}
	
	// Create service
	service := &ProductionStreamService{
		stream:  s,
		logger:  logger,
		metrics: observability.NewStreamMetrics(metricsCollector),
	}
	
	// Start service
	if err := service.Start(ctx); err != nil {
		logger.Error("Failed to start service", slog.String("error", err.Error()))
		os.Exit(1)
	}
	
	logger.Info("Production streaming service started",
		slog.String("version", "1.0.0"))
	
	// Wait for shutdown signal
	service.WaitForShutdown()
	
	// Graceful shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	if err := service.Shutdown(shutdownCtx); err != nil {
		logger.Error("Failed to shutdown gracefully", slog.String("error", err.Error()))
		os.Exit(1)
	}
	
	logger.Info("Service shutdown complete")
}

// Start initializes and starts the production service
func (s *ProductionStreamService) Start(ctx context.Context) error {
	// Start order event processing
	if err := s.startOrderProcessing(); err != nil {
		return fmt.Errorf("failed to start order processing: %w", err)
	}
	
	// Start HTTP health endpoint
	if err := s.startHTTPServer(); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}
	
	s.logger.Info("All services started successfully")
	return nil
}

// startOrderProcessing sets up order event processing with proper error handling and observability
func (s *ProductionStreamService) startOrderProcessing() error {
	// Subscribe to order events with production-ready configuration
	subscription, err := stream.SubscribeJSON(s.stream, "orders.events", s.handleOrderEvent,
		sub.WithConcurrency(5),
		sub.WithBufferSize(2048),
		sub.WithBackpressure(sub.BackpressureBlock),
		sub.WithMaxDeliver(3),
	)
	if err != nil {
		return fmt.Errorf("failed to subscribe to order events: %w", err)
	}
	
	s.logger.Info("Order event processing started",
		slog.String("topic", "orders.events"),
		slog.String("subscription_id", subscription.ID()))
	
	return nil
}

// handleOrderEvent processes individual order events with comprehensive error handling and metrics
func (s *ProductionStreamService) handleOrderEvent(ctx context.Context, event OrderEvent) error {
	startTime := time.Now()
	
	// Record message received metric
	s.metrics.RecordMessageReceived("orders.events")
	
	// Validate event
	if err := s.validateOrderEvent(&event); err != nil {
		s.logger.Warn("Invalid order event",
			slog.String("error", err.Error()),
			slog.String("order_id", event.OrderID))
		return fmt.Errorf("validation failed: %w", err)
	}
	
	// Process event based on status
	if err := s.processOrderEvent(ctx, &event); err != nil {
		s.logger.Error("Failed to process order event",
			slog.String("error", err.Error()),
			slog.String("order_id", event.OrderID),
			slog.String("status", event.Status))
		return fmt.Errorf("processing failed: %w", err)
	}
	
	// Record processing time
	processingTime := time.Since(startTime)
	s.metrics.RecordMessageProcessingTime("orders.events", processingTime)
	
	s.logger.Info("Order event processed successfully",
		slog.String("order_id", event.OrderID),
		slog.String("status", event.Status),
		slog.Duration("processing_time", processingTime))
	
	return nil
}

// validateOrderEvent validates an order event
func (s *ProductionStreamService) validateOrderEvent(event *OrderEvent) error {
	if event.OrderID == "" {
		return fmt.Errorf("order_id is required")
	}
	if event.CustomerID == "" {
		return fmt.Errorf("customer_id is required")
	}
	if event.Amount < 0 {
		return fmt.Errorf("amount must be non-negative")
	}
	if event.Status == "" {
		return fmt.Errorf("status is required")
	}
	return nil
}

// processOrderEvent processes an order event based on its status
func (s *ProductionStreamService) processOrderEvent(ctx context.Context, event *OrderEvent) error {
	switch event.Status {
	case "created":
		return s.handleOrderCreated(ctx, event)
	case "paid":
		return s.handleOrderPaid(ctx, event)
	case "shipped":
		return s.handleOrderShipped(ctx, event)
	case "delivered":
		return s.handleOrderDelivered(ctx, event)
	case "cancelled":
		return s.handleOrderCancelled(ctx, event)
	default:
		return fmt.Errorf("unknown order status: %s", event.Status)
	}
}

// handleOrderCreated processes order created events
func (s *ProductionStreamService) handleOrderCreated(ctx context.Context, event *OrderEvent) error {
	// Simulate business logic
	s.logger.Info("Processing order creation",
		slog.String("order_id", event.OrderID),
		slog.Float64("amount", event.Amount))
	
	// Publish confirmation event
	confirmationEvent := map[string]interface{}{
		"order_id":    event.OrderID,
		"customer_id": event.CustomerID,
		"status":      "confirmed",
		"timestamp":   time.Now(),
	}
	
	return s.stream.PublishJSON(ctx, "orders.confirmations", confirmationEvent)
}

// handleOrderPaid processes order paid events
func (s *ProductionStreamService) handleOrderPaid(ctx context.Context, event *OrderEvent) error {
	s.logger.Info("Processing order payment",
		slog.String("order_id", event.OrderID),
		slog.Float64("amount", event.Amount))
	
	// Trigger fulfillment process
	fulfillmentEvent := map[string]interface{}{
		"order_id":    event.OrderID,
		"customer_id": event.CustomerID,
		"amount":      event.Amount,
		"action":      "fulfill",
		"timestamp":   time.Now(),
	}
	
	return s.stream.PublishJSON(ctx, "fulfillment.requests", fulfillmentEvent)
}

// handleOrderShipped processes order shipped events
func (s *ProductionStreamService) handleOrderShipped(ctx context.Context, event *OrderEvent) error {
	s.logger.Info("Processing order shipment",
		slog.String("order_id", event.OrderID))
	
	// Send notification
	notificationEvent := map[string]interface{}{
		"customer_id": event.CustomerID,
		"order_id":    event.OrderID,
		"type":        "shipment_notification",
		"message":     "Your order has been shipped",
		"timestamp":   time.Now(),
	}
	
	return s.stream.PublishJSON(ctx, "notifications.customer", notificationEvent)
}

// handleOrderDelivered processes order delivered events
func (s *ProductionStreamService) handleOrderDelivered(ctx context.Context, event *OrderEvent) error {
	s.logger.Info("Processing order delivery",
		slog.String("order_id", event.OrderID))
	
	// Complete order and request feedback
	completionEvent := map[string]interface{}{
		"order_id":    event.OrderID,
		"customer_id": event.CustomerID,
		"status":      "completed",
		"timestamp":   time.Now(),
	}
	
	return s.stream.PublishJSON(ctx, "orders.completions", completionEvent)
}

// handleOrderCancelled processes order cancelled events
func (s *ProductionStreamService) handleOrderCancelled(ctx context.Context, event *OrderEvent) error {
	s.logger.Info("Processing order cancellation",
		slog.String("order_id", event.OrderID))
	
	// Trigger refund process
	refundEvent := map[string]interface{}{
		"order_id":    event.OrderID,
		"customer_id": event.CustomerID,
		"amount":      event.Amount,
		"reason":      "order_cancelled",
		"timestamp":   time.Now(),
	}
	
	return s.stream.PublishJSON(ctx, "payments.refunds", refundEvent)
}

// startHTTPServer starts the HTTP server for health checks and metrics
func (s *ProductionStreamService) startHTTPServer() error {
	mux := http.NewServeMux()
	
	// Health check endpoint
	mux.HandleFunc("/health", s.handleHealthCheck)
	
	// Metrics endpoint
	mux.HandleFunc("/metrics", s.handleMetrics)
	
	// Readiness check endpoint
	mux.HandleFunc("/ready", s.handleReadinessCheck)
	
	s.server = &http.Server{
		Addr:         ":8080",
		Handler:      mux,
		ReadTimeout:  time.Second * 30,
		WriteTimeout: time.Second * 30,
		IdleTimeout:  time.Second * 60,
	}
	
	go func() {
		s.logger.Info("HTTP server starting", slog.String("addr", ":8080"))
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", slog.String("error", err.Error()))
		}
	}()
	
	return nil
}

// handleHealthCheck provides a health check endpoint
func (s *ProductionStreamService) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := s.stream.Healthy(ctx); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "unhealthy",
			"reason": err.Error(),
		})
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// handleMetrics provides a metrics endpoint
func (s *ProductionStreamService) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := observability.GetDefaultMetricsCollector().GetMetrics()
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"metrics": metrics,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// handleReadinessCheck provides a readiness check endpoint
func (s *ProductionStreamService) handleReadinessCheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	err := s.stream.Healthy(ctx)
	ready := err == nil
	
	if !ready {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "not_ready",
			"reason": err.Error(),
		})
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ready",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// WaitForShutdown waits for interrupt signals
func (s *ProductionStreamService) WaitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	sig := <-sigChan
	s.logger.Info("Received shutdown signal", slog.String("signal", sig.String()))
}

// Shutdown gracefully shuts down the service
func (s *ProductionStreamService) Shutdown(ctx context.Context) error {
	s.logger.Info("Starting graceful shutdown")
	
	// Stop HTTP server
	if s.server != nil {
		if err := s.server.Shutdown(ctx); err != nil {
			s.logger.Error("HTTP server shutdown error", slog.String("error", err.Error()))
		} else {
			s.logger.Info("HTTP server stopped")
		}
	}
	
	// Close stream connection
	if err := s.stream.Close(ctx); err != nil {
		s.logger.Error("Stream close error", slog.String("error", err.Error()))
		return err
	}
	
	s.logger.Info("Stream connection closed")
	return nil
}

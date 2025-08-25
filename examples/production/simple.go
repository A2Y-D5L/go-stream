package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	stream "github.com/a2y-d5l/go-stream"
	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/observability"
)

// OrderEvent represents an order event in the system
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Amount     float64   `json:"amount"`
	Status     string    `json:"status"`
	Timestamp  time.Time `json:"timestamp"`
}

// ProductionExample demonstrates production patterns
func main() {
	ctx := context.Background()
	
	// Configure structured logging
	logger := observability.NewLogger(observability.LoggerConfig{
		Level:  slog.LevelInfo,
		Format: observability.JSON,
		Output: os.Stdout,
	})
	
	// Configure metrics
	metricsCollector := observability.NewInMemoryMetricsCollector()
	metricsCollector.Start(ctx)
	defer metricsCollector.Stop(ctx)
	
	// Initialize stream with production settings
	s, err := stream.New(ctx,
		client.WithHost("0.0.0.0"),
		client.WithPort(4222),
		client.WithConnectTimeout(time.Second*10),
		client.WithReconnectWait(time.Second*2),
	)
	if err != nil {
		logger.Error("Failed to create stream", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer s.Close(ctx)
	
	logger.Info("Production streaming service started")
	
	// Start HTTP server for monitoring
	startHTTPServer(s, logger, metricsCollector)
	
	// Set up order processing
	subscription, err := stream.SubscribeJSON(s, "orders.events", func(ctx context.Context, event OrderEvent) error {
		metrics := observability.NewStreamMetrics(metricsCollector)
		metrics.RecordMessageReceived("orders.events")
		
		logger.Info("Processing order event",
			slog.String("order_id", event.OrderID),
			slog.String("status", event.Status),
			slog.Float64("amount", event.Amount))
		
		// Simulate processing based on status
		switch event.Status {
		case "created":
			confirmationEvent := map[string]interface{}{
				"order_id":    event.OrderID,
				"customer_id": event.CustomerID,
				"status":      "confirmed",
				"timestamp":   time.Now(),
			}
			logger.Info("Publishing order confirmation")
			
			// For demo purposes, just log the publication
			logger.Info("Would publish to orders.confirmations", 
				slog.Any("event", confirmationEvent))
			
		case "paid":
			logger.Info("Order payment processed, would trigger fulfillment")
		case "shipped":
			logger.Info("Order shipped, would send customer notification")
		case "delivered":
			logger.Info("Order delivered, would complete order")
		case "cancelled":
			logger.Info("Order cancelled, would process refund")
		}
		
		return nil
	})
	
	if err != nil {
		logger.Error("Failed to subscribe", slog.String("error", err.Error()))
		os.Exit(1)
	}
	
	logger.Info("Order processing started")
	
	// Simulate order events for demonstration
	go simulateOrderEvents(s, logger)
	
	// Wait for shutdown
	waitForShutdown(logger)
	
	logger.Info("Shutting down")
	_ = subscription
}

func simulateOrderEvents(s *stream.Stream, logger observability.Logger) {
	ctx := context.Background()
	
	// Wait a moment for setup
	time.Sleep(2 * time.Second)
	
	events := []OrderEvent{
		{
			OrderID:    "order-001",
			CustomerID: "customer-123",
			Amount:     99.99,
			Status:     "created",
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "order-001",
			CustomerID: "customer-123",
			Amount:     99.99,
			Status:     "paid",
			Timestamp:  time.Now(),
		},
		{
			OrderID:    "order-001",
			CustomerID: "customer-123",
			Amount:     99.99,
			Status:     "shipped",
			Timestamp:  time.Now(),
		},
	}
	
	for _, event := range events {
		logger.Info("Publishing order event", 
			slog.String("order_id", event.OrderID),
			slog.String("status", event.Status))
		
		// For now, we'll use a simple message format since the advanced methods may not be available
		data, _ := json.Marshal(event)
		msg := stream.Message{
			Topic: "orders.events",
			Data:  data,
			ID:    fmt.Sprintf("msg-%d", time.Now().UnixNano()),
			Time:  time.Now(),
		}
		
		// Simulate publishing (actual implementation would use s.Publish)
		logger.Info("Simulated publish", slog.Any("message", msg))
		
		time.Sleep(2 * time.Second)
	}
}

func startHTTPServer(s *stream.Stream, logger observability.Logger, collector observability.MetricsCollector) {
	mux := http.NewServeMux()
	
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		if err := s.Healthy(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"status": "unhealthy",
				"reason": err.Error(),
			})
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"status":    "healthy",
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})
	
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		metrics := collector.GetMetrics()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"metrics":   metrics,
			"timestamp": time.Now().Format(time.RFC3339),
		})
	})
	
	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}
	
	go func() {
		logger.Info("HTTP server starting on :8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", slog.String("error", err.Error()))
		}
	}()
}

func waitForShutdown(logger observability.Logger) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	
	sig := <-sigChan
	logger.Info("Received shutdown signal", slog.String("signal", sig.String()))
}

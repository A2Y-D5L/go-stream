package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	stream "github.com/a2y-d5l/go-stream"
)

type OrderEvent struct {
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Status    string    `json:"status"` // "created", "paid", "shipped", "delivered", "cancelled"
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
}

type User struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	Verified bool   `json:"verified"`
}

func main() {
	// Initialize stream with JetStream enabled and persistent storage
	s, err := stream.New(context.Background(),
		stream.WithDefaultTopicMode(stream.TopicModeJetStream),
		stream.WithStoreDir("./jetstream-store"), // Persistent storage
	)
	if err != nil {
		log.Fatal("Failed to create stream:", err)
	}
	defer func() {
		if err := s.Close(context.Background()); err != nil {
			log.Printf("❌ Close error: %v", err)
		}
	}()

	fmt.Println("JetStream Durable Messaging Example")
	fmt.Println("This example demonstrates:")
	fmt.Println("- Durable message storage and replay")
	fmt.Println("- Consumer acknowledgments")
	fmt.Println("- Stream persistence across restarts")
	fmt.Println("- Different delivery semantics")
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()

	// Start order processing service (durable consumer)
	go runOrderProcessor(s)
	
	// Start user verification service (ephemeral consumer)
	go runUserVerificationService(s)
	
	// Start analytics service (replay all messages)
	go runAnalyticsService(s)

	// Start order event publisher
	go runOrderEventPublisher(s)
	
	// Start user registration publisher
	go runUserRegistrationPublisher(s)

	// Wait for interrupt
	waitForShutdown()
}

func runOrderProcessor(s *stream.Stream) {
	fmt.Println("📦 Starting Order Processing Service (durable consumer)")
	
	// Durable consumer - will resume from where it left off even after restart
	sub, err := stream.SubscribeJSON(s, "orders.events",
		func(ctx context.Context, order OrderEvent) error {
			// Simulate order processing
			time.Sleep(500 * time.Millisecond)
			
			fmt.Printf("📦 Processing order: %s (status: %s, amount: $%.2f)\n",
				order.OrderID, order.Status, order.Amount)
			
			// Simulate processing based on status
			switch order.Status {
			case "created":
				fmt.Printf("   ✅ Order %s validated and ready for payment\n", order.OrderID)
			case "paid":
				fmt.Printf("   💳 Payment processed for order %s\n", order.OrderID)
			case "shipped":
				fmt.Printf("   🚚 Order %s shipped to customer\n", order.OrderID)
			case "delivered":
				fmt.Printf("   📦 Order %s delivered successfully\n", order.OrderID)
			case "cancelled":
				fmt.Printf("   ❌ Order %s cancelled\n", order.OrderID)
			}
			
			return nil // Acknowledge the message
		},
		stream.WithDurable("order-processor"),     // Durable consumer name
		stream.WithAckPolicy(stream.AckManual),    // Manual acknowledgment
		stream.WithMaxDeliver(3),                  // Retry up to 3 times
		stream.WithStartAt(stream.DeliverNew),     // Only new messages (change to DeliverByStartTime for replay)
	)
	
	if err != nil {
		log.Printf("❌ Order processor subscribe error: %v", err)
		return
	}
	defer sub.Stop()
	
	// Keep the service running
	select {}
}

func runUserVerificationService(s *stream.Stream) {
	fmt.Println("👤 Starting User Verification Service")
	
	// Regular consumer for user events
	sub, err := stream.SubscribeJSON(s, "users.registrations",
		func(ctx context.Context, user User) error {
			// Simulate verification process
			time.Sleep(200 * time.Millisecond)
			
			if user.Verified {
				fmt.Printf("👤 ✅ User %s (%s) verification confirmed\n", user.Name, user.Email)
			} else {
				fmt.Printf("👤 📧 Sending verification email to %s (%s)\n", user.Name, user.Email)
				// In real world, would send verification email here
			}
			
			return nil
		},
		stream.WithConcurrency(2), // Process 2 users concurrently
	)
	
	if err != nil {
		log.Printf("❌ User verification subscribe error: %v", err)
		return
	}
	defer sub.Stop()
	
	// Keep the service running
	select {}
}

func runAnalyticsService(s *stream.Stream) {
	// Wait a bit before starting analytics to let some messages accumulate
	time.Sleep(3 * time.Second)
	
	fmt.Println("📊 Starting Analytics Service (replaying historical data)")
	
	// Analytics service that processes all historical order events
	startTime := time.Now().Add(-1 * time.Hour) // Replay from 1 hour ago
	
	sub, err := stream.SubscribeJSON(s, "orders.events",
		func(ctx context.Context, order OrderEvent) error {
			// Simple analytics - just count orders by status
			fmt.Printf("📊 Analytics: Order %s processed (status: %s, timestamp: %s)\n",
				order.OrderID, order.Status, order.Timestamp.Format("15:04:05"))
			
			return nil
		},
		stream.WithDurable("analytics-processor"),           // Durable for analytics
		stream.WithStartAt(stream.DeliverByStartTime),   // Replay from specific time
		stream.WithStartTime(startTime),                 // Start time for replay
		stream.WithConcurrency(1),                       // Sequential processing for analytics
	)
	
	if err != nil {
		log.Printf("❌ Analytics subscribe error: %v", err)
		return
	}
	defer sub.Stop()
	
	// Keep the service running
	select {}
}

func runOrderEventPublisher(s *stream.Stream) {
	fmt.Println("📤 Starting Order Event Publisher")
	
	orderStatuses := []string{"created", "paid", "shipped", "delivered", "cancelled"}
	orderID := 1
	
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		order := OrderEvent{
			OrderID:   fmt.Sprintf("order-%d", orderID),
			UserID:    fmt.Sprintf("user-%d", (orderID%10)+1),
			Status:    orderStatuses[orderID%len(orderStatuses)],
			Amount:    float64((orderID%100)+10) * 1.99, // Random amount
			Timestamp: time.Now(),
		}
		
		ctx := context.Background()
		err := s.PublishJSON(ctx, "orders.events", order)
		if err != nil {
			log.Printf("❌ Failed to publish order event: %v", err)
		} else {
			fmt.Printf("📤 Published order event: %s (status: %s)\n", order.OrderID, order.Status)
		}
		
		orderID++
	}
}

func runUserRegistrationPublisher(s *stream.Stream) {
	fmt.Println("📤 Starting User Registration Publisher")
	
	userID := 1
	
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		user := User{
			ID:       fmt.Sprintf("user-%d", userID),
			Name:     fmt.Sprintf("User %d", userID),
			Email:    fmt.Sprintf("user%d@example.com", userID),
			Verified: userID%3 == 0, // Every 3rd user is pre-verified
		}
		
		ctx := context.Background()
		err := s.PublishJSON(ctx, "users.registrations", user)
		if err != nil {
			log.Printf("❌ Failed to publish user registration: %v", err)
		} else {
			verificationStatus := "needs verification"
			if user.Verified {
				verificationStatus = "pre-verified"
			}
			fmt.Printf("📤 Published user registration: %s (%s)\n", user.Name, verificationStatus)
		}
		
		userID++
	}
}

func waitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("\n🔄 Shutting down gracefully...")
	fmt.Println("💾 JetStream data persisted to: ./jetstream-store")
	fmt.Println("🔄 Restart the application to see durable consumers resume from where they left off")
}

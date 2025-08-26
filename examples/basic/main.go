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

type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	// Initialize stream with default configuration
	s, err := stream.New(context.Background())
	if err != nil {
		log.Fatal("Failed to create stream:", err)
	}
	defer func() {
		if err := s.Close(context.Background()); err != nil {
			log.Printf("❌ Close error: %v", err)
		}
	}()

	fmt.Println("Basic pub/sub example started")
	fmt.Println("Press Ctrl+C to stop")

	// Start subscriber
	go runSubscriber(s)

	// Start publisher
	go runPublisher(s)

	// Wait for interrupt
	waitForShutdown()
}

func runSubscriber(s *stream.Stream) {
	fmt.Println("Starting subscriber for user.events...")

	sub, err := stream.SubscribeJSON(s, "user.events", func(ctx context.Context, user User) error {
		fmt.Printf("📨 Received user: ID=%s, Name=%s, Email=%s\n",
			user.ID, user.Name, user.Email)
		return nil
	})
	if err != nil {
		log.Printf("❌ Subscribe error: %v", err)
		return
	}
	defer sub.Stop()
}

func runPublisher(s *stream.Stream) {
	fmt.Println("Starting publisher...")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	userID := 1
	for range ticker.C {
		user := User{
			ID:    fmt.Sprintf("user-%d", userID),
			Name:  fmt.Sprintf("User %d", userID),
			Email: fmt.Sprintf("user%d@example.com", userID),
		}

		ctx := context.Background()
		err := s.PublishJSON(ctx, "user.events", user)
		if err != nil {
			log.Printf("❌ Publish error: %v", err)
		} else {
			fmt.Printf("📤 Published user: %s\n", user.ID)
		}
		userID++
	}
}

func waitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("\n🔄 Shutting down gracefully...")
}

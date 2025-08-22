package main

import (
	"context"
	"fmt"
	"time"

	"github.com/a2y-d5l/go-stream"
)

func main() {
	fmt.Println("Starting embedded NATS server test...")
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	s, err := stream.New(ctx)
	if err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}
	
	fmt.Printf("Server started successfully on port: %d\n", s.Port())
	
	// Test if it's healthy
	err = s.Healthy(ctx)
	if err != nil {
		fmt.Printf("Server not healthy: %v\n", err)
	} else {
		fmt.Println("Server is healthy!")
	}
	
	// Close the server
	err = s.Close(context.Background())
	if err != nil {
		fmt.Printf("Error closing server: %v\n", err)
	} else {
		fmt.Println("Server closed successfully")
	}
}

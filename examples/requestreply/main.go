package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	stream "github.com/a2y-d5l/go-stream"
)

type CalculationRequest struct {
	Operation string  `json:"operation"` // "add", "multiply", "divide"
	A         float64 `json:"a"`
	B         float64 `json:"b"`
	RequestID string  `json:"request_id"`
}

type CalculationResponse struct {
	Result    float64 `json:"result"`
	Error     string  `json:"error,omitempty"`
	RequestID string  `json:"request_id"`
	ServerID  string  `json:"server_id"`
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

	fmt.Println("Request/Reply example started")
	fmt.Println("This example demonstrates:")
	fmt.Println("- Multiple calculation servers handling requests")
	fmt.Println("- Load balancing across servers")
	fmt.Println("- Request/reply pattern with timeouts")
	fmt.Println("Press Ctrl+C to stop")
	fmt.Println()

	// Start calculation servers (multiple for load balancing)
	for i := 1; i <= 3; i++ {
		go runCalculationServer(s, fmt.Sprintf("calc-server-%d", i))
	}

	// Start client making requests
	go runClient(s)

	// Wait for interrupt
	waitForShutdown()
}

func runCalculationServer(s *stream.Stream, serverID string) {
	fmt.Printf("🖥️  Starting calculation server: %s\n", serverID)

	// Subscribe to calculation requests with queue group for load balancing
	sub, err := stream.SubscribeJSON(s, "calc.requests",
		func(ctx context.Context, req CalculationRequest) error {
			response := processCalculation(req, serverID)

			// Send response back to the reply topic
			replyTopic := stream.Topic(fmt.Sprintf("calc.responses.%s", req.RequestID))
			if err := s.PublishJSON(ctx, replyTopic, response); err != nil {
				log.Printf("❌ [%s] Failed to send response: %v", serverID, err)
				return err
			}

			fmt.Printf("🔢 [%s] Processed: %.2f %s %.2f = %.2f (req: %s)\n",
				serverID, req.A, req.Operation, req.B, response.Result, req.RequestID)
			return nil
		},
		stream.WithQueueGroupName("calc-servers"), // Load balancing
		stream.WithConcurrency(2),                 // Process 2 requests concurrently per server
	)
	if err != nil {
		log.Printf("❌ [%s] Subscribe error: %v", serverID, err)
		return
	}
	defer sub.Stop()

	// Keep the server running
	select {}
}

func processCalculation(req CalculationRequest, serverID string) CalculationResponse {
	response := CalculationResponse{
		RequestID: req.RequestID,
		ServerID:  serverID,
	}

	// Simulate some processing time
	time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

	switch strings.ToLower(req.Operation) {
	case "add":
		response.Result = req.A + req.B
	case "multiply":
		response.Result = req.A * req.B
	case "divide":
		if req.B == 0 {
			response.Error = "division by zero"
		} else {
			response.Result = req.A / req.B
		}
	default:
		response.Error = fmt.Sprintf("unknown operation: %s", req.Operation)
	}

	return response
}

func runClient(s *stream.Stream) {
	fmt.Println("📞 Starting client...")

	operations := []string{"add", "multiply", "divide"}
	requestID := 1

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		// Create random calculation request
		op := operations[rand.Intn(len(operations))]
		a := rand.Float64() * 100
		b := rand.Float64() * 10
		if op == "divide" && b == 0 {
			b = 1 // Avoid division by zero
		}

		reqID := fmt.Sprintf("req-%d", requestID)
		request := CalculationRequest{
			Operation: op,
			A:         a,
			B:         b,
			RequestID: reqID,
		}

		// Send request and wait for response
		go sendRequestAndWaitForResponse(s, request)

		requestID++
	}
}

func sendRequestAndWaitForResponse(s *stream.Stream, req CalculationRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Subscribe to the response topic before sending request
	responseTopic := stream.Topic(fmt.Sprintf("calc.responses.%s", req.RequestID))

	responseChan := make(chan CalculationResponse, 1)
	sub, err := stream.SubscribeJSON(s, responseTopic,
		func(ctx context.Context, resp CalculationResponse) error {
			responseChan <- resp
			return nil
		},
	)
	if err != nil {
		log.Printf("❌ Failed to subscribe to response topic: %v", err)
		return
	}
	defer sub.Stop()

	// Send the request
	fmt.Printf("📤 Sending request: %.2f %s %.2f (req: %s)\n",
		req.A, req.Operation, req.B, req.RequestID)

	if err := s.PublishJSON(ctx, "calc.requests", req); err != nil {
		log.Printf("❌ Failed to send request: %v", err)
		return
	}

	// Wait for response or timeout
	select {
	case response := <-responseChan:
		if response.Error != "" {
			fmt.Printf("❌ Response: %s (server: %s, req: %s)\n",
				response.Error, response.ServerID, response.RequestID)
		} else {
			fmt.Printf("✅ Response: %.2f (server: %s, req: %s)\n",
				response.Result, response.ServerID, response.RequestID)
		}
	case <-ctx.Done():
		fmt.Printf("⏰ Request timeout (req: %s)\n", req.RequestID)
	}
}

func waitForShutdown() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("\n🔄 Shutting down gracefully...")
}

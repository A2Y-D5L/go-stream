package stream

import (
	"context"
	"fmt"
	"testing"
	"time"

	nserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestNATSServerBasic(t *testing.T) {
	t.Run("basic NATS server startup", func(t *testing.T) {
		fmt.Println("Creating NATS server options...")
		
		// Very basic server options
		opts := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   -1, // dynamic port
			NoSigs: true,
			Debug:  false,  // Disable debug to reduce noise
			Trace:  false,  // Disable trace to reduce noise
		}
		
		fmt.Println("Creating NATS server...")
		server, err := nserver.NewServer(opts)
		if err != nil {
			t.Fatalf("Failed to create server: %v", err)
		}
		
		fmt.Println("Starting NATS server...")
		go server.Start()
		
		// Give it a moment to start
		time.Sleep(1 * time.Second)
		
		fmt.Printf("Server running: %v\n", server.Running())
		fmt.Printf("Server addr: %v\n", server.Addr())
		fmt.Printf("Server client URL: %s\n", server.ClientURL())
		
		// Try to connect directly instead of relying on ReadyForConnections
		fmt.Println("Attempting to connect as a client...")
		clientURL := server.ClientURL()
		
		// Try connecting with retries
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		var nc *nats.Conn
		connected := false
		
		for attempts := 0; attempts < 20 && !connected; attempts++ {
			select {
			case <-ctx.Done():
				fmt.Printf("Connection timeout: %v\n", ctx.Err())
				server.Shutdown()
				server.WaitForShutdown()
				t.Fatalf("Connection timeout: %v", ctx.Err())
			default:
				nc, err = nats.Connect(clientURL, nats.Timeout(500*time.Millisecond))
				if err != nil {
					fmt.Printf("Connection attempt %d failed: %v\n", attempts+1, err)
					time.Sleep(500 * time.Millisecond)
					continue
				}
				connected = true
				fmt.Printf("Successfully connected on attempt %d!\n", attempts+1)
			}
		}
		
		if connected {
			fmt.Printf("Connection successful! Server URL: %s\n", nc.ConnectedUrl())
			nc.Close()
			fmt.Println("Successfully connected to and disconnected from NATS server")
		}
		
		server.Shutdown()
		server.WaitForShutdown()
		
		if !connected {
			t.Fatalf("Failed to connect to NATS server")
		}
	})
}

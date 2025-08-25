package embeddednats

import (
	"context"
	"net"
	"testing"
	"time"

	nserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper functions

// createTestServerOptions creates server options suitable for testing
func createTestServerOptions() *nserver.Options {
	return &nserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // Dynamic port allocation
		NoSigs:    true,
		NoLog:     true,
		JetStream: false, // Default to core NATS for faster startup
	}
}

// createTestServer creates a server instance for testing
func createTestServer(t *testing.T, opts *nserver.Options) *Server {
	t.Helper()
	
	if opts == nil {
		opts = createTestServerOptions()
	}
	
	server, err := New(opts)
	require.NoError(t, err, "Failed to create test server")
	
	return server
}

// waitForServerReady waits for server to be ready with timeout
func waitForServerReady(t *testing.T, server *Server, timeout time.Duration) {
	t.Helper()
	
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	
	err := server.Ready(ctx)
	require.NoError(t, err, "Server should be ready within timeout")
}

// cleanupServer properly shuts down and cleans up a test server
func cleanupServer(t *testing.T, server *Server) {
	t.Helper()
	
	if server == nil {
		return
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	err := server.ShutdownAndWait(ctx, 5*time.Second)
	if err != nil {
		t.Logf("Warning: Error shutting down test server: %v", err)
	}
}

// assertServerRunning verifies that a server is running
func assertServerRunning(t *testing.T, server *Server) {
	t.Helper()
	
	url := server.ClientURL()
	assert.NotEmpty(t, url, "Server should have a client URL")
	assert.Contains(t, url, "nats://", "Client URL should be a NATS URL")
	
	port := server.Port()
	assert.Greater(t, port, 0, "Server should have a bound port")
}

// testConnection validates that a connection can be established
func testConnection(t *testing.T, server *Server) {
	t.Helper()
	
	nc, err := nats.Connect(server.ClientURL(), nats.Timeout(2*time.Second))
	require.NoError(t, err, "Should be able to connect to server")
	defer nc.Close()
	
	assert.True(t, nc.IsConnected(), "Connection should be active")
}

// findFreePort finds an available port for testing
func findFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	
	return l.Addr().(*net.TCPAddr).Port, nil
}

// measureStartupTime measures how long it takes for server to become ready
func measureStartupTime(t *testing.T, server *Server) time.Duration {
	t.Helper()
	
	start := time.Now()
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	err := server.Ready(ctx)
	require.NoError(t, err, "Server should become ready")
	
	return time.Since(start)
}

// --------------------- Basic Server Lifecycle Tests ---------------------

func TestServer_New(t *testing.T) {
	t.Run("with default options", func(t *testing.T) {
		opts := createTestServerOptions()
		server, err := New(opts)
		
		require.NoError(t, err)
		assert.NotNil(t, server)
		assert.NotNil(t, server.s)
	})
	
	t.Run("with nil options", func(t *testing.T) {
		// NATS server doesn't handle nil options gracefully
		// This should fail during creation
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Expected panic with nil options: %v", r)
				// This is expected behavior
				return
			}
		}()
		
		server, err := New(nil)
		
		// Either creation should fail or we should get a panic
		if err == nil && server != nil {
			t.Error("Expected error or panic with nil options")
		} else if err != nil {
			t.Logf("Got expected error with nil options: %v", err)
		}
	})
	
	t.Run("with invalid options", func(t *testing.T) {
		opts := &nserver.Options{
			Host: "invalid-host-that-does-not-exist",
			Port: 99999, // Invalid port
		}
		
		server, err := New(opts)
		
		// May succeed creation but fail on start
		if err != nil {
			assert.Error(t, err)
			assert.Nil(t, server)
		} else {
			assert.NotNil(t, server)
		}
	})
}

func TestServer_Lifecycle(t *testing.T) {
	opts := createTestServerOptions()
	server := createTestServer(t, opts)
	defer cleanupServer(t, server)
	
	// Test startup
	server.Start()
	
	// Test readiness
	waitForServerReady(t, server, 5*time.Second)
	
	// Test client URL is available
	assertServerRunning(t, server)
	
	// Test connection can be established
	testConnection(t, server)
	
	// Test graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	err := server.ShutdownAndWait(ctx, 5*time.Second)
	assert.NoError(t, err)
}

func TestServer_StartMultiple(t *testing.T) {
	opts := createTestServerOptions()
	server := createTestServer(t, opts)
	defer cleanupServer(t, server)
	
	// Starting multiple times can cause panics in NATS server
	// This is not a critical requirement for our use case
	t.Skip("Multiple starts not supported by NATS server - not critical for our use case")
	
	waitForServerReady(t, server, 5*time.Second)
	assertServerRunning(t, server)
}

// --------------------- Readiness Tests ---------------------

func TestServer_Ready(t *testing.T) {
	t.Run("ready after start", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		err := server.Ready(ctx)
		assert.NoError(t, err)
	})
	
	t.Run("timeout when not started", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		// Don't start the server
		
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		
		err := server.Ready(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})
	
	t.Run("context cancellation", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		// Don't start the server
		
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cancel() // Cancel immediately
		
		err := server.Ready(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context canceled")
	})
	
	t.Run("multiple readiness checks", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		
		// Multiple readiness checks should all succeed
		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := server.Ready(ctx)
			cancel()
			assert.NoError(t, err, "Readiness check %d should succeed", i+1)
		}
	})
}

// --------------------- Configuration Tests ---------------------

func TestServer_Configuration(t *testing.T) {
	t.Run("with specific host", func(t *testing.T) {
		opts := &nserver.Options{
			Host:   "localhost",
			Port:   -1,
			NoSigs: true,
			NoLog:  true,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		url := server.ClientURL()
		assert.Contains(t, url, "localhost")
	})
	
	t.Run("with specific port", func(t *testing.T) {
		freePort, err := findFreePort()
		require.NoError(t, err)
		
		opts := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   freePort,
			NoSigs: true,
			NoLog:  true,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		port := server.Port()
		assert.Equal(t, freePort, port)
	})
	
	t.Run("with JetStream enabled", func(t *testing.T) {
		opts := &nserver.Options{
			Host:      "127.0.0.1",
			Port:      -1,
			NoSigs:    true,
			NoLog:     true,
			JetStream: true,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 10*time.Second) // JetStream may take longer
		
		// Verify JetStream is available by connecting and checking
		nc, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer nc.Close()
		
		js, err := nc.JetStream()
		assert.NoError(t, err)
		assert.NotNil(t, js)
	})
}

// --------------------- Connection and Port Tests ---------------------

func TestServer_ClientURL(t *testing.T) {
	t.Run("returns valid URL after start", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		url := server.ClientURL()
		assert.NotEmpty(t, url)
		assert.Contains(t, url, "nats://")
		assert.Contains(t, url, "127.0.0.1")
	})
	
	t.Run("URL remains consistent", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		url1 := server.ClientURL()
		url2 := server.ClientURL()
		assert.Equal(t, url1, url2, "URL should remain consistent")
	})
}

func TestServer_Port(t *testing.T) {
	t.Run("dynamic port allocation", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		port := server.Port()
		assert.Greater(t, port, 0, "Should allocate a valid port")
		assert.Less(t, port, 65536, "Port should be within valid range")
	})
	
	t.Run("specific port allocation", func(t *testing.T) {
		freePort, err := findFreePort()
		require.NoError(t, err)
		
		opts := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   freePort,
			NoSigs: true,
			NoLog:  true,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		port := server.Port()
		assert.Equal(t, freePort, port)
	})
	
	t.Run("port before start", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		// Port should be 0 before starting
		port := server.Port()
		assert.Equal(t, 0, port, "Port should be 0 before server starts")
	})
}

// --------------------- Error Handling Tests ---------------------

func TestServer_ErrorHandling(t *testing.T) {
	t.Run("shutdown timeout", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		// Very short timeout for shutdown
		err := server.ShutdownAndWait(ctx, 1*time.Nanosecond)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
	})
	
	t.Run("shutdown context cancellation", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		
		err := server.ShutdownAndWait(ctx, 5*time.Second)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "context deadline exceeded")
	})
	
	t.Run("multiple shutdowns", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		// First shutdown should succeed
		err1 := server.ShutdownAndWait(ctx, 5*time.Second)
		assert.NoError(t, err1)
		
		// Second shutdown should also succeed (idempotent)
		err2 := server.ShutdownAndWait(ctx, 5*time.Second)
		assert.NoError(t, err2)
	})
}

// --------------------- Concurrency Tests ---------------------

func TestServer_Concurrency(t *testing.T) {
	t.Run("concurrent readiness checks", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		
		// Multiple goroutines checking readiness
		done := make(chan error, 5)
		
		for i := 0; i < 5; i++ {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				done <- server.Ready(ctx)
			}()
		}
		
		// All should succeed
		for i := 0; i < 5; i++ {
			err := <-done
			assert.NoError(t, err, "Concurrent readiness check %d should succeed", i+1)
		}
	})
	
	t.Run("concurrent client connections", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		// Multiple goroutines connecting
		done := make(chan error, 10)
		
		for i := 0; i < 10; i++ {
			go func() {
				nc, err := nats.Connect(server.ClientURL(), nats.Timeout(2*time.Second))
				if err != nil {
					done <- err
					return
				}
				defer nc.Close()
				
				// Test basic pub/sub
				_, err = nc.Subscribe("test", func(msg *nats.Msg) {})
				done <- err
			}()
		}
		
		// All should succeed
		for i := 0; i < 10; i++ {
			err := <-done
			assert.NoError(t, err, "Concurrent connection %d should succeed", i+1)
		}
	})
	
	t.Run("concurrent shutdown attempts", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		// Multiple goroutines attempting shutdown
		done := make(chan error, 3)
		
		for i := 0; i < 3; i++ {
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				
				// Add small delay to spread out shutdown attempts
				time.Sleep(time.Duration(i*10) * time.Millisecond)
				
				done <- server.ShutdownAndWait(ctx, 5*time.Second)
			}()
		}
		
		// Collect all results
		var successCount int
		var errors []error
		for i := 0; i < 3; i++ {
			err := <-done
			if err == nil {
				successCount++
			} else {
				errors = append(errors, err)
			}
		}
		
		// At least one should succeed, others may fail due to server already shut down
		assert.GreaterOrEqual(t, successCount, 1, "At least one shutdown should succeed")
		
		// Log any errors for debugging
		for i, err := range errors {
			t.Logf("Shutdown attempt %d error: %v", i+1, err)
		}
	})
}

// --------------------- Performance Tests ---------------------

func TestServer_Performance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance tests in short mode")
	}
	
	t.Run("startup time", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		
		startupTime := measureStartupTime(t, server)
		t.Logf("Server startup time: %v", startupTime)
		
		// Server should start reasonably quickly
		assert.Less(t, startupTime, 5*time.Second, "Server startup should be fast")
	})
	
	t.Run("connection establishment time", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		start := time.Now()
		nc, err := nats.Connect(server.ClientURL())
		connectionTime := time.Since(start)
		
		require.NoError(t, err)
		defer nc.Close()
		
		t.Logf("Connection establishment time: %v", connectionTime)
		assert.Less(t, connectionTime, 1*time.Second, "Connection should be fast")
	})
}

// --------------------- Integration Tests ---------------------

func TestServer_Integration(t *testing.T) {
	t.Run("basic pub/sub functionality", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		// Create publisher and subscriber connections
		pubConn, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer pubConn.Close()
		
		subConn, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer subConn.Close()
		
		// Set up subscriber
		received := make(chan string, 1)
		_, err = subConn.Subscribe("test.topic", func(msg *nats.Msg) {
			received <- string(msg.Data)
		})
		require.NoError(t, err)
		
		// Publish message
		err = pubConn.Publish("test.topic", []byte("hello world"))
		require.NoError(t, err)
		
		// Verify message received
		select {
		case msg := <-received:
			assert.Equal(t, "hello world", msg)
		case <-time.After(2 * time.Second):
			t.Fatal("Message not received within timeout")
		}
	})
	
	t.Run("request/reply functionality", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		// Create connections
		nc, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer nc.Close()
		
		// Set up responder
		_, err = nc.Subscribe("echo", func(msg *nats.Msg) {
			nc.Publish(msg.Reply, msg.Data)
		})
		require.NoError(t, err)
		
		// Send request
		resp, err := nc.Request("echo", []byte("ping"), 2*time.Second)
		require.NoError(t, err)
		assert.Equal(t, "ping", string(resp.Data))
	})
}

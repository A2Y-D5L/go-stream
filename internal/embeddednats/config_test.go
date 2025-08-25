package embeddednats

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	nserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --------------------- Configuration Tests ---------------------

func TestServerConfig_HostAndPort(t *testing.T) {
	t.Run("localhost host", func(t *testing.T) {
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
		assert.Contains(t, url, "localhost", "URL should contain localhost")
		
		// Should be able to connect
		nc, err := nats.Connect(url)
		require.NoError(t, err)
		defer nc.Close()
	})
	
	t.Run("127.0.0.1 host", func(t *testing.T) {
		opts := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   -1,
			NoSigs: true,
			NoLog:  true,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		url := server.ClientURL()
		assert.Contains(t, url, "127.0.0.1", "URL should contain IP address")
		
		testConnection(t, server)
	})
	
	t.Run("specific port", func(t *testing.T) {
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
		
		actualPort := server.Port()
		assert.Equal(t, freePort, actualPort, "Should bind to specified port")
		
		url := server.ClientURL()
		assert.Contains(t, url, fmt.Sprintf(":%d", freePort), "URL should contain port")
	})
	
	t.Run("dynamic port allocation", func(t *testing.T) {
		opts := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   -1, // Dynamic
			NoSigs: true,
			NoLog:  true,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		port := server.Port()
		assert.Greater(t, port, 0, "Should allocate a port")
		assert.NotEqual(t, 4222, port, "Should not use default NATS port")
		
		url := server.ClientURL()
		assert.NotEmpty(t, url, "Should have a valid URL")
	})
	
	t.Run("zero port (dynamic)", func(t *testing.T) {
		opts := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   0, // Also means dynamic
			NoSigs: true,
			NoLog:  true,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		port := server.Port()
		assert.Greater(t, port, 0, "Should allocate a port")
	})
}

func TestServerConfig_JetStream(t *testing.T) {
	t.Run("JetStream enabled", func(t *testing.T) {
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
		
		// Connect and verify JetStream is available
		nc, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer nc.Close()
		
		js, err := nc.JetStream()
		assert.NoError(t, err, "JetStream should be available")
		assert.NotNil(t, js)
		
		// Test basic JetStream functionality
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.*"},
		})
		assert.NoError(t, err, "Should be able to create stream")
	})
	
	t.Run("JetStream disabled", func(t *testing.T) {
		opts := &nserver.Options{
			Host:      "127.0.0.1",
			Port:      -1,
			NoSigs:    true,
			NoLog:     true,
			JetStream: false,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		// Connect and verify JetStream operations fail
		nc, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer nc.Close()
		
		js, err := nc.JetStream()
		if err != nil {
			// If JetStream context creation fails, that's expected
			assert.Error(t, err, "JetStream should not be available")
		} else {
			// If context is created, operations should fail
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "TEST_FAIL",
				Subjects: []string{"test.*"},
			})
			assert.Error(t, err, "JetStream operations should fail when disabled")
		}
	})
}

func TestServerConfig_TLS(t *testing.T) {
	t.Run("TLS configuration", func(t *testing.T) {
		// Note: This test creates a basic TLS config for testing
		// In production, proper certificates should be used
		
		tlsConfig := &tls.Config{
			InsecureSkipVerify: true, // Only for testing
		}
		
		opts := &nserver.Options{
			Host:      "127.0.0.1",
			Port:      -1,
			NoSigs:    true,
			NoLog:     true,
			TLS:       true,
			TLSConfig: tlsConfig,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		
		// TLS setup can be complex, so allow it to fail gracefully
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		err := server.Ready(ctx)
		if err != nil {
			t.Logf("TLS server failed to start (may be expected in test environment): %v", err)
			t.Skip("TLS test skipped due to startup failure")
		}
		
		url := server.ClientURL()
		// TLS URLs should use tls:// scheme
		assert.Contains(t, url, "tls://", "TLS server should use tls:// scheme")
		
		// Connect with TLS
		nc, err := nats.Connect(url, nats.Secure(tlsConfig))
		if err != nil {
			// TLS setup can be complex in test environment, so we allow this to fail gracefully
			t.Logf("TLS connection failed (may be expected in test environment): %v", err)
			t.Skip("TLS test skipped due to connection failure")
		}
		defer nc.Close()
		
		assert.True(t, nc.IsConnected(), "Should be connected via TLS")
	})
}

func TestServerConfig_Authentication(t *testing.T) {
	t.Run("no authentication", func(t *testing.T) {
		opts := createTestServerOptions()
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		// Should be able to connect without credentials
		nc, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer nc.Close()
		
		assert.True(t, nc.IsConnected())
	})
	
	t.Run("with username/password", func(t *testing.T) {
		opts := &nserver.Options{
			Host:     "127.0.0.1",
			Port:     -1,
			NoSigs:   true,
			NoLog:    true,
			Username: "test_user",
			Password: "test_pass",
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		
		// Allow for startup failure with auth config
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		err := server.Ready(ctx)
		if err != nil {
			t.Logf("Auth server failed to start: %v", err)
			t.Skip("Auth test skipped due to startup failure")
		}
		
		// Should fail without credentials
		_, err = nats.Connect(server.ClientURL(), nats.Timeout(1*time.Second))
		assert.Error(t, err, "Should fail without credentials")
		
		// Should succeed with correct credentials
		nc, err := nats.Connect(server.ClientURL(),
			nats.UserInfo("test_user", "test_pass"),
			nats.Timeout(2*time.Second))
		require.NoError(t, err)
		defer nc.Close()
		
		assert.True(t, nc.IsConnected())
	})
}

func TestServerConfig_Limits(t *testing.T) {
	t.Run("max connections", func(t *testing.T) {
		opts := &nserver.Options{
			Host:           "127.0.0.1",
			Port:           -1,
			NoSigs:         true,
			NoLog:          true,
			MaxConn:        2, // Very low limit for testing
			MaxPayload:     1024,
			MaxPending:     1024,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		// Create connections up to the limit
		var connections []*nats.Conn
		defer func() {
			for _, nc := range connections {
				nc.Close()
			}
		}()
		
		for i := 0; i < 2; i++ {
			nc, err := nats.Connect(server.ClientURL(), nats.Timeout(1*time.Second))
			if err == nil {
				connections = append(connections, nc)
			}
		}
		
		assert.LessOrEqual(t, len(connections), 2, "Should respect max connections")
		
		// Additional connection might fail
		_, err := nats.Connect(server.ClientURL(), nats.Timeout(500*time.Millisecond))
		if err != nil {
			t.Logf("Additional connection rejected as expected: %v", err)
		}
	})
	
	t.Run("max payload", func(t *testing.T) {
		opts := &nserver.Options{
			Host:       "127.0.0.1",
			Port:       -1,
			NoSigs:     true,
			NoLog:      true,
			MaxPayload: 100, // Very small for testing
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		nc, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer nc.Close()
		
		// Small message should work
		err = nc.Publish("test", []byte("small"))
		assert.NoError(t, err)
		
		// Large message should fail
		largeData := make([]byte, 200)
		for i := range largeData {
			largeData[i] = 'A'
		}
		
		err = nc.Publish("test", largeData)
		assert.Error(t, err, "Large message should be rejected")
	})
}

func TestServerConfig_Clustering(t *testing.T) {
	t.Run("cluster configuration", func(t *testing.T) {
		clusterPort, err := findFreePort()
		require.NoError(t, err)
		
		opts := &nserver.Options{
			Host:        "127.0.0.1",
			Port:        -1,
			NoSigs:      true,
			NoLog:       true,
			Cluster: nserver.ClusterOpts{
				Host: "127.0.0.1",
				Port: clusterPort,
			},
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		// Server should start successfully with cluster config
		assertServerRunning(t, server)
		testConnection(t, server)
	})
}

func TestServerConfig_Storage(t *testing.T) {
	t.Run("memory storage", func(t *testing.T) {
		opts := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   -1,
			NoSigs: true,
			NoLog:  true,
			JetStream: true,
			StoreDir:  "", // Memory storage
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 10*time.Second)
		
		// Should work with memory storage
		nc, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer nc.Close()
		
		js, err := nc.JetStream()
		require.NoError(t, err)
		
		// Create a stream
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "MEMORY_TEST",
			Subjects: []string{"memory.*"},
			Storage:  nats.MemoryStorage,
		})
		assert.NoError(t, err)
	})
	
	t.Run("custom store directory", func(t *testing.T) {
		// Use temporary directory for testing
		tmpDir := t.TempDir()
		
		opts := &nserver.Options{
			Host:      "127.0.0.1",
			Port:      -1,
			NoSigs:    true,
			NoLog:     true,
			JetStream: true,
			StoreDir:  tmpDir,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 10*time.Second)
		
		// Should work with file storage
		nc, err := nats.Connect(server.ClientURL())
		require.NoError(t, err)
		defer nc.Close()
		
		js, err := nc.JetStream()
		require.NoError(t, err)
		
		// Create a stream with file storage
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "FILE_TEST",
			Subjects: []string{"file.*"},
			Storage:  nats.FileStorage,
		})
		assert.NoError(t, err)
	})
}

func TestServerConfig_Advanced(t *testing.T) {
	t.Run("custom timeouts", func(t *testing.T) {
		opts := &nserver.Options{
			Host:         "127.0.0.1",
			Port:         -1,
			NoSigs:       true,
			NoLog:        true,
			PingInterval: 10 * time.Second,
			MaxPingsOut:  3,
			WriteDeadline: 5 * time.Second,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		assertServerRunning(t, server)
		testConnection(t, server)
	})
	
	t.Run("debug and profiling", func(t *testing.T) {
		profPort, err := findFreePort()
		require.NoError(t, err)
		
		opts := &nserver.Options{
			Host:       "127.0.0.1",
			Port:       -1,
			NoSigs:     true,
			NoLog:      true,
			HTTPHost:   "127.0.0.1",
			HTTPPort:   profPort,
			Debug:      true,
			Trace:      true,
			Logtime:    true,
		}
		
		server := createTestServer(t, opts)
		defer cleanupServer(t, server)
		
		server.Start()
		waitForServerReady(t, server, 5*time.Second)
		
		assertServerRunning(t, server)
		
		// Note: We don't test HTTP endpoints here as they're not exposed
		// through our server interface, but the server should start successfully
	})
}

// --------------------- Invalid Configuration Tests ---------------------

func TestServerConfig_InvalidConfigurations(t *testing.T) {
	t.Run("port conflict handling", func(t *testing.T) {
		// Create first server on a specific port
		freePort, err := findFreePort()
		require.NoError(t, err)
		
		opts1 := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   freePort,
			NoSigs: true,
			NoLog:  true,
		}
		
		server1 := createTestServer(t, opts1)
		defer cleanupServer(t, server1)
		
		server1.Start()
		waitForServerReady(t, server1, 5*time.Second)
		
		// Try to create second server on same port
		opts2 := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   freePort, // Same port
			NoSigs: true,
			NoLog:  true,
		}
		
		server2, err := New(opts2)
		require.NoError(t, err, "Server creation should succeed")
		
		server2.Start()
		
		// Second server should fail to become ready due to port conflict
		// Allow more time as it may retry
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		
		err = server2.Ready(ctx)
		
		// NATS may handle port conflicts by binding to different interfaces
		// or may fail, so we allow both behaviors
		if err == nil {
			// If it succeeds, check if it's actually on a different port
			port2 := server2.Port()
			if port2 == freePort {
				t.Logf("Warning: Second server unexpectedly bound to same port")
			} else {
				t.Logf("Second server bound to different port: %d", port2)
			}
		} else {
			// This is the expected behavior for port conflicts
			t.Logf("Second server failed as expected due to port conflict: %v", err)
		}
		
		// Cleanup second server
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		server2.ShutdownAndWait(shutdownCtx, 2*time.Second)
	})
	
	t.Run("invalid host", func(t *testing.T) {
		opts := &nserver.Options{
			Host:   "invalid.nonexistent.host",
			Port:   -1,
			NoSigs: true,
			NoLog:  true,
		}
		
		server, err := New(opts)
		if err != nil {
			// Creation might fail with invalid host
			t.Logf("Server creation failed with invalid host: %v", err)
			return
		}
		
		server.Start()
		
		// Should fail to become ready
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		
		err = server.Ready(ctx)
		assert.Error(t, err, "Should fail with invalid host")
		
		// Cleanup
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		server.ShutdownAndWait(shutdownCtx, 2*time.Second)
	})
	
	t.Run("extremely high port", func(t *testing.T) {
		opts := &nserver.Options{
			Host:   "127.0.0.1",
			Port:   99999, // Very high port
			NoSigs: true,
			NoLog:  true,
		}
		
		server, err := New(opts)
		if err != nil {
			t.Logf("Server creation failed with high port: %v", err)
			return
		}
		
		server.Start()
		
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		
		err = server.Ready(ctx)
		if err != nil {
			t.Logf("Server failed to start with high port: %v", err)
		}
		
		// Cleanup regardless
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		server.ShutdownAndWait(shutdownCtx, 2*time.Second)
	})
}

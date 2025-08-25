package client

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStream_New(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		cleanup func(*Stream)
		wantErr bool
	}{
		{
			name:    "default options",
			opts:    []Option{WithServerReadyTimeout(15 * time.Second), WithDisableJetStream()},
			wantErr: false,
		},
		{
			name:    "with custom host",
			opts:    []Option{WithHost("localhost"), WithServerReadyTimeout(15 * time.Second), WithDisableJetStream()},
			wantErr: false,
		},
		{
			name:    "with specific port",
			opts:    []Option{WithPort(0), WithServerReadyTimeout(15 * time.Second), WithDisableJetStream()}, // Use port 0 for dynamic allocation
			wantErr: false,
		},
		{
			name:    "with random port",
			opts:    []Option{WithRandomPort(), WithServerReadyTimeout(15 * time.Second), WithDisableJetStream()},
			wantErr: false,
		},
		{
			name: "with custom timeouts",
			opts: []Option{
				WithConnectTimeout(5 * time.Second),
				WithDrainTimeout(3 * time.Second),
				WithServerReadyTimeout(15 * time.Second),
				WithDisableJetStream(),
			},
			wantErr: false,
		},
		{
			name:    "with default topic mode core",
			opts:    []Option{WithDefaultTopicMode(TopicModeCore), WithServerReadyTimeout(15 * time.Second), WithDisableJetStream()},
			wantErr: false,
		},
		{
			name:    "with default topic mode jetstream",
			opts:    []Option{WithDefaultTopicMode(TopicModeJetStream), WithServerReadyTimeout(15 * time.Second)}, // Keep JetStream enabled for this test
			wantErr: false,
		},
		{
			name:    "with custom codec",
			opts:    []Option{WithDefaultCodec(JSONCodec), WithServerReadyTimeout(15 * time.Second), WithDisableJetStream()},
			wantErr: false,
		},
		{
			name:    "with max payload",
			opts:    []Option{WithMaxPayload(1024 * 1024), WithServerReadyTimeout(15 * time.Second), WithDisableJetStream()},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			s, err := New(ctx, tt.opts...)
			if tt.cleanup != nil && s != nil {
				defer tt.cleanup(s)
			} else if s != nil {
				defer CleanupStream(t, s)
			}

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, s)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, s)

			// Verify stream is healthy
			AssertStreamHealthy(t, s)

			// Verify internal state
			assert.True(t, s.started.Load())
			assert.False(t, s.closed.Load())
		})
	}
}

func TestStream_New_ErrorCases(t *testing.T) {
	tests := []struct {
		name    string
		opts    []Option
		wantErr bool
	}{
		{
			name: "invalid port - negative",
			opts: []Option{WithPort(-2), WithServerReadyTimeout(15 * time.Second), WithDisableJetStream()}, // -1 is valid (dynamic), -2 is not
			wantErr: true, // Port -2 should cause an error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			s, err := New(ctx, tt.opts...)
			if s != nil {
				defer CleanupStream(t, s)
			}

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestStream_Healthy(t *testing.T) {
	t.Run("healthy stream", func(t *testing.T) {
		s := CreateTestStream(t)
		defer CleanupStream(t, s)

		ctx, cancel := WithTestTimeout(2 * time.Second)
		defer cancel()

		err := s.Healthy(ctx)
		assert.NoError(t, err)
	})

	t.Run("closed stream", func(t *testing.T) {
		s := CreateTestStream(t)

		// Close the stream
		ctx, cancel := WithTestTimeout(5 * time.Second)
		defer cancel()
		err := s.Close(ctx)
		require.NoError(t, err)

		// Health check should fail
		err = s.Healthy(ctx)
		assert.Error(t, err)
	})

	t.Run("never started stream", func(t *testing.T) {
		// Create a stream object without calling New
		s := &Stream{}

		ctx, cancel := WithTestTimeout(2 * time.Second)
		defer cancel()

		err := s.Healthy(ctx)
		assert.Error(t, err)
		// Stream should not be healthy when not properly initialized
		assert.Contains(t, err.Error(), "not healthy")
	})
}

func TestStream_Close(t *testing.T) {
	t.Run("normal close", func(t *testing.T) {
		s := CreateTestStream(t)

		ctx, cancel := WithTestTimeout(10 * time.Second)
		defer cancel()

		err := s.Close(ctx)
		assert.NoError(t, err)

		// Verify stream is marked as closed
		assert.True(t, s.closed.Load())

		// Health check should fail after close
		err = s.Healthy(ctx)
		assert.Error(t, err)
	})

	t.Run("double close", func(t *testing.T) {
		s := CreateTestStream(t)

		ctx, cancel := WithTestTimeout(10 * time.Second)
		defer cancel()

		// First close should succeed
		err := s.Close(ctx)
		assert.NoError(t, err)

		// Second close should return error
		err = s.Close(ctx)
		assert.Error(t, err)
	})

	t.Run("close with timeout", func(t *testing.T) {
		s := CreateTestStream(t)

		// Use a very short timeout to test timeout handling
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
		defer cancel()

		err := s.Close(ctx)
		// This might succeed or fail depending on timing, but should not panic
		if err != nil {
			t.Logf("Close with short timeout returned error (expected): %v", err)
		}
	})

	t.Run("close never started stream", func(t *testing.T) {
		s := &Stream{}

		ctx, cancel := WithTestTimeout(2 * time.Second)
		defer cancel()

		err := s.Close(ctx)
		assert.Error(t, err)
	})
}

func TestStream_Configuration(t *testing.T) {
	t.Run("default configuration", func(t *testing.T) {
		cfg := defaultConfig()

		assert.Equal(t, "127.0.0.1", cfg.Host)
		assert.Equal(t, -1, cfg.Port)
		assert.Equal(t, TopicModeCore, cfg.DefaultTopicMode)
		assert.Equal(t, JSONCodec, cfg.DefaultCodec)
		assert.Equal(t, "go-stream", cfg.ClientName)
		assert.Nil(t, cfg.TLS)
	})

	t.Run("option application", func(t *testing.T) {
		cfg := defaultConfig()

		// Apply various options
		opts := []Option{
			WithHost("localhost"),
			WithPort(4222),
			WithDefaultTopicMode(TopicModeJetStream),
			WithConnectTimeout(10 * time.Second),
			WithMaxPayload(2048),
		}

		for _, opt := range opts {
			opt(&cfg)
		}

		assert.Equal(t, "localhost", cfg.Host)
		assert.Equal(t, 4222, cfg.Port)
		assert.Equal(t, TopicModeJetStream, cfg.DefaultTopicMode)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)
		assert.Equal(t, 2048, cfg.MaxPayload)
	})

	t.Run("TLS configuration", func(t *testing.T) {
		cfg := defaultConfig()

		tlsConfig := &tls.Config{
			InsecureSkipVerify: true,
		}

		WithTLS(tlsConfig)(&cfg)

		assert.NotNil(t, cfg.TLS)
		assert.True(t, cfg.TLS.InsecureSkipVerify)
	})
}

func TestStream_PortManagement(t *testing.T) {
	t.Run("dynamic port allocation", func(t *testing.T) {
		s := CreateTestStream(t, WithRandomPort())
		defer CleanupStream(t, s)

		// Server should have allocated a dynamic port
		port := s.srv.Port()
		assert.Greater(t, port, 0)

		// Client URL should contain the allocated port
		url := s.srv.ClientURL()
		assert.Contains(t, url, "nats://")
		assert.NotContains(t, url, ":0") // Should not contain the placeholder port
	})

	t.Run("specific port allocation", func(t *testing.T) {
		// Use port 0 which gets dynamically assigned by the OS
		s := CreateTestStream(t, WithPort(0))
		defer CleanupStream(t, s)

		port := s.srv.Port()
		assert.Greater(t, port, 0)
	})
}

func TestStream_ConcurrentOperations(t *testing.T) {
	t.Run("concurrent health checks", func(t *testing.T) {
		s := CreateTestStream(t)
		defer CleanupStream(t, s)

		const numGoroutines = 10
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				ctx, cancel := WithTestTimeout(5 * time.Second)
				defer cancel()
				errChan <- s.Healthy(ctx)
			}()
		}

		// All health checks should succeed
		for i := 0; i < numGoroutines; i++ {
			err := <-errChan
			assert.NoError(t, err)
		}
	})

	t.Run("concurrent close operations", func(t *testing.T) {
		s := CreateTestStream(t)

		const numGoroutines = 5
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				ctx, cancel := WithTestTimeout(10 * time.Second)
				defer cancel()
				errChan <- s.Close(ctx)
			}()
		}

		// First close should succeed, rest should fail
		successCount := 0
		errorCount := 0

		for i := 0; i < numGoroutines; i++ {
			err := <-errChan
			if err == nil {
				successCount++
			} else {
				errorCount++
			}
		}

		assert.Equal(t, 1, successCount, "Exactly one close should succeed")
		assert.Equal(t, numGoroutines-1, errorCount, "Rest should fail")
	})
}

package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/test/helpers"
	"github.com/a2y-d5l/go-stream/topic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStream_New(t *testing.T) {
	tests := []struct {
		name    string
		opts    []client.Option
		cleanup func(*client.Stream)
		wantErr bool
	}{
		{
			name:    "default options",
			opts:    []client.Option{client.WithServerReadyTimeout(15 * time.Second), client.WithDisableJetStream()},
			wantErr: false,
		},
		{
			name:    "with custom host",
			opts:    []client.Option{client.WithHost("localhost"), client.WithServerReadyTimeout(15 * time.Second), client.WithDisableJetStream()},
			wantErr: false,
		},
		{
			name:    "with specific port",
			opts:    []client.Option{client.WithPort(0), client.WithServerReadyTimeout(15 * time.Second), client.WithDisableJetStream()}, // Use port 0 for dynamic allocation
			wantErr: false,
		},
		{
			name:    "with random port",
			opts:    []client.Option{client.WithRandomPort(), client.WithServerReadyTimeout(15 * time.Second), client.WithDisableJetStream()},
			wantErr: false,
		},
		{
			name: "with custom timeouts",
			opts: []client.Option{
				client.WithConnectTimeout(5 * time.Second),
				client.WithDrainTimeout(3 * time.Second),
				client.WithServerReadyTimeout(15 * time.Second),
				client.WithDisableJetStream(),
			},
			wantErr: false,
		},
		{
			name:    "with default topic mode core",
			opts:    []client.Option{client.WithDefaultTopicMode(topic.ModeCore), client.WithServerReadyTimeout(15 * time.Second), client.WithDisableJetStream()},
			wantErr: false,
		},
		{
			name:    "with default topic mode jetstream",
			opts:    []client.Option{client.WithDefaultTopicMode(topic.ModeJetStream), client.WithServerReadyTimeout(15 * time.Second)}, // Keep JetStream enabled for this test
			wantErr: false,
		},
		{
			name:    "with custom codec",
			opts:    []client.Option{client.WithDefaultCodec(message.JSONCodec), client.WithServerReadyTimeout(15 * time.Second), client.WithDisableJetStream()},
			wantErr: false,
		},
		{
			name:    "with max payload",
			opts:    []client.Option{client.WithMaxPayload(1024 * 1024), client.WithServerReadyTimeout(15 * time.Second), client.WithDisableJetStream()},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			s, err := client.New(ctx, tt.opts...)
			if tt.cleanup != nil && s != nil {
				defer tt.cleanup(s)
			} else if s != nil {
				defer helpers.CleanupStream(t, s)
			}

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, s)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, s)

			// Verify stream is healthy
			helpers.AssertStreamHealthy(t, s)
		})
	}
}

func TestStream_New_ErrorCases(t *testing.T) {
	tests := []struct {
		name    string
		opts    []client.Option
		wantErr bool
	}{
		{
			name: "invalid port - negative",
			opts: []client.Option{client.WithPort(-2), client.WithServerReadyTimeout(15 * time.Second), client.WithDisableJetStream()}, // -1 is valid (dynamic), -2 is not
			wantErr: true, // Port -2 should cause an error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
			defer cancel()

			s, err := client.New(ctx, tt.opts...)
			if s != nil {
				defer helpers.CleanupStream(t, s)
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
		s := helpers.CreateTestStream(t)
		defer helpers.CleanupStream(t, s)

		ctx, cancel := context.WithTimeout(t.Context(), 2 * time.Second)
		defer cancel()

		err := s.Healthy(ctx)
		assert.NoError(t, err)
	})

	t.Run("closed stream", func(t *testing.T) {
		s := helpers.CreateTestStream(t)

		// Close the stream
		ctx, cancel := context.WithTimeout(t.Context(), 5 * time.Second)
		defer cancel()
		err := s.Close(ctx)
		require.NoError(t, err)

		// Health check should fail
		err = s.Healthy(ctx)
		assert.Error(t, err)
	})

	t.Run("never started stream", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 2 * time.Second)
		defer cancel()

		// Create a stream object without calling New
		err := (&client.Stream{}).Healthy(ctx)
		assert.Error(t, err)
		// Stream should not be healthy when not properly initialized
		assert.Contains(t, err.Error(), "not healthy")
	})
}

func TestStream_Close(t *testing.T) {
	t.Run("normal close", func(t *testing.T) {
		s := helpers.CreateTestStream(t)

		ctx, cancel := context.WithTimeout(t.Context(), 10 * time.Second)
		defer cancel()

		err := s.Close(ctx)
		assert.NoError(t, err)
		// Health check should fail after close
		assert.Error(t, s.Healthy(ctx))
	})

	t.Run("double close", func(t *testing.T) {
		s := helpers.CreateTestStream(t)
		// First close should succeed
		assert.NoError(t, s.Close(t.Context()))
		// Second close should return error
		assert.Error(t, s.Close(t.Context()))
	})

	t.Run("close with timeout", func(t *testing.T) {
		// Use a very short timeout to test timeout handling
		ctx, cancel := context.WithTimeout(t.Context(), 1*time.Microsecond)
		defer cancel()

		err := helpers.CreateTestStream(t).Close(ctx)
		// This might succeed or fail depending on timing, but should not panic
		if err != nil {
			t.Logf("Close with short timeout returned error (expected): %v", err)
		}
	})

	t.Run("close never started stream", func(t *testing.T) {
		assert.Error(t, (&client.Stream{}).Close(t.Context()))
	})
}

func TestStream_PortManagement(t *testing.T) {
	// t.Run("dynamic port allocation", func(t *testing.T) {
	// 	s := helpers.CreateTestStream(t, client.WithRandomPort())
	// 	defer helpers.CleanupStream(t, s)

	// 	// Server should have allocated a dynamic port
	// 	port := s.srv.Port()
	// 	assert.Greater(t, port, 0)

	// 	// Client URL should contain the allocated port
	// 	url := s.srv.ClientURL()
	// 	assert.Contains(t, url, "nats://")
	// 	assert.NotContains(t, url, ":0") // Should not contain the placeholder port
	// })

	t.Run("specific port allocation", func(t *testing.T) {
		// Use port 0 which gets dynamically assigned by the OS
		s := helpers.CreateTestStream(t, client.WithPort(0))
		defer helpers.CleanupStream(t, s)
		assert.Greater(t, s.Port(), 0)
	})
}

func TestStream_ConcurrentOperations(t *testing.T) {
	t.Run("concurrent health checks", func(t *testing.T) {
		s := helpers.CreateTestStream(t)
		defer helpers.CleanupStream(t, s)

		const numGoroutines = 10
		errChan := make(chan error, numGoroutines)

		for range numGoroutines {
			go func() {
				ctx, cancel := context.WithTimeout(t.Context(), 5 * time.Second)
				defer cancel()
				errChan <- s.Healthy(ctx)
			}()
		}

		// All health checks should succeed
		for range numGoroutines {
			err := <-errChan
			assert.NoError(t, err)
		}
	})

	t.Run("concurrent close operations", func(t *testing.T) {
		s := helpers.CreateTestStream(t)

		const numGoroutines = 5
		errChan := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				ctx, cancel := context.WithTimeout(t.Context(), 10 * time.Second)
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

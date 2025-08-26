package client

import (
	"crypto/tls"
	"log/slog"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/topic"
	"github.com/stretchr/testify/assert"
)

func TestConfig_Defaults(t *testing.T) {
	t.Run("default config values", func(t *testing.T) {
		cfg := defaultConfig()

		assert.Equal(t, "127.0.0.1", cfg.Host)
		assert.Equal(t, -1, cfg.Port)      // Random port
		assert.Equal(t, 0, cfg.MaxPayload) // NATS default
		assert.Equal(t, 5*time.Second, cfg.ServerReadyTimeout)
		assert.Equal(t, 5*time.Second, cfg.ServerShutdownMaxWait)
		assert.Equal(t, "go-stream", cfg.ClientName)
		assert.Equal(t, 2*time.Second, cfg.ConnectTimeout)
		assert.Equal(t, 2*time.Second, cfg.ConnectFlushTimeout)
		assert.Equal(t, 250*time.Millisecond, cfg.ReconnectWaitMin)
		assert.Equal(t, topic.ModeCore, cfg.DefaultTopicMode)
		assert.Equal(t, message.JSONCodec, cfg.DefaultCodec)
		assert.Equal(t, "X-Request-Id", cfg.RequestIDHeader)
		assert.Empty(t, cfg.User)
		assert.Empty(t, cfg.Pass)
		assert.Empty(t, cfg.Token)
		assert.Nil(t, cfg.log)
		assert.Nil(t, cfg.TLS)
		assert.Empty(t, cfg.StoreDir)
	})

	t.Run("zero value config", func(t *testing.T) {
		var cfg config

		assert.Empty(t, cfg.Host)
		assert.Equal(t, 0, cfg.Port)
		assert.Equal(t, 0, cfg.MaxPayload)
		assert.Equal(t, time.Duration(0), cfg.ServerReadyTimeout)
		assert.Empty(t, cfg.ClientName)
		assert.Equal(t, topic.Mode(0), cfg.DefaultTopicMode)
		assert.Nil(t, cfg.DefaultCodec)
	})
}

func TestConfig_WithHost(t *testing.T) {
	t.Run("valid host", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("localhost")(&cfg)
		assert.Equal(t, "localhost", cfg.Host)
	})

	t.Run("IP address", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("192.168.1.100")(&cfg)
		assert.Equal(t, "192.168.1.100", cfg.Host)
	})

	t.Run("empty host", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("")(&cfg)
		assert.Equal(t, "", cfg.Host)
	})

	t.Run("FQDN host", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("nats.example.com")(&cfg)
		assert.Equal(t, "nats.example.com", cfg.Host)
	})
}

func TestConfig_WithPort(t *testing.T) {
	t.Run("valid port", func(t *testing.T) {
		cfg := defaultConfig()
		WithPort(4222)(&cfg)
		assert.Equal(t, 4222, cfg.Port)
	})

	t.Run("high port number", func(t *testing.T) {
		cfg := defaultConfig()
		WithPort(65535)(&cfg)
		assert.Equal(t, 65535, cfg.Port)
	})

	t.Run("zero port", func(t *testing.T) {
		cfg := defaultConfig()
		WithPort(0)(&cfg)
		assert.Equal(t, 0, cfg.Port)
	})

	t.Run("random port", func(t *testing.T) {
		cfg := defaultConfig()
		WithRandomPort()(&cfg)
		assert.Equal(t, -1, cfg.Port)
	})
}

func TestConfig_WithTLS(t *testing.T) {
	t.Run("valid TLS config", func(t *testing.T) {
		cfg := defaultConfig()
		WithTLS(&tls.Config{ServerName: "example.com"})(&cfg)
		assert.Equal(t, &tls.Config{ServerName: "example.com"}, cfg.TLS)
	})

	t.Run("nil TLS config", func(t *testing.T) {
		cfg := defaultConfig()
		WithTLS(nil)(&cfg)
		assert.Nil(t, cfg.TLS)
	})
}

func TestConfig_WithStoreDir(t *testing.T) {
	t.Run("valid store directory", func(t *testing.T) {
		cfg := defaultConfig()
		WithStoreDir("/tmp/jetstream")(&cfg)
		assert.Equal(t, "/tmp/jetstream", cfg.StoreDir)
	})

	t.Run("empty store directory", func(t *testing.T) {
		cfg := defaultConfig()
		WithStoreDir("")(&cfg)

		assert.Equal(t, "", cfg.StoreDir)
	})

	t.Run("relative path", func(t *testing.T) {
		cfg := defaultConfig()
		WithStoreDir("./data/jetstream")(&cfg)

		assert.Equal(t, "./data/jetstream", cfg.StoreDir)
	})
}

func TestConfig_WithMaxPayload(t *testing.T) {
	t.Run("valid max payload", func(t *testing.T) {
		cfg := defaultConfig()
		WithMaxPayload(1024 * 1024)(&cfg)

		assert.Equal(t, 1024*1024, cfg.MaxPayload)
	})

	t.Run("zero max payload", func(t *testing.T) {
		cfg := defaultConfig()
		WithMaxPayload(0)(&cfg)

		assert.Equal(t, 0, cfg.MaxPayload)
	})

	t.Run("large max payload", func(t *testing.T) {
		cfg := defaultConfig()

		option := WithMaxPayload(100 * 1024 * 1024) // 100MB
		option(&cfg)

		assert.Equal(t, 100*1024*1024, cfg.MaxPayload)
	})
}

func TestConfig_WithDefaultTopicMode(t *testing.T) {
	t.Run("core topic mode", func(t *testing.T) {
		cfg := defaultConfig()
		WithDefaultTopicMode(topic.ModeCore)(&cfg)

		assert.Equal(t, topic.ModeCore, cfg.DefaultTopicMode)
	})

	t.Run("jetstream topic mode", func(t *testing.T) {
		cfg := defaultConfig()
		WithDefaultTopicMode(topic.ModeJetStream)(&cfg)

		assert.Equal(t, topic.ModeJetStream, cfg.DefaultTopicMode)
	})
}

func TestConfig_WithDefaultCodec(t *testing.T) {
	t.Run("JSON codec", func(t *testing.T) {
		cfg := defaultConfig()
		WithDefaultCodec(message.JSONCodec)(&cfg)

		assert.Equal(t, message.JSONCodec, cfg.DefaultCodec)
	})

	t.Run("nil codec", func(t *testing.T) {
		cfg := defaultConfig()
		WithDefaultCodec(nil)(&cfg)

		assert.Nil(t, cfg.DefaultCodec)
	})
}

func TestConfig_WithTimeouts(t *testing.T) {
	t.Run("connect timeout", func(t *testing.T) {
		cfg := defaultConfig()

		WithConnectTimeout(10 * time.Second)(&cfg)

		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)
	})

	t.Run("reconnect wait", func(t *testing.T) {
		cfg := defaultConfig()
		WithReconnectWait(1 * time.Second)(&cfg)

		assert.Equal(t, 1*time.Second, cfg.ReconnectWaitMin)
	})

	t.Run("drain timeout", func(t *testing.T) {
		cfg := defaultConfig()
		WithDrainTimeout(30 * time.Second)(&cfg)

		assert.Equal(t, 30*time.Second, cfg.ServerShutdownMaxWait)
	})

	t.Run("zero timeouts", func(t *testing.T) {
		cfg := defaultConfig()
		WithConnectTimeout(0)(&cfg)
		WithReconnectWait(0)(&cfg)
		WithDrainTimeout(0)(&cfg)

		assert.Equal(t, time.Duration(0), cfg.ConnectTimeout)
		assert.Equal(t, time.Duration(0), cfg.ReconnectWaitMin)
		assert.Equal(t, time.Duration(0), cfg.ServerShutdownMaxWait)
	})
}

func TestConfig_WithLogger(t *testing.T) {
	t.Run("valid logger", func(t *testing.T) {
		cfg := defaultConfig()
		WithLogger(slog.Default())(&cfg)

		assert.Equal(t, slog.Default(), cfg.log)
	})

	t.Run("nil logger", func(t *testing.T) {
		cfg := defaultConfig()
		WithLogger(nil)(&cfg)

		assert.Nil(t, cfg.log)
	})
}

func TestConfig_WithRequestIDHeader(t *testing.T) {
	t.Run("custom header name", func(t *testing.T) {
		cfg := defaultConfig()

		option := WithRequestIDHeader("X-Correlation-ID")
		option(&cfg)

		assert.Equal(t, "X-Correlation-ID", cfg.RequestIDHeader)
	})

	t.Run("empty header name", func(t *testing.T) {
		cfg := defaultConfig()
		WithRequestIDHeader("")(&cfg)

		assert.Equal(t, "", cfg.RequestIDHeader)
	})
}

func TestConfig_WithAuth(t *testing.T) {
	t.Run("basic auth", func(t *testing.T) {
		cfg := defaultConfig()
		WithBasicAuth("username", "password")(&cfg)

		assert.Equal(t, "username", cfg.User)
		assert.Equal(t, "password", cfg.Pass)
	})

	t.Run("empty basic auth", func(t *testing.T) {
		cfg := defaultConfig()
		WithBasicAuth("", "")(&cfg)

		assert.Equal(t, "", cfg.User)
		assert.Equal(t, "", cfg.Pass)
	})

	t.Run("token auth", func(t *testing.T) {
		cfg := defaultConfig()
		WithTokenAuth("secret-token-123")(&cfg)

		assert.Equal(t, "secret-token-123", cfg.Token)
	})

	t.Run("empty token auth", func(t *testing.T) {
		cfg := defaultConfig()
		WithTokenAuth("")(&cfg)

		assert.Equal(t, "", cfg.Token)
	})
}

func TestConfig_MultipleOptions(t *testing.T) {
	t.Run("apply multiple options", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("192.168.1.100")(&cfg)
		WithPort(8222)(&cfg)
		WithMaxPayload(2048)(&cfg)
		WithConnectTimeout(5 * time.Second)(&cfg)
		WithDefaultTopicMode(topic.ModeJetStream)(&cfg)
		WithRequestIDHeader("X-Trace-ID")(&cfg)

		assert.Equal(t, "192.168.1.100", cfg.Host)
		assert.Equal(t, 8222, cfg.Port)
		assert.Equal(t, 2048, cfg.MaxPayload)
		assert.Equal(t, 5*time.Second, cfg.ConnectTimeout)
		assert.Equal(t, topic.ModeJetStream, cfg.DefaultTopicMode)
		assert.Equal(t, "X-Trace-ID", cfg.RequestIDHeader)
	})

	t.Run("override options", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("localhost")(&cfg)
		WithPort(4222)(&cfg)
		WithHost("remote-host")(&cfg)
		WithPort(8222)(&cfg)

		assert.Equal(t, "remote-host", cfg.Host)
		assert.Equal(t, 8222, cfg.Port)
	})
}

func TestConfig_OptionTypes(t *testing.T) {
	t.Run("option function signature", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("test")(&cfg)

		assert.Equal(t, "test", cfg.Host)
	})

	t.Run("option chaining", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("test-host")(&cfg)
		WithPort(9999)(&cfg)
		WithMaxPayload(4096)(&cfg)

		assert.Equal(t, "test-host", cfg.Host)
		assert.Equal(t, 9999, cfg.Port)
		assert.Equal(t, 4096, cfg.MaxPayload)
	})
}

func TestConfig_EdgeCases(t *testing.T) {
	t.Run("nil config", func(t *testing.T) {
		assert.Panics(t, func() { WithHost("test")(nil) })
	})

	t.Run("option reuse", func(t *testing.T) {
		hostOption := WithHost("shared-host")
		cfg1 := defaultConfig()
		cfg2 := defaultConfig()

		hostOption(&cfg1)
		hostOption(&cfg2)

		assert.Equal(t, cfg1.Host, cfg2.Host)
		assert.Equal(t, "shared-host", cfg1.Host)
		assert.Equal(t, "shared-host", cfg2.Host)
	})

	t.Run("extreme values", func(t *testing.T) {
		cfg := defaultConfig()
		WithPort(65535)(&cfg)
		WithMaxPayload(1<<31 - 1)(&cfg) // Max int32
		WithConnectTimeout(24 * time.Hour)(&cfg)

		assert.Equal(t, 65535, cfg.Port)
		assert.Equal(t, 1<<31-1, cfg.MaxPayload)
		assert.Equal(t, 24*time.Hour, cfg.ConnectTimeout)
	})
}

func TestConfig_MemoryAndPerformance(t *testing.T) {
	t.Run("config memory allocation", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("test-host")(&cfg)
		cfgCopy := cfg

		assert.Equal(t, cfg.Host, cfgCopy.Host)

		differentHost := "different-host"
		WithHost(differentHost)(&cfgCopy)

		assert.Equal(t, differentHost, cfgCopy.Host)
		assert.NotEqual(t, cfg.Host, cfgCopy.Host)
	})

	t.Run("option performance", func(t *testing.T) {
		cfg := defaultConfig()
		start := time.Now()
		for range 1000 {
			WithHost("localhost")(&cfg)
			WithPort(4222)(&cfg)
			WithMaxPayload(1024)(&cfg)
			WithConnectTimeout(time.Second)(&cfg)
		}

		// Should be very fast (less than 10ms for 4000 option applications)
		assert.Less(t, time.Since(start), 10*time.Millisecond)
	})
}

func TestConfig_Integration(t *testing.T) {
	t.Run("typical configuration", func(t *testing.T) {
		cfg := defaultConfig()
		WithHost("nats.production.com")(&cfg)
		WithPort(4222)(&cfg)
		WithMaxPayload(10 * 1024 * 1024)(&cfg) // 10MB
		WithStoreDir("/data/jetstream")(&cfg)
		WithDefaultTopicMode(topic.ModeJetStream)(&cfg)
		WithConnectTimeout(10 * time.Second)(&cfg)
		WithReconnectWait(500 * time.Millisecond)(&cfg)
		WithDrainTimeout(30 * time.Second)(&cfg)
		WithRequestIDHeader("X-Request-ID")(&cfg)
		WithBasicAuth("nats-user", "secure-password")(&cfg)

		// Verify all values are set correctly
		assert.Equal(t, "nats.production.com", cfg.Host)
		assert.Equal(t, 4222, cfg.Port)
		assert.Equal(t, 10*1024*1024, cfg.MaxPayload)
		assert.Equal(t, "/data/jetstream", cfg.StoreDir)
		assert.Equal(t, topic.ModeJetStream, cfg.DefaultTopicMode)
		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout)
		assert.Equal(t, 500*time.Millisecond, cfg.ReconnectWaitMin)
		assert.Equal(t, 30*time.Second, cfg.ServerShutdownMaxWait)
		assert.Equal(t, "X-Request-ID", cfg.RequestIDHeader)
		assert.Equal(t, "nats-user", cfg.User)
		assert.Equal(t, "secure-password", cfg.Pass)
	})
}

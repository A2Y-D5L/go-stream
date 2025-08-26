package client

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/topic"
)

// config holds all tunables for the Stream (via functional options).
type config struct {
	// Server
	Host                  string
	Port                  int // -1 selects a random free port
	TLS                   *tls.Config
	StoreDir              string
	MaxPayload            int
	ServerReadyTimeout    time.Duration
	ServerShutdownMaxWait time.Duration

	// Client
	ClientName          string
	ConnectTimeout      time.Duration
	ConnectFlushTimeout time.Duration
	ReconnectWaitMin    time.Duration

	// Defaults & behavior
	DefaultTopicMode topic.Mode
	DefaultCodec     message.Codec
	RequestIDHeader  string
	DrainTimeout     time.Duration

	// JetStream settings
	DisableJetStream bool // For testing purposes

	// Auth (optional)
	User  string
	Pass  string
	Token string

	// Logging
	log *slog.Logger
}

func defaultConfig() config {
	return config{
		Host:                  "127.0.0.1",
		Port:                  -1, // dynamic port by default
		MaxPayload:            0,  // nats default
		ServerReadyTimeout:    5 * time.Second,
		ServerShutdownMaxWait: 5 * time.Second,

		ClientName:          "go-stream",
		ConnectTimeout:      2 * time.Second,
		ConnectFlushTimeout: 2 * time.Second,
		ReconnectWaitMin:    250 * time.Millisecond,

		DefaultTopicMode: topic.ModeCore,
		DefaultCodec:     message.JSONCodec,
		RequestIDHeader:  "X-Request-Id",
	}
}

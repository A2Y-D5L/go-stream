package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	nserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/a2y-d5l/go-stream/internal/embeddednats"
	"github.com/a2y-d5l/go-stream/topic"
)

var (
	// ErrStreamClosed indicates the stream was already closed (or never started).
	ErrStreamClosed = errors.New("stream is closed")
	// ErrStreamUnhealthy indicates server or client is not ready/connected.
	ErrStreamUnhealthy = errors.New("stream is not healthy")
)

// Stream owns the embedded nats-server, client connection(s), and registries.
// It is safe for concurrent use.
type Stream struct {
	mu  sync.RWMutex
	cfg config
	log *slog.Logger

	srv *embeddednats.Server
	nc  *nats.Conn
	js  nats.JetStreamContext // lazily created when needed

	started atomic.Bool
	closed  atomic.Bool
}

// New starts an embedded NATS server, waits until it's ready, and connects a client.
//
// Defaults "just work" for local dev: loopback host, dynamic port, Core topic mode,
// JSON codec.
func New(ctx context.Context, opts ...Option) (*Stream, error) {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.log == nil {
		cfg.log = slog.Default()
	}

	// Server options
	sopts := &nserver.Options{
		Host:                  cfg.Host,
		Port:                  cfg.Port, // -1 => dynamic
		NoSigs:                true,     // embedded
		MaxPayload:            int32(cfg.MaxPayload),
		JetStream:             !cfg.DisableJetStream, // server-level enable; topic usage is opt-in later
		DisableShortFirstPing: true,
	}
	if cfg.TLS != nil {
		sopts.TLSConfig = cfg.TLS
		sopts.TLS = true
	}
	if cfg.StoreDir != "" {
		abs := cfg.StoreDir
		if !filepath.IsAbs(abs) {
			abs, _ = filepath.Abs(abs)
		}
		if err := os.MkdirAll(abs, 0o755); err != nil {
			return nil, fmt.Errorf("create store dir: %w", err)
		}
		sopts.StoreDir = abs
	}

	srv, err := embeddednats.New(sopts)
	if err != nil {
		return nil, err
	}
	srv.Start()

	readyCtx, cancel := context.WithTimeout(ctx, cfg.ServerReadyTimeout)
	defer cancel()
	if err := srv.Ready(readyCtx); err != nil {
		_ = srv.ShutdownAndWait(context.Background(), cfg.ServerShutdownMaxWait)
		return nil, fmt.Errorf("nats server not ready: %w", err)
	}

	// Client options
	copts := []nats.Option{
		nats.Name(cfg.ClientName),
		nats.Timeout(cfg.ConnectTimeout),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1), // infinite
		nats.ReconnectWait(cfg.ReconnectWaitMin),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				cfg.log.Warn("nats disconnected", "err", err)
			} else {
				cfg.log.Warn("nats disconnected")
			}
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) { cfg.log.Info("reconnected", "url", nc.ConnectedUrl()) }),
		nats.ClosedHandler(func(_ *nats.Conn) { cfg.log.Info("connection closed") }),
	}
	if cfg.TLS != nil {
		copts = append(copts, nats.Secure(cfg.TLS))
	}

	nc, err := nats.Connect(srv.ClientURL(), copts...)
	if err != nil {
		_ = srv.ShutdownAndWait(context.Background(), cfg.ServerShutdownMaxWait)
		return nil, fmt.Errorf("nats client connect: %w", err)
	}
	if err := nc.FlushTimeout(cfg.ConnectFlushTimeout); err != nil {
		nc.Close()
		_ = srv.ShutdownAndWait(context.Background(), cfg.ServerShutdownMaxWait)
		return nil, fmt.Errorf("initial flush: %w", err)
	}

	s := &Stream{cfg: cfg, log: cfg.log, srv: srv, nc: nc}
	s.started.Store(true)

	if cfg.DefaultTopicMode == topic.TopicModeJetStream {
		if js, jerr := nc.JetStream(); jerr == nil {
			s.js = js
		} else {
			cfg.log.Warn("jetstream init failed (will retry lazily)", "err", jerr)
		}
	}

	cfg.log.Info("go-stream started",
		"host", cfg.Host,
		"port", srv.Port(),
		"default_mode", cfg.DefaultTopicMode.String(),
		"default_codec", cfg.DefaultCodec.ContentType(),
	)

	return s, nil
}

// GetNATSConnection returns the underlying NATS connection for advanced usage.
// Use with caution as this exposes the internal connection.
func (s *Stream) GetNATSConnection() *nats.Conn {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nc
}

// GetJetStreamContext returns the JetStream context for advanced usage.
// Returns nil if JetStream is not available or configured.
func (s *Stream) GetJetStreamContext() nats.JetStreamContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.js
}

// Port returns the port number the embedded server is listening on.
// Returns -1 if the server is not running.
func (s *Stream) Port() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.srv == nil {
		return -1
	}
	return s.srv.Port()
}

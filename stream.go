package stream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	nserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/a2y-d5l/go-stream/internal/embeddednats"
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

// New starts an embedded NATS server, waits until it’s ready, and connects a client.
//
// Defaults “just work” for local dev: loopback host, dynamic port, Core topic mode,
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

	if cfg.DefaultTopicMode == TopicModeJetStream {
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

// Healthy returns an error if the stream is not in a healthy state. This is a
// lightweight check that verifies the basic operational status of the stream.
// For a more thorough check including server readiness, use DeepHealthCheck().
func (s *Stream) Healthy(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	switch {
	case !s.started.Load():
		return fmt.Errorf("%w: stream not started", ErrStreamUnhealthy)
	case s.closed.Load():
		return fmt.Errorf("%w: stream already closed", ErrStreamUnhealthy)
	case s.nc == nil:
		return fmt.Errorf("%w: client not initialized", ErrStreamUnhealthy)
	case s.srv == nil:
		return fmt.Errorf("%w: server not initialized", ErrStreamUnhealthy)
	case s.js == nil && s.cfg.DefaultTopicMode == TopicModeJetStream:
		return fmt.Errorf("%w: durable stream not initialized", ErrStreamUnhealthy)
	case s.nc.Status() != nats.CONNECTED:
		return fmt.Errorf("%w: client not connected", ErrStreamUnhealthy)
	default:
		// For high-load scenarios, if the client is connected, assume the server is ready.
		// This avoids expensive server readiness checks on every publish operation.
		// If there are actual server issues, they will manifest as client connection problems.
		return nil
	}
}

// DeepHealthCheck performs a thorough health check including server readiness verification.
// This is more expensive than Healthy() and should be used sparingly.
func (s *Stream) DeepHealthCheck(ctx context.Context) error {
	// First do the basic health check
	if err := s.Healthy(ctx); err != nil {
		return err
	}

	s.mu.RLock()
	srv := s.srv
	s.mu.RUnlock()

	// Now do the expensive server readiness check
	if srv != nil {
		ctx, cancel := context.WithTimeout(ctx, s.cfg.ServerReadyTimeout)
		defer cancel()

		if err := srv.Ready(ctx); err != nil {
			return fmt.Errorf("%w: server not ready", ErrStreamUnhealthy)
		}
	}

	return nil
}

// Close drains the client and shuts down the server gracefully.
func (s *Stream) Close(ctx context.Context) error {
	if !s.started.Load() || !s.closed.CompareAndSwap(false, true) {
		return ErrStreamClosed
	}

	// Copy current state to local variables to avoid holding the lock
	// while waiting for the client to drain or server to shutdown.
	// This is important to avoid deadlocks if the client or server
	// tries to acquire the same lock.
	// It also allows the caller to call Close multiple times safely.
	// If the stream is already closed, we can return early.
	if !s.started.Load() {
		return ErrStreamClosed
	}

	s.log.Info("closing stream...")

	s.mu.Lock()
	nc := s.nc
	srv := s.srv
	drainTO := s.cfg.DrainTimeout
	maxWait := s.cfg.ServerShutdownMaxWait
	s.mu.Unlock()

	var merr multiErr

	if nc != nil {
		done := make(chan error, 1)
		go func() { done <- nc.Drain() }()
		select {
		case err := <-done:
			if err != nil {
				merr.add(fmt.Errorf("nats drain: %w", err))
			}
		case <-time.After(drainTO):
			merr.add(fmt.Errorf("nats drain timeout after %s", drainTO))
			nc.Close()
		case <-ctx.Done():
			merr.add(fmt.Errorf("nats drain canceled: %w", ctx.Err()))
			nc.Close()
		}
	}

	if srv != nil {
		if err := srv.ShutdownAndWait(ctx, maxWait); err != nil {
			merr.add(err)
		}
	}

	s.mu.Lock()
	s.nc, s.srv, s.js = nil, nil, nil
	s.mu.Unlock()

	if len(merr) > 0 {
		return merr
	}

	s.log.Info("stream closed.")

	return nil
}

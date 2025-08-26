package client

import (
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/topic"
)

// Option configures the Stream.
type Option func(*config)

// WithHost sets the listen host for the embedded server (default 127.0.0.1).
func WithHost(h string) Option { return func(c *config) { c.Host = h } }

// WithPort sets the server port. Use WithRandomPort for dynamic.
func WithPort(p int) Option { return func(c *config) { c.Port = p } }

// WithRandomPort selects a random free port for the embedded server.
func WithRandomPort() Option { return func(c *config) { c.Port = -1 } }

// WithTLS enables TLS for the embedded server and client.
func WithTLS(cfg *tls.Config) Option { return func(c *config) { c.TLS = cfg } }

// WithStoreDir sets the JetStream store directory (useful for durable topics later).
func WithStoreDir(dir string) Option { return func(c *config) { c.StoreDir = dir } }

// WithMaxPayload sets the server max payload size (bytes).
func WithMaxPayload(bytes int) Option { return func(c *config) { c.MaxPayload = bytes } }

// WithDefaultTopicMode sets the default topic mode for new topics (Core or JetStream).
func WithDefaultTopicMode(m topic.Mode) Option { return func(c *config) { c.DefaultTopicMode = m } }

// WithDefaultCodec overrides the default codec (JSON by default).
func WithDefaultCodec(cd message.Codec) Option { return func(c *config) { c.DefaultCodec = cd } }

// WithConnectTimeout sets the client connect timeout.
func WithConnectTimeout(d time.Duration) Option { return func(c *config) { c.ConnectTimeout = d } }

// WithReconnectWait sets the fixed reconnect wait (min).
func WithReconnectWait(min time.Duration) Option { return func(c *config) { c.ReconnectWaitMin = min } }

// WithDrainTimeout sets how long Close waits for client drain before hard-close.
func WithDrainTimeout(d time.Duration) Option {
	return func(c *config) {
		c.DrainTimeout = d
		c.ServerShutdownMaxWait = d
	}
}

// WithServerReadyTimeout sets how long to wait for the embedded server to be ready.
func WithServerReadyTimeout(d time.Duration) Option {
	return func(c *config) { c.ServerReadyTimeout = d }
}

// WithDisableJetStream disables JetStream (useful for testing).
func WithDisableJetStream() Option { return func(c *config) { c.DisableJetStream = true } }

// WithLogger injects a slog logger.
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.log = l } }

// WithRequestIDHeader changes the correlation header name (default X-Request-Id).
func WithRequestIDHeader(name string) Option { return func(c *config) { c.RequestIDHeader = name } }

// WithBasicAuth sets user/password auth for the embedded server & client.
func WithBasicAuth(user, pass string) Option { return func(c *config) { c.User, c.Pass = user, pass } }

// WithTokenAuth sets token auth for the embedded server & client.
func WithTokenAuth(token string) Option { return func(c *config) { c.Token = token } }

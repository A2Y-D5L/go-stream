package pub

import (
	"context"
	"time"

	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/topic"
)

type Publisher interface {
	Publish(ctx context.Context, topic topic.Topic, msg message.Message, opts ...Option) error
}

type PublisherFunc func(ctx context.Context, topic topic.Topic, msg message.Message, opts ...Option) error

func (f PublisherFunc) Publish(ctx context.Context, topic topic.Topic, msg message.Message, opts ...Option) error {
	return f(ctx, topic, msg, opts...)
}

type Option func(*config)

type config struct {
	headers map[string]string
	replyTo string
	flush   bool
	timeout time.Duration // used when flushing with a timeout
}

// WithHeaders merges the given headers into the outbound message.
func WithHeaders(h map[string]string) Option {
	return func(c *config) {
		if c.headers == nil {
			c.headers = make(map[string]string, len(h))
		}
		for k, v := range h {
			c.headers[k] = v
		}
	}
}

// WithReplyTo sets the NATS Reply field for publish or request flows.
func WithReplyTo(t topic.Topic) Option {
	return func(c *config) { c.replyTo = string(t) }
}

// WithMessageID sets/overrides the X-message.Message-Id header.
func WithMessageID(id string) Option {
	return func(c *config) {
		if c.headers == nil {
			c.headers = make(map[string]string, 1)
		}
		c.headers["X-message.Message-Id"] = id
	}
}

// WithFlush controls whether to flush after publish; pair with WithFlushTimeout.
func WithFlush(on bool) Option {
	return func(c *config) { c.flush = on }
}

// WithFlushTimeout sets an optional timeout used when flushing.
func WithFlushTimeout(d time.Duration) Option {
	return func(c *config) { c.timeout = d }
}

package stream

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
)

type Publisher interface {
	Publish(ctx context.Context, topic Topic, msg Message, opts ...PublishOption) error
}

type PublisherFunc func(ctx context.Context, topic Topic, msg Message, opts ...PublishOption) error

func (f PublisherFunc) Publish(ctx context.Context, topic Topic, msg Message, opts ...PublishOption) error {
	return f(ctx, topic, msg, opts...)
}

type PublishOption func(*publishCfg)

type publishCfg struct {
	headers map[string]string
	replyTo string
	flush   bool
	timeout time.Duration // used when flushing with a timeout
}

// WithHeaders merges the given headers into the outbound message.
func WithHeaders(h map[string]string) PublishOption {
	return func(c *publishCfg) {
		if c.headers == nil {
			c.headers = make(map[string]string, len(h))
		}
		for k, v := range h {
			c.headers[k] = v
		}
	}
}

// WithReplyTo sets the NATS Reply field for publish or request flows.
func WithReplyTo(t Topic) PublishOption {
	return func(c *publishCfg) { c.replyTo = string(t) }
}

// WithMessageID sets/overrides the X-Message-Id header.
func WithMessageID(id string) PublishOption {
	return func(c *publishCfg) {
		if c.headers == nil {
			c.headers = make(map[string]string, 1)
		}
		c.headers["X-Message-Id"] = id
	}
}

// WithFlush controls whether to flush after publish; pair with WithFlushTimeout.
func WithFlush(on bool) PublishOption {
	return func(c *publishCfg) { c.flush = on }
}

// WithFlushTimeout sets an optional timeout used when flushing.
func WithFlushTimeout(d time.Duration) PublishOption {
	return func(c *publishCfg) { c.timeout = d }
}

// -------------------------- Stream façade (Core impl) ----------------------

func (s *Stream) Publish(ctx context.Context, topic Topic, msg Message, opts ...PublishOption) error {
	if err := s.Healthy(ctx); err != nil {
		return err
	}
	cfg := &publishCfg{}
	for _, opt := range opts {
		opt(cfg)
	}

	// Copy + merge headers to avoid mutating caller's message.
	out := msg
	if len(cfg.headers) > 0 {
		if out.Headers == nil {
			out.Headers = make(map[string]string, len(cfg.headers))
		}
		for k, v := range cfg.headers {
			out.Headers[k] = v
		}
	}

	nm := &nats.Msg{
		Subject: string(topic),
		Data:    out.Data,
		Reply:   cfg.replyTo,
		Header:  nats.Header{},
	}
	for k, v := range out.Headers {
		nm.Header.Set(k, v)
	}

	if err := s.nc.PublishMsg(nm); err != nil {
		return err
	}

	if cfg.flush {
		to := cfg.timeout
		if to <= 0 {
			to = s.cfg.ConnectFlushTimeout
		}
		ctx, cancel := context.WithTimeout(ctx, to)
		defer cancel()
		return s.nc.FlushWithContext(ctx)
	}
	return nil
}

func (s *Stream) Request(
	ctx context.Context,
	topic Topic,
	msg Message,
	timeout time.Duration,
	opts ...PublishOption,
) (Message, error) {
	if err := s.Healthy(ctx); err != nil {
		return Message{}, err
	}
	cfg := &publishCfg{}
	for _, opt := range opts {
		opt(cfg)
	}
	if timeout <= 0 {
		timeout = s.cfg.ConnectTimeout
	}

	nm := &nats.Msg{
		Subject: string(topic),
		Data:    msg.Data,
		Reply:   cfg.replyTo,
		Header:  nats.Header{},
	}
	for k, v := range msg.Headers {
		nm.Header.Set(k, v)
	}

	resp, err := s.nc.RequestMsg(nm, timeout)
	if err != nil {
		// Map NATS timeout to a standard context error for now.
		if errors.Is(err, nats.ErrTimeout) {
			return Message{}, context.DeadlineExceeded
		}
		return Message{}, err
	}

	out := Message{
		Topic:   Topic(resp.Subject),
		Data:    resp.Data,
		Headers: map[string]string{},
		Time:    time.Now(),
	}
	for k, vv := range resp.Header {
		if len(vv) > 0 {
			out.Headers[k] = vv[0]
		}
	}
	return out, nil
}

func (s *Stream) PublishJSON(ctx context.Context, topic Topic, v any, opts ...PublishOption) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	// Ensure JSON content type.
	opts = append(opts, WithHeaders(map[string]string{"Content-Type": "application/json"}))
	return s.Publish(ctx, topic, Message{
		Topic:   topic,
		Data:    b,
		Headers: map[string]string{"Content-Type": "application/json"},
		Time:    time.Now(),
	}, opts...)
}

func (s *Stream) RegisterPublisher(name string, p Publisher) error { return nil }
func (s *Stream) Publisher(name string) (Publisher, bool)          { return nil, false }

// --------------------------- Generic helpers -------------------------------

// RequestJSON sends a typed request and decodes a typed response using JSON.
// (When the Codec system is fully wired, this can delegate to the resolved codec.)
func RequestJSON[T any](
	s *Stream,
	ctx context.Context,
	topic Topic,
	req any,
	timeout time.Duration,
	opts ...PublishOption,
) (T, error) {
	var zero T

	b, err := json.Marshal(req)
	if err != nil {
		return zero, err
	}
	msg := Message{
		Topic:   topic,
		Data:    b,
		Headers: map[string]string{"Content-Type": "application/json"},
		Time:    time.Now(),
	}
	resp, err := s.Request(ctx, topic, msg, timeout, opts...)
	if err != nil {
		return zero, err
	}
	if len(resp.Data) == 0 {
		return zero, errors.New("empty response")
	}

	var out T
	if err := json.Unmarshal(resp.Data, &out); err != nil {
		return zero, err
	}
	return out, nil
}

// --------------------------- Typed constructors ----------------------------

// JSONRequester wraps RequestJSON with captured stream/topic.
type JSONRequester[Req, Resp any] struct {
	s     *Stream
	topic Topic
}

func NewJSONRequester[Req, Resp any](s *Stream, topic Topic) JSONRequester[Req, Resp] {
	return JSONRequester[Req, Resp]{s: s, topic: topic}
}

func (r JSONRequester[Req, Resp]) Request(ctx context.Context, req Req, timeout time.Duration, opts ...PublishOption) (Resp, error) {
	return RequestJSON[Resp](r.s, ctx, r.topic, req, timeout, opts...)
}

// JSONPublisher wraps PublishJSON with captured stream/topic.
type JSONPublisher[T any] struct {
	s     *Stream
	topic Topic
}

func NewJSONPublisher[T any](s *Stream, topic Topic) JSONPublisher[T] {
	return JSONPublisher[T]{s: s, topic: topic}
}

func (p JSONPublisher[T]) Publish(ctx context.Context, v T, opts ...PublishOption) error {
	return p.s.PublishJSON(ctx, p.topic, v, opts...)
}

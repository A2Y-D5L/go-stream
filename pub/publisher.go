package pub

import (
	"maps"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/topic"
)

// StreamPublisher implements Publisher using a NATS connection.
// It provides the core publishing functionality for the streaming system.
type StreamPublisher struct {
	nc           *nats.Conn
	healthCheck  func(context.Context) error
	flushTimeout time.Duration
}

// NewStreamPublisher creates a new StreamPublisher with the given NATS connection.
func NewStreamPublisher(nc *nats.Conn, healthCheck func(context.Context) error, flushTimeout time.Duration) *StreamPublisher {
	return &StreamPublisher{
		nc:           nc,
		healthCheck:  healthCheck,
		flushTimeout: flushTimeout,
	}
}

// Publish sends a message to the specified topic.
func (sp *StreamPublisher) Publish(ctx context.Context, t topic.Topic, msg message.Message, opts ...Option) error {
	if err := sp.healthCheck(ctx); err != nil {
		return err
	}

	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	// Copy + merge headers to avoid mutating caller's message.
	out := msg
	if len(cfg.headers) > 0 {
		if out.Headers == nil {
			out.Headers = make(map[string]string, len(cfg.headers))
		}
		maps.Copy(out.Headers, cfg.headers)
	}

	nm := &nats.Msg{
		Subject: string(t),
		Data:    out.Data,
		Reply:   cfg.replyTo,
		Header:  nats.Header{},
	}
	for k, v := range out.Headers {
		nm.Header.Set(k, v)
	}

	if err := sp.nc.PublishMsg(nm); err != nil {
		return err
	}

	if cfg.flush {
		to := cfg.timeout
		if to <= 0 {
			to = sp.flushTimeout
		}
		ctx, cancel := context.WithTimeout(ctx, to)
		defer cancel()
		return sp.nc.FlushWithContext(ctx)
	}
	return nil
}

// Request sends a request message and waits for a response.
func (sp *StreamPublisher) Request(
	ctx context.Context,
	t topic.Topic,
	msg message.Message,
	timeout time.Duration,
	opts ...Option,
) (message.Message, error) {
	if err := sp.healthCheck(ctx); err != nil {
		return message.Message{}, err
	}

	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}
	if timeout <= 0 {
		timeout = sp.flushTimeout
	}

	nm := &nats.Msg{
		Subject: string(t),
		Data:    msg.Data,
		Reply:   cfg.replyTo,
		Header:  nats.Header{},
	}
	for k, v := range msg.Headers {
		nm.Header.Set(k, v)
	}

	resp, err := sp.nc.RequestMsg(nm, timeout)
	if err != nil {
		// Map NATS timeout to a standard context error for now.
		if errors.Is(err, nats.ErrTimeout) {
			return message.Message{}, context.DeadlineExceeded
		}
		return message.Message{}, err
	}

	out := message.Message{
		Topic:   topic.Topic(resp.Subject),
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

// PublishJSON sends a JSON-encoded message to the specified topic.
func (sp *StreamPublisher) PublishJSON(ctx context.Context, t topic.Topic, v any, opts ...Option) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	// Ensure JSON content type.
	opts = append(opts, WithHeaders(map[string]string{"Content-Type": "application/json"}))
	return sp.Publish(ctx, t, message.Message{
		Topic:   t,
		Data:    b,
		Headers: map[string]string{"Content-Type": "application/json"},
		Time:    time.Now(),
	}, opts...)
}

// RequestJSON sends a typed request and decodes a typed response using JSON.
func RequestJSON[T any](
	sp *StreamPublisher,
	ctx context.Context,
	t topic.Topic,
	req any,
	timeout time.Duration,
	opts ...Option,
) (T, error) {
	var zero T

	b, err := json.Marshal(req)
	if err != nil {
		return zero, err
	}
	msg := message.Message{
		Topic:   t,
		Data:    b,
		Headers: map[string]string{"Content-Type": "application/json"},
		Time:    time.Now(),
	}
	resp, err := sp.Request(ctx, t, msg, timeout, opts...)
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

// JSONRequester wraps RequestJSON with captured stream/topic.
type JSONRequester[Req, Resp any] struct {
	sp    *StreamPublisher
	topic topic.Topic
}

func NewJSONRequester[Req, Resp any](sp *StreamPublisher, t topic.Topic) JSONRequester[Req, Resp] {
	return JSONRequester[Req, Resp]{sp: sp, topic: t}
}

func (r JSONRequester[Req, Resp]) Request(ctx context.Context, req Req, timeout time.Duration, opts ...Option) (Resp, error) {
	return RequestJSON[Resp](r.sp, ctx, r.topic, req, timeout, opts...)
}

// JSONPublisher wraps PublishJSON with captured stream/topic.
type JSONPublisher[T any] struct {
	sp    *StreamPublisher
	topic topic.Topic
}

func NewJSONPublisher[T any](sp *StreamPublisher, t topic.Topic) JSONPublisher[T] {
	return JSONPublisher[T]{sp: sp, topic: t}
}

func (p JSONPublisher[T]) Publish(ctx context.Context, v T, opts ...Option) error {
	return p.sp.PublishJSON(ctx, p.topic, v, opts...)
}

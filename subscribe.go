package stream

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"
)

// ----------------------------- Subscriber API ------------------------------

type Subscriber interface {
	Handle(ctx context.Context, msg Message) error
}

type SubscriberFunc func(ctx context.Context, msg Message) error

func (f SubscriberFunc) Handle(ctx context.Context, msg Message) error { return f(ctx, msg) }

// Backpressure and ack semantics.
type BackpressurePolicy int

const (
	BackpressureBlock BackpressurePolicy = iota // default
	BackpressureDropNewest
	BackpressureDropOldest
)

type AckPolicy int

const (
	AckAuto AckPolicy = iota // (JS only; ignored in Core mode)
	AckManual
)

type StartPosition int

const (
	DeliverNew StartPosition = iota
	DeliverByStartTime
	DeliverLastPerSubject
)

type SubscribeOptions struct {
	// Delivery topology
	QueueGroupName string
	Concurrency    int
	BufferSize     int
	Backpressure   BackpressurePolicy

	// JetStream semantics (ignored in Core mode for now)
	AckPolicy  AckPolicy
	MaxDeliver int
	Backoff    []time.Duration
	Durable    string
	StartAt    StartPosition
	StartTime  time.Time
	DLQ        Topic

	// Codec (reserved for later)
	Codec Codec
}

type SubscribeOption func(*SubscribeOptions)

type Subscription interface {
	Drain(ctx context.Context) error
	Stop() error
}

// -------------------------- Stream façade (Core impl) ----------------------

func (s *Stream) Subscribe(topic Topic, sub Subscriber, opts ...SubscribeOption) (Subscription, error) {
	if err := s.Healthy(context.Background()); err != nil {
		return nil, err
	}

	cfg := defaultSubscribeOptions()
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 1024
	}
	qgroup := cfg.QueueGroupName
	if qgroup == "" {
		qgroup = "q." + sanitizeTopic(string(topic))
	}

	// bounded queue + worker pool
	ch := make(chan Message, cfg.BufferSize)

	// nats callback -> enqueue with backpressure policy
	cb := func(m *nats.Msg) {
		msg := Message{
			Topic:   Topic(m.Subject),
			Data:    m.Data,
			Headers: map[string]string{},
			Time:    time.Now(),
		}
		for k, vv := range m.Header {
			if len(vv) > 0 {
				msg.Headers[k] = vv[0]
			}
		}

		switch cfg.Backpressure {
		case BackpressureBlock:
			ch <- msg
		case BackpressureDropNewest:
			select {
			case ch <- msg:
			default:
				// drop newest
			}
		case BackpressureDropOldest:
			select {
			case ch <- msg:
			default:
				select {
				case <-ch:
				default:
				}
				ch <- msg
			}
		}
	}

	ns, err := s.nc.QueueSubscribe(string(topic), qgroup, cb)
	if err != nil {
		return nil, err
	}
	if err := s.nc.FlushTimeout(s.cfg.ConnectFlushTimeout); err != nil {
		_ = ns.Unsubscribe()
		return nil, err
	}

	// workers
	var wg sync.WaitGroup
	wg.Add(cfg.Concurrency)
	stop := make(chan struct{})
	for i := 0; i < cfg.Concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				case m, ok := <-ch:
					if !ok {
						return
					}
					// Handle message with panic recovery
					func() {
						defer func() {
							if r := recover(); r != nil {
								// Log the panic but don't crash the worker
								if s.log != nil {
									s.log.Error("subscriber panic recovered", 
										"panic", r, 
										"topic", m.Topic)
								}
							}
						}()
						// Best-effort ctx (can be enhanced with tracing/req-id in later steps)
						_ = sub.Handle(context.Background(), m)
					}()
				}
			}
		}()
	}

	return &coreSub{
		ns:   ns,
		ch:   ch,
		wg:   &wg,
		stop: stop,
	}, nil
}

// --------------------------- Generic helpers -------------------------------

func SubscribeJSON[T any](
	s *Stream,
	topic Topic,
	handler func(ctx context.Context, m T) error,
	opts ...SubscribeOption,
) (Subscription, error) {
	adapter := SubscriberFunc(func(ctx context.Context, msg Message) error {
		var t T
		if len(msg.Data) == 0 {
			return errors.New("decode error: empty message")
		}
		if err := json.Unmarshal(msg.Data, &t); err != nil {
			return err
		}
		return handler(ctx, t)
	})
	return s.Subscribe(topic, adapter, opts...)
}

// --------------------------- Typed constructors ----------------------------

type JSONSubscriber[T any] struct {
	s     *Stream
	topic Topic
	opts  []SubscribeOption
}

func NewJSONSubscriber[T any](s *Stream, topic Topic, opts ...SubscribeOption) JSONSubscriber[T] {
	return JSONSubscriber[T]{s: s, topic: topic, opts: opts}
}

func (j JSONSubscriber[T]) Subscribe(handler func(ctx context.Context, m T) error) (Subscription, error) {
	return SubscribeJSON(j.s, j.topic, handler, j.opts...)
}

// ------------------------------- core sub ----------------------------------

type coreSub struct {
	ns   *nats.Subscription
	ch   chan Message
	wg   *sync.WaitGroup
	stop chan struct{}
}

func (c *coreSub) Drain(ctx context.Context) error {
	// Stop accepting new messages from NATS.
	if c.ns != nil {
		if err := c.ns.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			return err
		}
	}
	// Wait for queue to drain.
	t := time.NewTicker(20 * time.Millisecond)
	defer t.Stop()
	for {
		if len(c.ch) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			goto STOP
		case <-t.C:
		}
	}
STOP:
	close(c.stop)
	close(c.ch)
	c.wg.Wait()
	return nil
}

func (c *coreSub) Stop() error {
	if c.ns != nil {
		_ = c.ns.Unsubscribe()
	}
	close(c.stop)
	close(c.ch)
	c.wg.Wait()
	return nil
}

// ------------------------------- opts utils --------------------------------

func defaultSubscribeOptions() SubscribeOptions {
	return SubscribeOptions{
		Concurrency:  1,
		BufferSize:   1024,
		Backpressure: BackpressureBlock,
	}
}

// Option helpers (subset wired for Core mode; others are accepted but unused)
func WithQueueGroupName(name string) SubscribeOption { return func(o *SubscribeOptions) { o.QueueGroupName = name } }
func WithConcurrency(n int) SubscribeOption          { return func(o *SubscribeOptions) { o.Concurrency = n } }
func WithBufferSize(n int) SubscribeOption           { return func(o *SubscribeOptions) { o.BufferSize = n } }
func WithBackpressure(p BackpressurePolicy) SubscribeOption {
	return func(o *SubscribeOptions) { o.Backpressure = p }
}
func WithAckPolicy(p AckPolicy) SubscribeOption                 { return func(o *SubscribeOptions) { o.AckPolicy = p } }
func WithMaxDeliver(n int) SubscribeOption                      { return func(o *SubscribeOptions) { o.MaxDeliver = n } }
func WithExponentialBackoff(initial time.Duration, steps int, factor float64) SubscribeOption {
	seq := make([]time.Duration, 0, steps)
	cur := initial
	for i := 0; i < steps; i++ {
		seq = append(seq, cur)
		cur = time.Duration(float64(cur) * factor)
	}
	return func(o *SubscribeOptions) { o.Backoff = seq }
}
func WithDurable(name string) SubscribeOption   { return func(o *SubscribeOptions) { o.Durable = name } }
func WithStartAt(pos StartPosition) SubscribeOption {
	return func(o *SubscribeOptions) { o.StartAt = pos }
}
func WithStartTime(t time.Time) SubscribeOption { return func(o *SubscribeOptions) { o.StartTime = t } }
func WithDLQ(t Topic) SubscribeOption           { return func(o *SubscribeOptions) { o.DLQ = t } }

// ------------------------------- helpers -----------------------------------

func sanitizeTopic(s string) string {
	// Replace non-alnum with underscores, collapse repeats, lowercase.
	var b strings.Builder
	b.Grow(len(s))
	lastUnderscore := false
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(unicode.ToLower(r))
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := b.String()
	if strings.HasPrefix(out, "_") {
		out = strings.TrimLeft(out, "_")
	}
	if strings.HasSuffix(out, "_") {
		out = strings.TrimRight(out, "_")
	}
	if out == "" {
		return "unnamed"
	}
	if len(out) > 48 {
		return out[:48]
	}
	return out
}
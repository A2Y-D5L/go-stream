package sub

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nats-io/nats.go"

	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/topic"
)

// StreamSubscriber implements the core subscription functionality.
type StreamSubscriber struct {
	nc           *nats.Conn
	healthCheck  func(context.Context) error
	flushTimeout time.Duration
	log          *slog.Logger
}

// NewStreamSubscriber creates a new StreamSubscriber with the given NATS connection.
func NewStreamSubscriber(nc *nats.Conn, healthCheck func(context.Context) error, flushTimeout time.Duration, log *slog.Logger) *StreamSubscriber {
	return &StreamSubscriber{
		nc:           nc,
		healthCheck:  healthCheck,
		flushTimeout: flushTimeout,
		log:          log,
	}
}

// Subscribe creates a new subscription for the given topic.
func (ss *StreamSubscriber) Subscribe(t topic.Topic, sub Subscriber, opts ...Option) (Subscription, error) {
	if err := ss.healthCheck(context.Background()); err != nil {
		return nil, err
	}

	cfg := defaultOptions()
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
		qgroup = "q." + sanitizeTopic(string(t))
	}

	// bounded queue + worker pool
	ch := make(chan message.Message, cfg.BufferSize)

	// nats callback -> enqueue with backpressure policy
	cb := func(m *nats.Msg) {
		msg := message.Message{
			Topic:   topic.Topic(m.Subject),
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

	ns, err := ss.nc.QueueSubscribe(string(t), qgroup, cb)
	if err != nil {
		return nil, err
	}
	if err := ss.nc.FlushTimeout(ss.flushTimeout); err != nil {
		_ = ns.Unsubscribe()
		return nil, err
	}

	// workers
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Go(func() {
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
								if ss.log != nil {
									ss.log.Error("subscriber panic recovered",
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
		})
	}

	return &coreSub{
		ns:   ns,
		ch:   ch,
		wg:   &wg,
		stop: stop,
	}, nil
}

// coreSub implements the Subscription interface.
type coreSub struct {
	ns       *nats.Subscription
	ch       chan message.Message
	wg       *sync.WaitGroup
	stop     chan struct{}
	stopOnce sync.Once
	chOnce   sync.Once
}

func (c *coreSub) Drain(ctx context.Context) error {
	// Stop accepting new messages from NATS but let pending ones complete
	if c.ns != nil {
		if err := c.ns.Unsubscribe(); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			return err
		}
	}
	
	// Wait for the internal queue to drain with timeout
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	
	timeout := time.After(5 * time.Second) // Internal timeout for queue draining
	
	for {
		select {
		case <-timeout:
			// If queue doesn't drain within reasonable time, proceed to shutdown
			goto SHUTDOWN
		case <-ctx.Done():
			// Context cancelled, proceed to shutdown
			goto SHUTDOWN
		case <-ticker.C:
			// Check if queue is empty
			if len(c.ch) == 0 {
				goto SHUTDOWN
			}
		}
	}
	
SHUTDOWN:
	// Now close the message channel to signal no more messages will come
	c.chOnce.Do(func() {
		close(c.ch)
	})
	
	// Wait for all workers to finish processing
	c.wg.Wait()
	
	// Clean up stop channel
	c.stopOnce.Do(func() {
		close(c.stop)
	})
	
	return nil
}

func (c *coreSub) Stop() error {
	if c.ns != nil {
		_ = c.ns.Unsubscribe()
	}
	c.stopOnce.Do(func() {
		close(c.stop)
	})
	c.chOnce.Do(func() {
		close(c.ch)
	})
	c.wg.Wait()
	return nil
}

// sanitizeTopic cleans topic names for use as queue group names.
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

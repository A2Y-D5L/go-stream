package sub

import (
	"time"

	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/topic"
)

type Options struct {
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
	DLQ        topic.Topic

	// Codec (reserved for later)
	Codec message.Codec
}

type Option func(*Options)

func defaultOptions() Options {
	return Options{
		Concurrency:  1,
		BufferSize:   1024,
		Backpressure: BackpressureBlock,
	}
}

// Option helpers (subset wired for Core mode; others are accepted but unused)
func WithQueueGroupName(name string) Option { return func(o *Options) { o.QueueGroupName = name } }
func WithConcurrency(n int) Option          { return func(o *Options) { o.Concurrency = n } }
func WithBufferSize(n int) Option           { return func(o *Options) { o.BufferSize = n } }
func WithBackpressure(p BackpressurePolicy) Option {
	return func(o *Options) { o.Backpressure = p }
}
func WithAckPolicy(p AckPolicy) Option                 { return func(o *Options) { o.AckPolicy = p } }
func WithMaxDeliver(n int) Option                      { return func(o *Options) { o.MaxDeliver = n } }
func WithExponentialBackoff(initial time.Duration, steps int, factor float64) Option {
	seq := make([]time.Duration, 0, steps)
	cur := initial
	for i := 0; i < steps; i++ {
		seq = append(seq, cur)
		cur = time.Duration(float64(cur) * factor)
	}
	return func(o *Options) { o.Backoff = seq }
}
func WithDurable(name string) Option   { return func(o *Options) { o.Durable = name } }
func WithStartAt(pos StartPosition) Option {
	return func(o *Options) { o.StartAt = pos }
}
func WithStartTime(t time.Time) Option { return func(o *Options) { o.StartTime = t } }
func WithDLQ(t topic.Topic) Option           { return func(o *Options) { o.DLQ = t } }

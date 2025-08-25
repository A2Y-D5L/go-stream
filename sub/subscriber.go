package sub

import (
	"context"

	"github.com/a2y-d5l/go-stream/message"
)

type Subscriber interface {
	Handle(ctx context.Context, msg message.Message) error
}

type SubscriberFunc func(ctx context.Context, msg message.Message) error

func (f SubscriberFunc) Handle(ctx context.Context, msg message.Message) error { return f(ctx, msg) }

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

type Subscription interface {
	Drain(ctx context.Context) error
	Stop() error
}

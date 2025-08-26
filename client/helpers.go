package client

import (
	"context"
	"encoding/json"
	"time"

	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/pub"
	"github.com/a2y-d5l/go-stream/sub"
	"github.com/a2y-d5l/go-stream/topic"
)

// Stream methods facade
func (s *Stream) Publish(ctx context.Context, t topic.Topic, msg message.Message, opts ...pub.Option) error {
	publisher := pub.NewStreamPublisher(s.GetNATSConnection(), s.Healthy, time.Second*2)
	return publisher.Publish(ctx, t, msg, opts...)
}

func (s *Stream) PublishJSON(ctx context.Context, t topic.Topic, v any, opts ...pub.Option) error {
	publisher := pub.NewStreamPublisher(s.GetNATSConnection(), s.Healthy, time.Second*2)
	return publisher.PublishJSON(ctx, t, v, opts...)
}

func (s *Stream) Request(ctx context.Context, t topic.Topic, msg message.Message, timeout time.Duration, opts ...pub.Option) (message.Message, error) {
	publisher := pub.NewStreamPublisher(s.GetNATSConnection(), s.Healthy, time.Second*2)
	return publisher.Request(ctx, t, msg, timeout, opts...)
}

func (s *Stream) Subscribe(t topic.Topic, subscriber sub.Subscriber, opts ...sub.Option) (sub.Subscription, error) {
	ss := sub.NewStreamSubscriber(s.GetNATSConnection(), s.Healthy, time.Second*2, nil)
	return ss.Subscribe(t, subscriber, opts...)
}

// Generic helpers
func RequestJSON[T any](s *Stream, ctx context.Context, t topic.Topic, req any, timeout time.Duration, opts ...pub.Option) (T, error) {
	publisher := pub.NewStreamPublisher(s.GetNATSConnection(), s.Healthy, time.Second*2)
	return pub.RequestJSON[T](publisher, ctx, t, req, timeout, opts...)
}

func SubscribeJSON[T any](s *Stream, t topic.Topic, handler func(context.Context, T) error, opts ...sub.Option) (sub.Subscription, error) {
	adapter := sub.SubscriberFunc(func(ctx context.Context, msg message.Message) error {
		var data T
		if len(msg.Data) == 0 {
			return nil // Skip empty messages
		}
		if err := json.Unmarshal(msg.Data, &data); err != nil {
			return err
		}
		return handler(ctx, data)
	})
	return s.Subscribe(t, adapter, opts...)
}

// Typed constructors
func NewJSONRequester[Req, Resp any](s *Stream, t topic.Topic) pub.JSONRequester[Req, Resp] {
	publisher := pub.NewStreamPublisher(s.GetNATSConnection(), s.Healthy, time.Second*2)
	return pub.NewJSONRequester[Req, Resp](publisher, t)
}

func NewJSONPublisher[T any](s *Stream, t topic.Topic) pub.JSONPublisher[T] {
	publisher := pub.NewStreamPublisher(s.GetNATSConnection(), s.Healthy, time.Second*2)
	return pub.NewJSONPublisher[T](publisher, t)
}

// Placeholder registry methods (for future implementation)
func (s *Stream) RegisterPublisher(name string, p pub.Publisher) error { return nil }
func (s *Stream) Publisher(name string) (pub.Publisher, bool)          { return nil, false }

// Package stream provides an embedded NATS server with a small, ergonomic façade.
//
// This package has been restructured into focused subpackages for better organization:
//
//   - github.com/a2y-d5l/go-stream/client    - Main Stream type and configuration
//   - github.com/a2y-d5l/go-stream/pub       - Publishing functionality
//   - github.com/a2y-d5l/go-stream/sub       - Subscription functionality
//   - github.com/a2y-d5l/go-stream/message   - Message types and codecs
//   - github.com/a2y-d5l/go-stream/topic     - Topic types and management
//
// The root package maintains backward compatibility by providing the same API
// as before, but now delegates to the new structured packages internally.
//
// Example usage:
//
//	s, err := stream.New(ctx, stream.WithHost("localhost"))
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer s.Close(ctx)
//
//	// Publishing
//	err = s.Publish(ctx, "events.user.created", stream.Message{
//		Data: []byte(`{"user_id": "123"}`),
//	})
//
//	// Subscribing
//	sub, err := s.Subscribe("events.user.created",
//		stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
//			log.Printf("Received: %s", msg.Data)
//			return nil
//		}))
package stream

package testutil

import (
	"encoding/json"

	"github.com/nats-io/nats.go"

	"github.com/a2y-d5l/go-stream/topic"
)

// StreamInterface defines the minimal interface needed for test helpers.
type StreamInterface interface {
	GetNATSConnection() *nats.Conn
}

// TestResponder is a helper for testing request/reply functionality
// It sets up a proper NATS responder that can handle reply-to automatically
func TestResponder(s StreamInterface, t topic.Topic, handler func(data []byte) ([]byte, error)) (*nats.Subscription, error) {
	nc := s.GetNATSConnection()
	return nc.Subscribe(string(t), func(msg *nats.Msg) {
		if msg.Reply != "" {
			response, err := handler(msg.Data)
			if err != nil {
				// Send error response
				nc.Publish(msg.Reply, []byte("error: "+err.Error()))
				return
			}
			nc.Publish(msg.Reply, response)
		}
	})
}

// TestJSONResponder is a helper for testing JSON request/reply functionality
func TestJSONResponder(s StreamInterface, t topic.Topic, handler func(data []byte) (any, error)) (*nats.Subscription, error) {
	nc := s.GetNATSConnection()
	return nc.Subscribe(string(t), func(msg *nats.Msg) {
		if msg.Reply != "" {
			response, err := handler(msg.Data)
			if err != nil {
				// Send error response
				nc.Publish(msg.Reply, []byte("error: "+err.Error()))
				return
			}
			
			// Marshal response to JSON
			jsonData, err := json.Marshal(response)
			if err != nil {
				nc.Publish(msg.Reply, []byte("error: json marshal failed"))
				return
			}
			
			// Create response message with headers
			respMsg := &nats.Msg{
				Subject: msg.Reply,
				Data:    jsonData,
				Header:  nats.Header{},
			}
			respMsg.Header.Set("Content-Type", "application/json")
			nc.PublishMsg(respMsg)
		}
	})
}

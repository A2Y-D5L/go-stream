package stream

import (
	"encoding/json"

	"github.com/nats-io/nats.go"
)

// TestResponder is a helper for testing request/reply functionality
// It sets up a proper NATS responder that can handle reply-to automatically
func (s *Stream) TestResponder(topic Topic, handler func(data []byte) ([]byte, error)) (*nats.Subscription, error) {
	return s.nc.Subscribe(string(topic), func(msg *nats.Msg) {
		if msg.Reply != "" {
			response, err := handler(msg.Data)
			if err != nil {
				// Send error response
				s.nc.Publish(msg.Reply, []byte("error: "+err.Error()))
				return
			}
			s.nc.Publish(msg.Reply, response)
		}
	})
}

// TestJSONResponder is a helper for testing JSON request/reply functionality
func (s *Stream) TestJSONResponder(topic Topic, handler func(data []byte) (any, error)) (*nats.Subscription, error) {
	return s.nc.Subscribe(string(topic), func(msg *nats.Msg) {
		if msg.Reply != "" {
			response, err := handler(msg.Data)
			if err != nil {
				// Send error response
				s.nc.Publish(msg.Reply, []byte("error: "+err.Error()))
				return
			}
			
			// Marshal response to JSON
			jsonData, err := json.Marshal(response)
			if err != nil {
				s.nc.Publish(msg.Reply, []byte("error: json marshal failed"))
				return
			}
			
			// Create response message with headers
			respMsg := &nats.Msg{
				Subject: msg.Reply,
				Data:    jsonData,
				Header:  nats.Header{},
			}
			respMsg.Header.Set("Content-Type", "application/json")
			s.nc.PublishMsg(respMsg)
		}
	})
}

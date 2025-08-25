package message

import (
	"time"

	"github.com/a2y-d5l/go-stream/topic"
)

// Message represents a message in the streaming system
type Message struct {
	Topic   topic.Topic `json:"topic"`
	Data    []byte      `json:"data"`
	Headers Headers     `json:"headers"`
	ID      string      `json:"id"`
	Time    time.Time   `json:"time"`
}

// NewMessage creates a new message with the specified topic and data
func NewMessage(t topic.Topic, data []byte) *Message {
	return &Message{
		Topic:   t,
		Data:    data,
		Headers: NewHeaders(),
		Time:    time.Now(),
	}
}

// WithHeaders sets the message headers
func (m *Message) WithHeaders(headers Headers) *Message {
	m.Headers = headers
	return m
}

// WithID sets the message ID
func (m *Message) WithID(id string) *Message {
	m.ID = id
	m.Headers.SetMessageID(id)
	return m
}

// WithTimestamp sets the message timestamp
func (m *Message) WithTimestamp(t time.Time) *Message {
	m.Time = t
	m.Headers.SetTimestamp(t)
	return m
}

// Clone creates a deep copy of the message
func (m *Message) Clone() *Message {
	clone := &Message{
		Topic:   m.Topic,
		Data:    make([]byte, len(m.Data)),
		Headers: m.Headers.Clone(),
		ID:      m.ID,
		Time:    m.Time,
	}
	copy(clone.Data, m.Data)
	return clone
}

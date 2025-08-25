package topic

import (
	"errors"
	"time"
)

var (
	// ErrInvalidTopic indicates the topic name is invalid.
	ErrInvalidTopic = errors.New("invalid topic name")
	// ErrTopicExists indicates the topic already exists.
	ErrTopicExists = errors.New("topic already exists")
	// ErrUnknownTopic indicates the topic does not exist.
	ErrUnknownTopic = errors.New("unknown topic")
)

type Topic string

// TopicMode controls the default topic mode at the Stream level (topics can override).
type TopicMode int

const (
	TopicModeCore TopicMode = iota
	TopicModeJetStream
)

func (m TopicMode) String() string {
	switch m {
	case TopicModeCore:
		return "core"
	case TopicModeJetStream:
		return "jetstream"
	default:
		return "unknown"
	}
}

type Retention int

const (
	RetentionEphemeral Retention = iota // in-memory/core, or JS w/ small limits
	RetentionDurable                    // JS persisted stream
)

type DiscardPolicy int

const (
	DiscardOld DiscardPolicy = iota
	DiscardNew
)

// TopicOptions holds configuration for individual topics.
type TopicOptions struct {
	Mode           TopicMode
	Retention      Retention
	DiscardPolicy  DiscardPolicy
	MaxAge         time.Duration
	MaxMessages    int64
	MaxBytes       int64
	MaxConsumers   int
	Replicas       int
	NoAck          bool
	Duplicates     time.Duration
}

// ValidateTopic validates a topic name according to NATS subject naming rules
func ValidateTopic(topic Topic) error {
	if topic == "" {
		return ErrInvalidTopic
	}

	topicStr := string(topic)

	// Check for invalid characters
	for _, char := range topicStr {
		if char == ' ' || char == '\t' || char == '\n' || char == '\r' {
			return ErrInvalidTopic
		}
	}

	// Additional NATS subject validation could be added here
	// For now, we just check for basic invalid characters

	return nil
}

// String returns the string representation of the topic
func (t Topic) String() string {
	return string(t)
}

// IsEmpty returns true if the topic is empty
func (t Topic) IsEmpty() bool {
	return t == ""
}

// Equals checks if two topics are equal
func (t Topic) Equals(other Topic) bool {
	return t == other
}

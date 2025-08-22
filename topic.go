package stream

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

type TopicOptions struct {
	Mode            TopicMode     // default: stream default (TopicModeCore unless changed)
	Retention       Retention     // default: Ephemeral
	MaxBytes        int64         // JS
	MaxMsgs         int64         // JS
	MaxAge          time.Duration // JS
	Replicas        int           // JS; single embedded server => 1
	DiscardPolicy   DiscardPolicy // JS
	SubjectOverride string        // optional subject mapping
	Codec           Codec         // optional per-topic default
}

type TopicOption func(*TopicOptions)

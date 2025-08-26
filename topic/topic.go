package topic

import (
	"errors"
	"strings"
	"time"
	"unicode"
)

var (
	// ErrInvalidName indicates the topic name is invalid.
	ErrInvalidName = errors.New("invalid topic name")
	// ErrAlreadyExists indicates the topic already exists.
	ErrAlreadyExists = errors.New("topic already exists")
	// ErrUnknown indicates the topic does not exist.
	ErrUnknown = errors.New("unknown topic")
)

type Topic string

func New(name string) Topic {
	return Topic(Sanitize(name))
}

// Mode controls the default topic mode at the Stream level (topics can override).
type Mode int

const (
	ModeCore Mode = iota
	ModeJetStream
)

func (m Mode) String() string {
	switch m {
	case ModeCore:
		return "core"
	case ModeJetStream:
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

// Options holds configuration for individual topics.
type Options struct {
	Mode          Mode
	Retention     Retention
	DiscardPolicy DiscardPolicy
	MaxAge        time.Duration
	MaxMessages   int64
	MaxBytes      int64
	MaxConsumers  int
	Replicas      int
	NoAck         bool
	Duplicates    time.Duration
}

// Validate validates a topic name according to NATS subject naming rules
func Validate(t Topic) error {
	if t == "" {
		return ErrInvalidName
	}

	topicStr := string(t)

	// Check for invalid characters
	for _, char := range topicStr {
		if char == ' ' || char == '\t' || char == '\n' || char == '\r' {
			return ErrInvalidName
		}
	}

	// Check for NATS subject validation rules
	// No leading or trailing dots
	if strings.HasPrefix(topicStr, ".") || strings.HasSuffix(topicStr, ".") {
		return ErrInvalidName
	}

	// No double dots
	if strings.Contains(topicStr, "..") {
		return ErrInvalidName
	}

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

func Sanitize(s string) string {
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

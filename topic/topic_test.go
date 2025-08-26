package topic_test

import (
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream/topic"
	"github.com/stretchr/testify/assert"
)

func TestTopic_Validation(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		valid    bool
		expected string
	}{
		{
			name:     "simple valid topic",
			topic:    "user.events",
			valid:    true,
			expected: "user.events",
		},
		{
			name:     "multi-level topic",
			topic:    "order.created.v1",
			valid:    true,
			expected: "order.created.v1",
		},
		{
			name:     "single word topic",
			topic:    "health",
			valid:    true,
			expected: "health",
		},
		{
			name:     "topic with numbers",
			topic:    "user123.action456",
			valid:    true,
			expected: "user123.action456",
		},
		{
			name:     "topic with hyphens",
			topic:    "user-events.order-created",
			valid:    true,
			expected: "user-events.order-created",
		},
		{
			name:     "topic with underscores",
			topic:    "user_events.order_created",
			valid:    true,
			expected: "user_events.order_created",
		},
		{
			name:     "empty topic",
			topic:    "",
			valid:    false,
			expected: "",
		},
		{
			name:     "topic with spaces",
			topic:    "user events",
			valid:    false,
			expected: "",
		},
		{
			name:     "topic with double dots",
			topic:    "user..events",
			valid:    false,
			expected: "",
		},
		{
			name:     "topic starting with dot",
			topic:    ".user.events",
			valid:    false,
			expected: "",
		},
		{
			name:     "topic ending with dot",
			topic:    "user.events.",
			valid:    false,
			expected: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := topic.Validate(topic.Topic(test.topic)); test.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestTopic_Sanitization(t *testing.T) {
	// Test the topic.Sanitize function from subscribe.go
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple topic",
			input:    "user.events",
			expected: "user_events",
		},
		{
			name:     "topic with multiple dots",
			input:    "order.created.v1",
			expected: "order_created_v1",
		},
		{
			name:     "topic with special characters",
			input:    "user@events#test",
			expected: "user_events_test",
		},
		{
			name:     "topic with consecutive special chars",
			input:    "user...events",
			expected: "user_events",
		},
		{
			name:     "topic with leading/trailing special chars",
			input:    ".user.events.",
			expected: "user_events",
		},
		{
			name:     "empty topic",
			input:    "",
			expected: "unnamed",
		},
		{
			name:     "only special characters",
			input:    "...",
			expected: "unnamed",
		},
		{
			name:     "alphanumeric only",
			input:    "user123events456",
			expected: "user123events456",
		},
		{
			name:     "mixed case",
			input:    "User.Events.Test",
			expected: "user_events_test",
		},
		{
			name:     "very long topic",
			input:    "very.long.topic.name.that.exceeds.the.maximum.length.limit.and.should.be.truncated.at.some.point",
			expected: "very_long_topic_name_that_exceeds_the_maximum_le", // Should be truncated at 48 chars
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := topic.Sanitize(tt.input)
			assert.Equal(t, tt.expected, result)

			// Verify sanitized name is within length limit
			assert.LessOrEqual(t, len(result), 48)

			// Verify no consecutive underscores
			assert.NotContains(t, result, "__")

			// Verify no leading/trailing underscores
			if len(result) > 0 && result != "unnamed" {
				assert.NotEqual(t, '_', result[0])
				assert.NotEqual(t, '_', result[len(result)-1])
			}
		})
	}
}

func TestTopicMode_String(t *testing.T) {
	tests := []struct {
		mode     topic.Mode
		expected string
	}{
		{topic.ModeCore, "core"},
		{topic.ModeJetStream, "jetstream"},
		{topic.Mode(999), "unknown"}, // Invalid mode
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.mode.String())
		})
	}
}

func TestTopicOptions_Defaults(t *testing.T) {
	t.Run("zero value options", func(t *testing.T) {
		var opts topic.Options

		assert.Equal(t, topic.Mode(0), opts.Mode)           // TopicModeCore
		assert.Equal(t, topic.Retention(0), opts.Retention) // RetentionEphemeral
		assert.Equal(t, int64(0), opts.MaxBytes)
		assert.Equal(t, int64(0), opts.MaxMessages)
		assert.Equal(t, time.Duration(0), opts.MaxAge)
		assert.Equal(t, 0, opts.Replicas)
		assert.Equal(t, topic.DiscardPolicy(0), opts.DiscardPolicy) // DiscardOld
		// assert.Empty(t, opts.SubjectOverride)
		// assert.Nil(t, opts.Codec)
	})
}

func TestTopicOptions_Configuration(t *testing.T) {
	t.Run("jetstream configuration", func(t *testing.T) {
		opts := topic.Options{
			Mode:          topic.ModeJetStream,
			Retention:     topic.RetentionDurable,
			MaxBytes:      1024 * 1024 * 1024, // 1GB
			MaxMessages:   1000000,
			MaxAge:        24 * time.Hour,
			Replicas:      1, // Single embedded server
			DiscardPolicy: topic.DiscardOld,
		}

		assert.Equal(t, topic.ModeJetStream, opts.Mode)
		assert.Equal(t, topic.RetentionDurable, opts.Retention)
		assert.Equal(t, int64(1024*1024*1024), opts.MaxBytes)
		assert.Equal(t, int64(1000000), opts.MaxMessages)
		assert.Equal(t, 24*time.Hour, opts.MaxAge)
		assert.Equal(t, 1, opts.Replicas)
		assert.Equal(t, topic.DiscardOld, opts.DiscardPolicy)
	})

	t.Run("core mode configuration", func(t *testing.T) {
		opts := topic.Options{
			Mode: topic.ModeCore,
			// SubjectOverride: "custom.subject.name",
			// Codec:           message.JSONCodec,
		}

		assert.Equal(t, topic.ModeCore, opts.Mode)
		// assert.Equal(t, "custom.subject.name", opts.SubjectOverride)
		// assert.Equal(t, message.JSONCodec, opts.Codec)
	})
}

func TestRetention_Constants(t *testing.T) {
	t.Run("retention values", func(t *testing.T) {
		assert.Equal(t, topic.Retention(0), topic.RetentionEphemeral)
		assert.Equal(t, topic.Retention(1), topic.RetentionDurable)
	})
}

func TestDiscardPolicy_Constants(t *testing.T) {
	t.Run("discard policy values", func(t *testing.T) {
		assert.Equal(t, topic.DiscardPolicy(0), topic.DiscardOld)
		assert.Equal(t, topic.DiscardPolicy(1), topic.DiscardNew)
	})
}

func TestTopic_TypeSafety(t *testing.T) {
	t.Run("topic type conversion", func(t *testing.T) {
		str := "test.topic"
		top := topic.Topic(str)

		assert.Equal(t, str, string(top))
		assert.IsType(t, topic.Topic(""), top)
	})

	t.Run("topic comparison", func(t *testing.T) {
		topic1 := topic.Topic("same.topic")
		topic2 := topic.Topic("same.topic")
		topic3 := topic.Topic("different.topic")

		assert.Equal(t, topic1, topic2)
		assert.NotEqual(t, topic1, topic3)
	})

	t.Run("topic as map key", func(t *testing.T) {
		// Test that Topic can be used as a map key
		topicMap := make(map[topic.Topic]string)

		topic1 := topic.Topic("key1")
		topic2 := topic.Topic("key2")

		topicMap[topic1] = "value1"
		topicMap[topic2] = "value2"

		assert.Equal(t, "value1", topicMap[topic1])
		assert.Equal(t, "value2", topicMap[topic2])
		assert.Len(t, topicMap, 2)
	})
}

func TestTopic_EdgeCases(t *testing.T) {
	t.Run("unicode topic names", func(t *testing.T) {
		unicodeTopic := topic.Topic("用户.事件.测试")
		assert.Equal(t, "用户.事件.测试", string(unicodeTopic))

		// Test sanitization of unicode
		sanitized := topic.Sanitize(string(unicodeTopic))
		assert.NotEmpty(t, sanitized)
	})

	t.Run("very long topic names", func(t *testing.T) {
		longTopic := topic.Topic("this.is.a.very.long.topic.name.that.might.cause.issues.if.not.handled.properly.in.the.system")
		assert.NotEmpty(t, string(longTopic))

		// Test sanitization handles long names
		sanitized := topic.Sanitize(string(longTopic))
		assert.LessOrEqual(t, len(sanitized), 48)
	})

	t.Run("topic with only dots", func(t *testing.T) {
		dotTopic := topic.Topic(".....")
		sanitized := topic.Sanitize(string(dotTopic))
		assert.Equal(t, "unnamed", sanitized)
	})
}

func TestTopic_NATS_Compatibility(t *testing.T) {
	t.Run("valid NATS subjects", func(t *testing.T) {
		validSubjects := []string{
			"user.events",
			"order.created.v1",
			"system.health.check",
			"a",
			"a.b.c.d.e.f.g",
		}

		for _, subject := range validSubjects {
			topic := topic.Topic(subject)
			assert.Equal(t, subject, string(topic))
		}
	})

	t.Run("topic to subject mapping", func(t *testing.T) {
		// Test that topics can be used directly as NATS subjects
		topic := topic.Topic("user.events")
		subject := string(topic)

		assert.Equal(t, "user.events", subject)
		assert.NotContains(t, subject, " ")  // No spaces
		assert.NotContains(t, subject, "\t") // No tabs
		assert.NotContains(t, subject, "\n") // No newlines
	})
}

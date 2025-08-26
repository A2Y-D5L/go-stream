package topic

import (
	"testing"
)

func TestNewPattern(t *testing.T) {
	tests := []struct {
		pattern string
		valid   bool
	}{
		{"*", true},
		{">", true},
		{"events.*", true},
		{"events.>", true},
		{"events.*.commands", true},
		{"", false},
		{"events.>.commands", false},
		{"events.user>", false},
	}

	for _, test := range tests {
		pattern, err := NewPattern(test.pattern)
		if test.valid && err != nil {
			t.Errorf("Pattern %s should be valid but got error: %v", test.pattern, err)
		}
		if !test.valid && err == nil {
			t.Errorf("Pattern %s should be invalid but no error returned", test.pattern)
		}
		if test.valid && pattern == nil {
			t.Errorf("Pattern %s should return non-nil pattern", test.pattern)
		}
	}
}

func TestPattern_Match(t *testing.T) {
	tests := []struct {
		pattern string
		topic   string
		match   bool
	}{
		{"*", "events", true},
		{"*", "commands", true},
		{"*", "events.user", false},
		{">", "events", true},
		{">", "events.user", true},
		{">", "events.user.created", true},
		{"events.*", "events.user", true},
		{"events.*", "events.order", true},
		{"events.*", "events", false},
		{"events.*", "events.user.created", false},
		{"events.>", "events.user", true},
		{"events.>", "events.user.created", true},
		{"events.>", "events", false},
		{"events.>", "commands.user", false},
		{"events.*.commands", "events.user.commands", true},
		{"events.*.commands", "events.order.commands", true},
		{"events.*.commands", "events.commands", false},
		{"events.*.commands", "events.user.order.commands", false},
	}

	for _, test := range tests {
		pattern, err := NewPattern(test.pattern)
		if err != nil {
			t.Fatalf("Failed to create pattern %s: %v", test.pattern, err)
		}

		match := pattern.Match(Topic(test.topic))
		if match != test.match {
			t.Errorf("Pattern %s vs topic %s: expected %v, got %v",
				test.pattern, test.topic, test.match, match)
		}
	}
}

func TestPattern_String(t *testing.T) {
	patternStr := "events.*"
	pattern, err := NewPattern(patternStr)
	if err != nil {
		t.Fatalf("Failed to create pattern: %v", err)
	}

	if pattern.String() != patternStr {
		t.Errorf("Expected pattern string %s, got %s", patternStr, pattern.String())
	}
}

func TestValidatePattern(t *testing.T) {
	tests := []struct {
		pattern string
		valid   bool
	}{
		{"*", true},
		{">", true},
		{"events.*", true},
		{"events.>", true},
		{"events.*.commands", true},
		{"", false},
		{"events..commands", false},
		{"events.>.commands", false},
		{"events.user>", false},
		{"events.>extra", false},
	}

	for _, test := range tests {
		err := ValidatePattern(test.pattern)
		if test.valid && err != nil {
			t.Errorf("Pattern %s should be valid but got error: %v", test.pattern, err)
		}
		if !test.valid && err == nil {
			t.Errorf("Pattern %s should be invalid but no error returned", test.pattern)
		}
	}
}

func TestMatchMany(t *testing.T) {
	patterns := []*Pattern{}

	pattern1, _ := NewPattern("events.*")
	pattern2, _ := NewPattern("commands.*")
	patterns = append(patterns, pattern1, pattern2)

	tests := []struct {
		topic string
		match bool
	}{
		{"events.user", true},
		{"commands.order", true},
		{"queries.user", false},
		{"responses.order", false},
	}

	for _, test := range tests {
		match := MatchMany(Topic(test.topic), patterns...)
		if match != test.match {
			t.Errorf("Topic %s: expected %v, got %v", test.topic, test.match, match)
		}
	}
}

func TestFilterTopics(t *testing.T) {
	topics := []Topic{
		"events.user.created",
		"events.order.placed",
		"commands.user.create",
		"commands.order.place",
		"queries.user.get",
	}

	pattern, err := NewPattern("events.>")
	if err != nil {
		t.Fatalf("Failed to create pattern: %v", err)
	}

	filtered := FilterTopics(topics, pattern)
	expected := 2 // events.user.created, events.order.placed

	if len(filtered) != expected {
		t.Errorf("Expected %d filtered topics, got %d", expected, len(filtered))
	}

	for _, topic := range filtered {
		if !pattern.Match(topic) {
			t.Errorf("Filtered topic %s should match pattern", topic)
		}
	}
}

func TestCommonPatterns(t *testing.T) {
	tests := []struct {
		pattern *Pattern
		topic   string
		match   bool
	}{
		{CommonPatterns.All, "anything", true},
		{CommonPatterns.Events, "events.user", true},
		{CommonPatterns.Events, "commands.user", false},
		{CommonPatterns.Commands, "commands.user", true},
		{CommonPatterns.Commands, "events.user", false},
		{CommonPatterns.Queries, "queries.user", true},
		{CommonPatterns.Queries, "events.user", false},
		{CommonPatterns.Responses, "responses.user", true},
		{CommonPatterns.Responses, "queries.user", false},
	}

	for _, test := range tests {
		match := test.pattern.Match(Topic(test.topic))
		if match != test.match {
			t.Errorf("Pattern %s vs topic %s: expected %v, got %v",
				test.pattern.String(), test.topic, test.match, match)
		}
	}
}

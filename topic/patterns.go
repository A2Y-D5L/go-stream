package topic

import (
	"regexp"
	"strings"
)

// Pattern represents a topic pattern for matching multiple topics
type Pattern struct {
	pattern string
	regex   *regexp.Regexp
}

// NewPattern creates a new topic pattern
// Supports NATS-style wildcards:
// - * matches exactly one token
// - > matches one or more tokens at the end
func NewPattern(pattern string) (*Pattern, error) {
	if err := ValidatePattern(pattern); err != nil {
		return nil, err
	}

	// Convert NATS pattern to regex
	regexPattern := convertToRegex(pattern)
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return nil, err
	}

	return &Pattern{
		pattern: pattern,
		regex:   regex,
	}, nil
}

// Match checks if the pattern matches the given topic
func (p *Pattern) Match(topic Topic) bool {
	return p.regex.MatchString(string(topic))
}

// String returns the string representation of the pattern
func (p *Pattern) String() string {
	return p.pattern
}

// ValidatePattern validates a topic pattern
func ValidatePattern(pattern string) error {
	if pattern == "" {
		return ErrInvalidTopic
	}

	// Check for invalid characters and patterns
	tokens := strings.Split(pattern, ".")
	for i, token := range tokens {
		if token == "" && i < len(tokens)-1 {
			return ErrInvalidTopic
		}
		
		// > can only be at the end
		if strings.Contains(token, ">") && i != len(tokens)-1 {
			return ErrInvalidTopic
		}
		
		// > must be the entire token
		if strings.Contains(token, ">") && token != ">" {
			return ErrInvalidTopic
		}
	}

	return nil
}

// convertToRegex converts NATS-style patterns to regex
func convertToRegex(pattern string) string {
	// Escape regex special characters except our wildcards
	escaped := regexp.QuoteMeta(pattern)
	
	// Replace escaped wildcards with regex equivalents
	escaped = strings.ReplaceAll(escaped, "\\*", "[^.]+")
	escaped = strings.ReplaceAll(escaped, "\\>", ".*")
	
	// Anchor the pattern
	return "^" + escaped + "$"
}

// MatchMany checks if any of the given patterns match the topic
func MatchMany(topic Topic, patterns ...*Pattern) bool {
	for _, pattern := range patterns {
		if pattern.Match(topic) {
			return true
		}
	}
	return false
}

// FilterTopics filters topics by pattern
func FilterTopics(topics []Topic, pattern *Pattern) []Topic {
	var matched []Topic
	for _, topic := range topics {
		if pattern.Match(topic) {
			matched = append(matched, topic)
		}
	}
	return matched
}

// CommonPatterns provides commonly used topic patterns
var CommonPatterns = struct {
	All       *Pattern
	Events    *Pattern
	Commands  *Pattern
	Queries   *Pattern
	Responses *Pattern
}{
	All:       mustPattern("*"),
	Events:    mustPattern("events.*"),
	Commands:  mustPattern("commands.*"),
	Queries:   mustPattern("queries.*"),
	Responses: mustPattern("responses.*"),
}

// mustPattern creates a pattern and panics on error (for initialization)
func mustPattern(pattern string) *Pattern {
	p, err := NewPattern(pattern)
	if err != nil {
		panic(err)
	}
	return p
}

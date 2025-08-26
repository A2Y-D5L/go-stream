package message

import (
	"fmt"
	"strconv"
	"time"
)

// HeaderKey represents a message header key
type HeaderKey string

// Standard header keys
const (
	HeaderContentType     HeaderKey = "content-type"
	HeaderContentEncoding HeaderKey = "content-encoding"
	HeaderTimestamp       HeaderKey = "timestamp"
	HeaderMessageID       HeaderKey = "message-id"
	HeaderCorrelationID   HeaderKey = "correlation-id"
	HeaderReplyTo         HeaderKey = "reply-to"
	HeaderRetryCount      HeaderKey = "retry-count"
	HeaderExpiration      HeaderKey = "expiration"
)

// Headers provides convenience methods for working with message headers
type Headers map[string]string

// NewHeaders creates a new headers map
func NewHeaders() Headers {
	return make(Headers)
}

// Set sets a header value
func (h Headers) Set(key HeaderKey, value string) {
	h[string(key)] = value
}

// Get retrieves a header value
func (h Headers) Get(key HeaderKey) string {
	return h[string(key)]
}

// Has checks if a header exists
func (h Headers) Has(key HeaderKey) bool {
	_, exists := h[string(key)]
	return exists
}

// Delete removes a header
func (h Headers) Delete(key HeaderKey) {
	delete(h, string(key))
}

// SetContentType sets the content type header
func (h Headers) SetContentType(contentType string) {
	h.Set(HeaderContentType, contentType)
}

// GetContentType gets the content type header
func (h Headers) GetContentType() string {
	return h.Get(HeaderContentType)
}

// SetTimestamp sets the timestamp header
func (h Headers) SetTimestamp(t time.Time) {
	h.Set(HeaderTimestamp, t.Format(time.RFC3339Nano))
}

// GetTimestamp gets the timestamp header as a time.Time
func (h Headers) GetTimestamp() (time.Time, error) {
	ts := h.Get(HeaderTimestamp)
	if ts == "" {
		return time.Time{}, fmt.Errorf("timestamp header not found")
	}
	return time.Parse(time.RFC3339Nano, ts)
}

// SetMessageID sets the message ID header
func (h Headers) SetMessageID(id string) {
	h.Set(HeaderMessageID, id)
}

// GetMessageID gets the message ID header
func (h Headers) GetMessageID() string {
	return h.Get(HeaderMessageID)
}

// SetCorrelationID sets the correlation ID header
func (h Headers) SetCorrelationID(id string) {
	h.Set(HeaderCorrelationID, id)
}

// GetCorrelationID gets the correlation ID header
func (h Headers) GetCorrelationID() string {
	return h.Get(HeaderCorrelationID)
}

// SetReplyTo sets the reply-to header
func (h Headers) SetReplyTo(topic string) {
	h.Set(HeaderReplyTo, topic)
}

// GetReplyTo gets the reply-to header
func (h Headers) GetReplyTo() string {
	return h.Get(HeaderReplyTo)
}

// SetRetryCount sets the retry count header
func (h Headers) SetRetryCount(count int) {
	h.Set(HeaderRetryCount, strconv.Itoa(count))
}

// GetRetryCount gets the retry count header
func (h Headers) GetRetryCount() (int, error) {
	countStr := h.Get(HeaderRetryCount)
	if countStr == "" {
		return 0, nil
	}
	return strconv.Atoi(countStr)
}

// SetExpiration sets the expiration header
func (h Headers) SetExpiration(expiration time.Time) {
	h.Set(HeaderExpiration, expiration.Format(time.RFC3339))
}

// GetExpiration gets the expiration header
func (h Headers) GetExpiration() (time.Time, error) {
	exp := h.Get(HeaderExpiration)
	if exp == "" {
		return time.Time{}, fmt.Errorf("expiration header not found")
	}
	return time.Parse(time.RFC3339, exp)
}

// Clone creates a copy of the headers
func (h Headers) Clone() Headers {
	clone := NewHeaders()
	for k, v := range h {
		clone[k] = v
	}
	return clone
}

// ToMap returns the headers as a map[string]string
func (h Headers) ToMap() map[string]string {
	return map[string]string(h)
}

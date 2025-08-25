package observability

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// SpanKind represents the type of span
type SpanKind int

const (
	// SpanKindInternal represents internal operations
	SpanKindInternal SpanKind = iota
	// SpanKindClient represents client operations
	SpanKindClient
	// SpanKindServer represents server operations
	SpanKindServer
	// SpanKindProducer represents message producer operations
	SpanKindProducer
	// SpanKindConsumer represents message consumer operations
	SpanKindConsumer
)

// SpanStatus represents the status of a span
type SpanStatus int

const (
	// SpanStatusUnset indicates the span status is not set
	SpanStatusUnset SpanStatus = iota
	// SpanStatusOK indicates the span completed successfully
	SpanStatusOK
	// SpanStatusError indicates the span completed with an error
	SpanStatusError
)

// Span represents a tracing span
type Span interface {
	// SetName sets the span name
	SetName(name string)
	
	// SetAttribute sets a span attribute
	SetAttribute(key string, value interface{})
	
	// SetStatus sets the span status
	SetStatus(status SpanStatus, description string)
	
	// AddEvent adds an event to the span
	AddEvent(name string, attributes map[string]interface{})
	
	// End completes the span
	End()
	
	// Context returns the span context
	Context() SpanContext
	
	// IsRecording returns true if the span is recording
	IsRecording() bool
}

// SpanContext contains span context information
type SpanContext struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
	Flags   byte   `json:"flags"`
}

// Tracer interface defines the contract for distributed tracing
type Tracer interface {
	// Start creates a new span
	Start(ctx context.Context, operationName string, opts ...SpanOption) (context.Context, Span)
	
	// Extract extracts span context from a carrier
	Extract(ctx context.Context, carrier map[string]string) (context.Context, error)
	
	// Inject injects span context into a carrier
	Inject(ctx context.Context, carrier map[string]string) error
}

// SpanOption configures span creation
type SpanOption func(*SpanConfig)

// SpanConfig holds span configuration
type SpanConfig struct {
	Kind       SpanKind
	Parent     SpanContext
	Attributes map[string]interface{}
}

// WithSpanKind sets the span kind
func WithSpanKind(kind SpanKind) SpanOption {
	return func(c *SpanConfig) {
		c.Kind = kind
	}
}

// WithAttributes sets span attributes
func WithAttributes(attrs map[string]interface{}) SpanOption {
	return func(c *SpanConfig) {
		if c.Attributes == nil {
			c.Attributes = make(map[string]interface{})
		}
		for k, v := range attrs {
			c.Attributes[k] = v
		}
	}
}

// WithParent sets the parent span context
func WithParent(parent SpanContext) SpanOption {
	return func(c *SpanConfig) {
		c.Parent = parent
	}
}

// InMemorySpan is a simple in-memory span implementation
type InMemorySpan struct {
	mu          sync.RWMutex
	name        string
	context     SpanContext
	kind        SpanKind
	status      SpanStatus
	description string
	attributes  map[string]interface{}
	events      []SpanEvent
	startTime   time.Time
	endTime     time.Time
	ended       bool
}

// SpanEvent represents an event within a span
type SpanEvent struct {
	Name       string                 `json:"name"`
	Timestamp  time.Time              `json:"timestamp"`
	Attributes map[string]interface{} `json:"attributes"`
}

// NewInMemorySpan creates a new in-memory span
func NewInMemorySpan(name string, context SpanContext, kind SpanKind) *InMemorySpan {
	return &InMemorySpan{
		name:       name,
		context:    context,
		kind:       kind,
		status:     SpanStatusUnset,
		attributes: make(map[string]interface{}),
		events:     make([]SpanEvent, 0),
		startTime:  time.Now(),
	}
}

// SetName sets the span name
func (s *InMemorySpan) SetName(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.name = name
}

// SetAttribute sets a span attribute
func (s *InMemorySpan) SetAttribute(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attributes[key] = value
}

// SetStatus sets the span status
func (s *InMemorySpan) SetStatus(status SpanStatus, description string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
	s.description = description
}

// AddEvent adds an event to the span
func (s *InMemorySpan) AddEvent(name string, attributes map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	event := SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: make(map[string]interface{}),
	}
	
	for k, v := range attributes {
		event.Attributes[k] = v
	}
	
	s.events = append(s.events, event)
}

// End completes the span
func (s *InMemorySpan) End() {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if !s.ended {
		s.endTime = time.Now()
		s.ended = true
	}
}

// Context returns the span context
func (s *InMemorySpan) Context() SpanContext {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.context
}

// IsRecording returns true if the span is recording
func (s *InMemorySpan) IsRecording() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.ended
}

// InMemoryTracer is a simple in-memory tracer implementation
type InMemoryTracer struct {
	mu    sync.RWMutex
	spans map[string]*InMemorySpan
}

// NewInMemoryTracer creates a new in-memory tracer
func NewInMemoryTracer() *InMemoryTracer {
	return &InMemoryTracer{
		spans: make(map[string]*InMemorySpan),
	}
}

// Start creates a new span
func (t *InMemoryTracer) Start(ctx context.Context, operationName string, opts ...SpanOption) (context.Context, Span) {
	config := &SpanConfig{
		Kind:       SpanKindInternal,
		Attributes: make(map[string]interface{}),
	}
	
	for _, opt := range opts {
		opt(config)
	}
	
	spanContext := SpanContext{
		TraceID: generateTraceID(),
		SpanID:  generateSpanID(),
		Flags:   0,
	}
	
	span := NewInMemorySpan(operationName, spanContext, config.Kind)
	
	// Set initial attributes
	for k, v := range config.Attributes {
		span.SetAttribute(k, v)
	}
	
	t.mu.Lock()
	t.spans[spanContext.SpanID] = span
	t.mu.Unlock()
	
	// Store span in context
	ctx = context.WithValue(ctx, spanContextKey{}, spanContext)
	
	return ctx, span
}

// Extract extracts span context from a carrier
func (t *InMemoryTracer) Extract(ctx context.Context, carrier map[string]string) (context.Context, error) {
	traceID, exists := carrier["trace-id"]
	if !exists {
		return ctx, nil
	}
	
	spanID, exists := carrier["span-id"]
	if !exists {
		return ctx, nil
	}
	
	spanContext := SpanContext{
		TraceID: traceID,
		SpanID:  spanID,
		Flags:   0,
	}
	
	return context.WithValue(ctx, spanContextKey{}, spanContext), nil
}

// Inject injects span context into a carrier
func (t *InMemoryTracer) Inject(ctx context.Context, carrier map[string]string) error {
	spanCtx, ok := ctx.Value(spanContextKey{}).(SpanContext)
	if !ok {
		return fmt.Errorf("no span context found in context")
	}
	
	carrier["trace-id"] = spanCtx.TraceID
	carrier["span-id"] = spanCtx.SpanID
	
	return nil
}

// GetSpans returns all spans (for testing)
func (t *InMemoryTracer) GetSpans() map[string]*InMemorySpan {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	spans := make(map[string]*InMemorySpan)
	for k, v := range t.spans {
		spans[k] = v
	}
	return spans
}

// spanContextKey is used as a key for span context in context.Context
type spanContextKey struct{}

// generateTraceID generates a random trace ID
func generateTraceID() string {
	return fmt.Sprintf("trace-%d", time.Now().UnixNano())
}

// generateSpanID generates a random span ID
func generateSpanID() string {
	return fmt.Sprintf("span-%d", time.Now().UnixNano())
}

// defaultTracer is the package-level tracer
var defaultTracer Tracer = NewInMemoryTracer()

// SetDefaultTracer sets the package-level tracer
func SetDefaultTracer(tracer Tracer) {
	defaultTracer = tracer
}

// GetDefaultTracer returns the package-level tracer
func GetDefaultTracer() Tracer {
	return defaultTracer
}

// StartSpan is a convenience function to start a span with the default tracer
func StartSpan(ctx context.Context, operationName string, opts ...SpanOption) (context.Context, Span) {
	return defaultTracer.Start(ctx, operationName, opts...)
}

// SpanFromContext extracts the span context from a context
func SpanFromContext(ctx context.Context) (SpanContext, bool) {
	spanCtx, ok := ctx.Value(spanContextKey{}).(SpanContext)
	return spanCtx, ok
}

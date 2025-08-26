package observability

import (
	"maps"
	"context"
	"sync"
	"time"
)

// MetricType represents the type of metric
type MetricType int

const (
	// Counter metrics only increase
	Counter MetricType = iota
	// Gauge metrics can go up or down
	Gauge
	// Histogram metrics track distributions
	Histogram
)

// Metric represents a single metric measurement
type Metric struct {
	Name      string            `json:"name"`
	Type      MetricType        `json:"type"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels"`
	Timestamp time.Time         `json:"timestamp"`
}

// MetricsCollector interface defines the contract for metrics collection
type MetricsCollector interface {
	// Counter operations
	IncrementCounter(name string, labels map[string]string)
	IncrementCounterBy(name string, value float64, labels map[string]string)

	// Gauge operations
	SetGauge(name string, value float64, labels map[string]string)
	IncrementGauge(name string, labels map[string]string)
	DecrementGauge(name string, labels map[string]string)

	// Histogram operations
	RecordHistogram(name string, value float64, labels map[string]string)

	// Metric retrieval
	GetMetrics() []Metric
	GetMetric(name string, labels map[string]string) (*Metric, bool)

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// InMemoryMetricsCollector is a simple in-memory metrics collector
type InMemoryMetricsCollector struct {
	mu      sync.RWMutex
	metrics map[string]*Metric
	running bool
}

// NewInMemoryMetricsCollector creates a new in-memory metrics collector
func NewInMemoryMetricsCollector() *InMemoryMetricsCollector {
	return &InMemoryMetricsCollector{
		metrics: make(map[string]*Metric),
	}
}

// IncrementCounter increments a counter metric by 1
func (c *InMemoryMetricsCollector) IncrementCounter(name string, labels map[string]string) {
	c.IncrementCounterBy(name, 1.0, labels)
}

// IncrementCounterBy increments a counter metric by the specified value
func (c *InMemoryMetricsCollector) IncrementCounterBy(name string, value float64, labels map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.metricKey(name, labels)
	metric, exists := c.metrics[key]
	if !exists {
		metric = &Metric{
			Name:      name,
			Type:      Counter,
			Value:     0,
			Labels:    copyLabels(labels),
			Timestamp: time.Now(),
		}
		c.metrics[key] = metric
	}

	metric.Value += value
	metric.Timestamp = time.Now()
}

// SetGauge sets a gauge metric to the specified value
func (c *InMemoryMetricsCollector) SetGauge(name string, value float64, labels map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.metricKey(name, labels)
	metric := &Metric{
		Name:      name,
		Type:      Gauge,
		Value:     value,
		Labels:    copyLabels(labels),
		Timestamp: time.Now(),
	}
	c.metrics[key] = metric
}

// IncrementGauge increments a gauge metric by 1
func (c *InMemoryMetricsCollector) IncrementGauge(name string, labels map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.metricKey(name, labels)
	metric, exists := c.metrics[key]
	if !exists {
		metric = &Metric{
			Name:      name,
			Type:      Gauge,
			Value:     0,
			Labels:    copyLabels(labels),
			Timestamp: time.Now(),
		}
		c.metrics[key] = metric
	}

	metric.Value++
	metric.Timestamp = time.Now()
}

// DecrementGauge decrements a gauge metric by 1
func (c *InMemoryMetricsCollector) DecrementGauge(name string, labels map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.metricKey(name, labels)
	metric, exists := c.metrics[key]
	if !exists {
		metric = &Metric{
			Name:      name,
			Type:      Gauge,
			Value:     0,
			Labels:    copyLabels(labels),
			Timestamp: time.Now(),
		}
		c.metrics[key] = metric
	}

	metric.Value--
	metric.Timestamp = time.Now()
}

// RecordHistogram records a value in a histogram metric
func (c *InMemoryMetricsCollector) RecordHistogram(name string, value float64, labels map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	key := c.metricKey(name, labels)
	metric := &Metric{
		Name:      name,
		Type:      Histogram,
		Value:     value,
		Labels:    copyLabels(labels),
		Timestamp: time.Now(),
	}
	c.metrics[key] = metric
}

// GetMetrics returns all metrics
func (c *InMemoryMetricsCollector) GetMetrics() []Metric {
	c.mu.RLock()
	defer c.mu.RUnlock()

	metrics := make([]Metric, 0, len(c.metrics))
	for _, metric := range c.metrics {
		metrics = append(metrics, *metric)
	}
	return metrics
}

// GetMetric returns a specific metric
func (c *InMemoryMetricsCollector) GetMetric(name string, labels map[string]string) (*Metric, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	key := c.metricKey(name, labels)
	metric, exists := c.metrics[key]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent external modification
	return &Metric{
		Name:      metric.Name,
		Type:      metric.Type,
		Value:     metric.Value,
		Labels:    copyLabels(metric.Labels),
		Timestamp: metric.Timestamp,
	}, true
}

// Start starts the metrics collector
func (c *InMemoryMetricsCollector) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.running = true
	return nil
}

// Stop stops the metrics collector
func (c *InMemoryMetricsCollector) Stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.running = false
	return nil
}

// metricKey generates a unique key for a metric based on name and labels
func (c *InMemoryMetricsCollector) metricKey(name string, labels map[string]string) string {
	key := name
	for k, v := range labels {
		key += ":" + k + "=" + v
	}

	return key
}

// copyLabels creates a copy of the labels map
func copyLabels(labels map[string]string) map[string]string {
	if labels == nil {
		return nil
	}

	copy := make(map[string]string, len(labels))
	maps.Copy(copy, labels)
	return copy
}

// defaultMetricsCollector is the package-level metrics collector
var defaultMetricsCollector MetricsCollector = NewInMemoryMetricsCollector()

// SetDefaultMetricsCollector sets the package-level metrics collector
func SetDefaultMetricsCollector(collector MetricsCollector) {
	defaultMetricsCollector = collector
}

// GetDefaultMetricsCollector returns the package-level metrics collector
func GetDefaultMetricsCollector() MetricsCollector {
	return defaultMetricsCollector
}

// StreamMetrics contains standard metrics for stream operations
type StreamMetrics struct {
	collector MetricsCollector
}

// NewStreamMetrics creates a new stream metrics instance
func NewStreamMetrics(collector MetricsCollector) *StreamMetrics {
	if collector == nil {
		collector = defaultMetricsCollector
	}
	return &StreamMetrics{collector: collector}
}

// RecordMessagePublished records a published message
func (m *StreamMetrics) RecordMessagePublished(topic string, success bool) {
	labels := map[string]string{
		"topic":   topic,
		"success": boolToString(success),
	}
	m.collector.IncrementCounter("stream_messages_published_total", labels)
}

// RecordMessageReceived records a received message
func (m *StreamMetrics) RecordMessageReceived(topic string) {
	labels := map[string]string{"topic": topic}
	m.collector.IncrementCounter("stream_messages_received_total", labels)
}

// RecordMessageProcessingTime records message processing time
func (m *StreamMetrics) RecordMessageProcessingTime(topic string, duration time.Duration) {
	labels := map[string]string{"topic": topic}
	m.collector.RecordHistogram("stream_message_processing_duration_ms",
		float64(duration.Nanoseconds())/1e6, labels)
}

// RecordActiveSubscriptions records the number of active subscriptions
func (m *StreamMetrics) RecordActiveSubscriptions(topic string, count int) {
	labels := map[string]string{"topic": topic}
	m.collector.SetGauge("stream_active_subscriptions", float64(count), labels)
}

// RecordConnectionStatus records connection status
func (m *StreamMetrics) RecordConnectionStatus(connected bool) {
	value := 0.0
	if connected {
		value = 1.0
	}
	m.collector.SetGauge("stream_connection_status", value, nil)
}

// boolToString converts boolean to string
func boolToString(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

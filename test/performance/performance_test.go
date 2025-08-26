package performance

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream"
	"github.com/a2y-d5l/go-stream/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPerformance_EndToEndThroughput tests end-to-end message throughput
func TestPerformance_EndToEndThroughput(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("performance.throughput.test")

	// Capture performance metrics
	var publishStart, publishEnd, receiveStart, receiveEnd time.Time
	messageCount := int64(10000)
	receivedCount := int64(0)

	received := make(chan struct{}, int(messageCount))

	subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		count := atomic.AddInt64(&receivedCount, 1)
		if count == 1 {
			receiveStart = time.Now()
		}
		received <- struct{}{}
		if count == messageCount {
			receiveEnd = time.Now()
		}
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	// Wait for subscription to establish
	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Measure publish performance
	publishStart = time.Now()
	for i := range messageCount {
		msg := stream.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "performance-message-%d", i),
			Headers: map[string]string{
				"stream.Message-ID": fmt.Sprintf("perf-%d", i),
			},
			Time: time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}
	publishEnd = time.Now()

	// Wait for all messages to be received
	for i := range messageCount {
		select {
		case <-received:
			// stream.Message received
		case <-time.After(30 * time.Second):
			t.Fatalf("Timeout waiting for message %d", i+1)
		}
	}

	// Calculate and verify performance metrics
	publishDuration := publishEnd.Sub(publishStart)
	receiveDuration := receiveEnd.Sub(receiveStart)
	publishThroughput := float64(messageCount) / publishDuration.Seconds()
	receiveThroughput := float64(messageCount) / receiveDuration.Seconds()

	t.Logf("Performance Metrics:")
	t.Logf("  Messages: %d", messageCount)
	t.Logf("  Publish Duration: %v", publishDuration)
	t.Logf("  Receive Duration: %v", receiveDuration)
	t.Logf("  Publish Throughput: %.2f msg/sec", publishThroughput)
	t.Logf("  Receive Throughput: %.2f msg/sec", receiveThroughput)

	// Verify reasonable performance (adjust thresholds based on requirements)
	assert.Greater(t, publishThroughput, 1000.0, "Publish throughput should be > 1000 msg/sec")
	assert.Greater(t, receiveThroughput, 1000.0, "Receive throughput should be > 1000 msg/sec")
	assert.Equal(t, messageCount, atomic.LoadInt64(&receivedCount), "All messages should be received")
}

// TestPerformance_MemoryUsage tests memory usage patterns under load
func TestPerformance_MemoryUsage(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := stream.Topic("performance.memory.test")

	// Baseline memory measurement
	runtime.GC()
	var baselineMemStats runtime.MemStats
	runtime.ReadMemStats(&baselineMemStats)

	processedCount := int64(0)

	subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		atomic.AddInt64(&processedCount, 1)
		return nil
	})

	sub, err := s.Subscribe(top, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages and measure memory usage
	messageCount := 50000
	for i := range messageCount {
		msg := stream.Message{
			Topic: top,
			Data:  fmt.Appendf(nil, "memory-test-message-%d-with-some-content-to-make-it-larger", i),
			Headers: map[string]string{
				"Content-Type":      "text/plain",
				"stream.Message-ID": fmt.Sprintf("mem-%d", i),
				"Extra-Header":      fmt.Sprintf("extra-data-%d", i),
			},
			Time: time.Now(),
		}
		err := s.Publish(ctx, top, msg)
		require.NoError(t, err)

		// Measure memory periodically
		if i%10000 == 0 && i > 0 {
			runtime.GC()
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			memoryUsed := memStats.Alloc - baselineMemStats.Alloc
			t.Logf("After %d messages: Memory used: %d bytes", i, memoryUsed)
		}
	}

	// Wait for processing to complete
	time.Sleep(5 * time.Second)

	// Final memory measurement
	runtime.GC()
	var finalMemStats runtime.MemStats
	runtime.ReadMemStats(&finalMemStats)

	finalMemoryUsed := finalMemStats.Alloc - baselineMemStats.Alloc
	processed := atomic.LoadInt64(&processedCount)

	t.Logf("Final Memory Metrics:")
	t.Logf("  Messages processed: %d", processed)
	t.Logf("  Memory baseline: %d bytes", baselineMemStats.Alloc)
	t.Logf("  Memory final: %d bytes", finalMemStats.Alloc)
	t.Logf("  Memory used: %d bytes", finalMemoryUsed)
	t.Logf("  Memory per message: %.2f bytes", float64(finalMemoryUsed)/float64(processed))

	// Verify memory usage is reasonable (adjust threshold based on requirements)
	memoryPerMessage := float64(finalMemoryUsed) / float64(processed)
	assert.Less(t, memoryPerMessage, 1000.0, "Memory per message should be reasonable")
	assert.Equal(t, int64(messageCount), processed, "All messages should be processed")
}

// TestPerformance_Latency tests message latency patterns
func TestPerformance_Latency(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("performance.latency.test")

	var latencies []time.Duration
	var latencyMutex sync.Mutex

	subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		receiveTime := time.Now()

		// Extract send time from message headers
		if sendTimeStr, exists := msg.Headers["Send-Time"]; exists {
			if sendTime, err := time.Parse(time.RFC3339Nano, sendTimeStr); err == nil {
				latency := receiveTime.Sub(sendTime)
				latencyMutex.Lock()
				latencies = append(latencies, latency)
				latencyMutex.Unlock()
			}
		}
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages with timing information
	messageCount := 1000
	for i := range messageCount {
		sendTime := time.Now()
		msg := stream.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "latency-test-%d", i),
			Headers: map[string]string{
				"Send-Time":         sendTime.Format(time.RFC3339Nano),
				"stream.Message-ID": fmt.Sprintf("lat-%d", i),
			},
			Time: sendTime,
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)

		// Add small delay to avoid overwhelming the system
		time.Sleep(time.Millisecond)
	}

	// Wait for all messages to be processed
	time.Sleep(5 * time.Second)

	// Analyze latency metrics
	latencyMutex.Lock()
	defer latencyMutex.Unlock()

	require.Greater(t, len(latencies), messageCount/2, "Should have received most messages")

	// Calculate latency statistics
	var totalLatency time.Duration
	minLatency := latencies[0]
	maxLatency := latencies[0]

	for _, latency := range latencies {
		totalLatency += latency
		if latency < minLatency {
			minLatency = latency
		}
		if latency > maxLatency {
			maxLatency = latency
		}
	}

	avgLatency := totalLatency / time.Duration(len(latencies))

	// Calculate percentiles (simple implementation)
	latenciesMs := make([]float64, len(latencies))
	for i, lat := range latencies {
		latenciesMs[i] = float64(lat.Nanoseconds()) / 1e6 // Convert to milliseconds
	}

	t.Logf("Latency Metrics:")
	t.Logf("  Messages analyzed: %d", len(latencies))
	t.Logf("  Average latency: %v", avgLatency)
	t.Logf("  Min latency: %v", minLatency)
	t.Logf("  Max latency: %v", maxLatency)

	// Verify latency is reasonable (adjust thresholds based on requirements)
	assert.Less(t, avgLatency, 100*time.Millisecond, "Average latency should be < 100ms")
	assert.Less(t, maxLatency, 500*time.Millisecond, "Max latency should be < 500ms")
}

// TestPerformance_ScalingCharacteristics tests how performance scales with load
func TestPerformance_ScalingCharacteristics(t *testing.T) {
	s := helpers.CreateTestStream(t)
	for _, scale := range []struct {
		name         string
		messageCount int
		subscribers  int
	}{
		{"Light Load", 1000, 1},
		{"Medium Load", 5000, 3},
		{"Heavy Load", 10000, 5},
	} {
		t.Run(scale.name, func(t *testing.T) {
			topic := stream.Topic(fmt.Sprintf("performance.scaling.%s", scale.name))
			receivedCount := int64(0)

			// Create multiple subscribers
			var subs []stream.Subscription
			for range scale.subscribers {
				subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
					atomic.AddInt64(&receivedCount, 1)
					return nil
				})

				sub, err := s.Subscribe(topic, subscriber)
				require.NoError(t, err)
				subs = append(subs, sub)
			}

			defer func() {
				for _, sub := range subs {
					sub.Stop()
				}
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			// Measure performance for this scale
			startTime := time.Now()

			for i := range scale.messageCount {
				msg := stream.Message{
					Topic: topic,
					Data:  fmt.Appendf(nil, "scaling-test-%d", i),
					Time:  time.Now(),
				}
				err := s.Publish(ctx, topic, msg)
				require.NoError(t, err)
			}

			// Wait for processing
			time.Sleep(3 * time.Second)

			endTime := time.Now()
			duration := endTime.Sub(startTime)
			throughput := float64(scale.messageCount) / duration.Seconds()

			received := atomic.LoadInt64(&receivedCount)
			expectedReceived := int64(scale.messageCount * scale.subscribers)

			t.Logf("Scale %s Results:", scale.name)
			t.Logf("  Messages: %d, Subscribers: %d", scale.messageCount, scale.subscribers)
			t.Logf("  Duration: %v", duration)
			t.Logf("  Throughput: %.2f msg/sec", throughput)
			t.Logf("  Expected receptions: %d, Actual: %d", expectedReceived, received)

			// Verify scaling behavior
			assert.Greater(t, throughput, 100.0, "Throughput should be reasonable under load")
			assert.GreaterOrEqual(t, received, expectedReceived/2, "Should receive most messages")
		})
	}
}

// TestPerformance_ConcurrentOperations tests performance under concurrent operations
func TestPerformance_ConcurrentOperations(t *testing.T) {
	s := helpers.CreateTestStream(t)

	topics := []stream.Topic{
		stream.Topic("performance.concurrent.topic1"),
		stream.Topic("performance.concurrent.topic2"),
		stream.Topic("performance.concurrent.topic3"),
	}

	totalReceived := int64(0)
	var subs []stream.Subscription

	// Create subscribers for all topics
	for _, topic := range topics {
		subscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
			atomic.AddInt64(&totalReceived, 1)
			// Simulate some processing time
			time.Sleep(time.Microsecond * 100)
			return nil
		})

		sub, err := s.Subscribe(topic, subscriber)
		require.NoError(t, err)
		subs = append(subs, sub)
	}

	defer func() {
		for _, sub := range subs {
			sub.Stop()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Concurrent publishing to multiple topics
	messagesPerTopic := 1000
	startTime := time.Now()

	var wg sync.WaitGroup
	for _, topic := range topics {
		wg.Add(1)
		go func(t stream.Topic) {
			defer wg.Done()
			for i := range messagesPerTopic {
				msg := stream.Message{
					Topic: t,
					Data:  fmt.Appendf(nil, "concurrent-msg-%d", i),
					Time:  time.Now(),
				}
				err := s.Publish(ctx, t, msg)
				if err != nil {
					// In a real scenario, we might want to handle this differently
					return
				}
			}
		}(topic)
	}

	wg.Wait()

	// Wait for processing
	time.Sleep(5 * time.Second)

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	totalMessages := len(topics) * messagesPerTopic
	throughput := float64(totalMessages) / duration.Seconds()

	received := atomic.LoadInt64(&totalReceived)

	t.Logf("Concurrent Operations Results:")
	t.Logf("  Topics: %d", len(topics))
	t.Logf("  Messages per topic: %d", messagesPerTopic)
	t.Logf("  Total messages: %d", totalMessages)
	t.Logf("  Duration: %v", duration)
	t.Logf("  Throughput: %.2f msg/sec", throughput)
	t.Logf("  Messages received: %d", received)

	// Verify concurrent performance
	assert.Greater(t, throughput, 500.0, "Concurrent throughput should be reasonable")
	assert.GreaterOrEqual(t, received, int64(totalMessages)/2, "Should receive most messages")
}

// TestPerformance_BackpressureHandling tests system behavior under backpressure
func TestPerformance_BackpressureHandling(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("performance.backpressure.test")

	processedCount := int64(0)

	// Slow subscriber to create backpressure
	sub, err := s.Subscribe(topic, stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		// Simulate slow processing
		time.Sleep(10 * time.Millisecond)
		atomic.AddInt64(&processedCount, 1)
		return nil
	}))
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages rapidly to create backpressure
	messageCount := 500
	publishStart := time.Now()

	for i := range messageCount {
		msg := stream.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "backpressure-test-%d", i),
			Time:  time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	publishDuration := time.Since(publishStart)

	// Wait for processing to catch up
	time.Sleep(10 * time.Second)

	processed := atomic.LoadInt64(&processedCount)
	publishThroughput := float64(messageCount) / publishDuration.Seconds()

	t.Logf("Backpressure Test Results:")
	t.Logf("  Messages published: %d", messageCount)
	t.Logf("  Messages processed: %d", processed)
	t.Logf("  Publish duration: %v", publishDuration)
	t.Logf("  Publish throughput: %.2f msg/sec", publishThroughput)
	t.Logf("  Processing ratio: %.2f%%", float64(processed)/float64(messageCount)*100)

	// Verify backpressure handling
	assert.Greater(t, publishThroughput, 100.0, "Should be able to publish under backpressure")
	assert.Greater(t, processed, int64(messageCount)/4, "Should process some messages despite backpressure")
}

// TestPerformance_RegressionDetection tests for performance regressions
func TestPerformance_RegressionDetection(t *testing.T) {
	s := helpers.CreateTestStream(t)
	topic := stream.Topic("performance.regression.test")

	// Expected baseline performance (adjust based on your requirements)
	expectedMinThroughput := 1000.0 // messages per second
	expectedMaxLatency := 50 * time.Millisecond

	receivedCount := int64(0)
	var totalLatency time.Duration
	var latencyMutex sync.Mutex

	sub, err := s.Subscribe(topic, stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		receiveTime := time.Now()

		if sendTimeStr, exists := msg.Headers["Send-Time"]; exists {
			if sendTime, err := time.Parse(time.RFC3339Nano, sendTimeStr); err == nil {
				latency := receiveTime.Sub(sendTime)
				latencyMutex.Lock()
				totalLatency += latency
				latencyMutex.Unlock()
			}
		}

		atomic.AddInt64(&receivedCount, 1)
		return nil
	}))
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Performance test
	messageCount := 5000
	startTime := time.Now()

	for i := range messageCount {
		sendTime := time.Now()
		msg := stream.Message{
			Topic: topic,
			Data:  fmt.Appendf(nil, "regression-test-%d", i),
			Headers: map[string]string{
				"Send-Time": sendTime.Format(time.RFC3339Nano),
			},
			Time: sendTime,
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	throughput := float64(messageCount) / duration.Seconds()

	received := atomic.LoadInt64(&receivedCount)
	avgLatency := totalLatency / time.Duration(received)

	t.Logf("Regression Test Results:")
	t.Logf("  Messages: %d", messageCount)
	t.Logf("  Received: %d", received)
	t.Logf("  Duration: %v", duration)
	t.Logf("  Throughput: %.2f msg/sec", throughput)
	t.Logf("  Average latency: %v", avgLatency)
	t.Logf("  Expected min throughput: %.2f msg/sec", expectedMinThroughput)
	t.Logf("  Expected max latency: %v", expectedMaxLatency)

	// Regression checks
	assert.GreaterOrEqual(t, throughput, expectedMinThroughput,
		"Throughput regression detected: %.2f < %.2f", throughput, expectedMinThroughput)
	assert.LessOrEqual(t, avgLatency, expectedMaxLatency,
		"Latency regression detected: %v > %v", avgLatency, expectedMaxLatency)
	assert.GreaterOrEqual(t, received, int64(messageCount*90/100),
		"stream.Message delivery regression detected")
}

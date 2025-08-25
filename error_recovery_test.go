package stream

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrorRecovery tests system recovery after various types of failures
func TestErrorRecovery(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("error.recovery.test")
	
	// Test recovery from subscriber errors
	errorCount := int64(0)
	recoveredCount := int64(0)
	
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		count := atomic.AddInt64(&errorCount, 1)
		if count <= 3 {
			return errors.New("simulated processing error")
		}
		atomic.AddInt64(&recoveredCount, 1)
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages that will initially fail
	for i := 0; i < 10; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("message-%d", i)),
			Time:  time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing and recovery
	time.Sleep(3 * time.Second)

	// Verify some messages were processed after recovery
	assert.Greater(t, atomic.LoadInt64(&recoveredCount), int64(0), 
		"Should have recovered and processed some messages")
}

// TestPartialFailureHandling tests handling of partial system failures
func TestPartialFailureHandling(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("partial.failure.test")
	
	successCount := int64(0)
	failureCount := int64(0)
	
	// Subscriber that fails on specific messages
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		msgStr := string(msg.Data)
		if msgStr == "fail-message" {
			atomic.AddInt64(&failureCount, 1)
			return errors.New("intentional failure")
		}
		atomic.AddInt64(&successCount, 1)
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish mix of successful and failing messages
	messages := []string{
		"success-1", "fail-message", "success-2", 
		"success-3", "fail-message", "success-4",
	}

	for _, msgData := range messages {
		msg := Message{
			Topic: topic,
			Data:  []byte(msgData),
			Time:  time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify partial success handling
	assert.Equal(t, int64(4), atomic.LoadInt64(&successCount), 
		"Should process successful messages")
	assert.Equal(t, int64(2), atomic.LoadInt64(&failureCount), 
		"Should encounter expected failures")
}

// TestGracefulDegradation tests system behavior under degraded conditions
func TestGracefulDegradation(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("degradation.test")
	
	// Simulate gradually degrading subscriber performance
	processedCount := int64(0)
	
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		count := atomic.LoadInt64(&processedCount)
		
		// Simulate increasing processing delay (degradation)
		delay := time.Duration(count*10) * time.Millisecond
		if delay > 100*time.Millisecond {
			delay = 100 * time.Millisecond // Cap the delay
		}
		
		time.Sleep(delay)
		atomic.AddInt64(&processedCount, 1)
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages at constant rate
	messageCount := 20
	for i := 0; i < messageCount; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("degraded-message-%d", i)),
			Time:  time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
		
		time.Sleep(50 * time.Millisecond) // Constant publish rate
	}

	// Wait for processing
	time.Sleep(5 * time.Second)

	// Verify system handled degradation gracefully
	processed := atomic.LoadInt64(&processedCount)
	assert.Greater(t, processed, int64(0), "Should process some messages despite degradation")
	t.Logf("Processed %d out of %d messages under degraded conditions", processed, messageCount)
}

// TestPanicRecovery tests recovery from panics in subscribers
func TestPanicRecovery(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("panic.recovery.test")
	
	processedCount := int64(0)
	panicCount := int64(0)
	
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		msgStr := string(msg.Data)
		if msgStr == "panic-trigger" {
			atomic.AddInt64(&panicCount, 1)
			panic("simulated panic in subscriber")
		}
		atomic.AddInt64(&processedCount, 1)
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages including panic triggers
	messages := []string{
		"normal-1", "panic-trigger", "normal-2",
		"normal-3", "panic-trigger", "normal-4",
	}

	for _, msgData := range messages {
		msg := Message{
			Topic: topic,
			Data:  []byte(msgData),
			Time:  time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Verify system continued processing after panics
	assert.Equal(t, int64(4), atomic.LoadInt64(&processedCount), 
		"Should process normal messages despite panics")
	t.Logf("Handled %d panics gracefully", atomic.LoadInt64(&panicCount))
}

// TestCircuitBreakerPattern tests circuit breaker behavior for failing services
func TestCircuitBreakerPattern(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("circuit.breaker.test")
	
	failureCount := int64(0)
	successCount := int64(0)
	circuitOpen := int64(0)
	
	// Simulate circuit breaker pattern
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		// Check if circuit should be "open" (too many failures)
		failures := atomic.LoadInt64(&failureCount)
		if failures > 3 && failures < 10 {
			atomic.StoreInt64(&circuitOpen, 1)
			return errors.New("circuit breaker open - too many failures")
		}
		
		// Reset circuit after some time
		if failures >= 10 {
			atomic.StoreInt64(&circuitOpen, 0)
			atomic.StoreInt64(&failureCount, 0) // Reset failure count
		}
		
		// Simulate some operations failing
		msgStr := string(msg.Data)
		if msgStr == "failing-operation" && atomic.LoadInt64(&circuitOpen) == 0 {
			atomic.AddInt64(&failureCount, 1)
			return errors.New("operation failed")
		}
		
		if atomic.LoadInt64(&circuitOpen) == 0 {
			atomic.AddInt64(&successCount, 1)
		}
		
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Send messages that will trigger circuit breaker
	for i := 0; i < 15; i++ {
		var msgData string
		if i < 5 {
			msgData = "failing-operation"
		} else {
			msgData = fmt.Sprintf("normal-operation-%d", i)
		}
		
		msg := Message{
			Topic: topic,
			Data:  []byte(msgData),
			Time:  time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
		
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for processing
	time.Sleep(2 * time.Second)

	// Verify circuit breaker behavior
	success := atomic.LoadInt64(&successCount)
	t.Logf("Circuit breaker test: %d successful operations", success)
	assert.Greater(t, success, int64(0), "Should have some successful operations")
}

// TestResourceExhaustionRecovery tests recovery from resource exhaustion
func TestResourceExhaustionRecovery(t *testing.T) {
	s := CreateTestStream(t)
	topic := Topic("resource.exhaustion.test")
	
	processedCount := int64(0)
	rejectedCount := int64(0)
	
	// Simulate resource exhaustion and recovery
	subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
		count := atomic.LoadInt64(&processedCount)
		
		// Simulate resource exhaustion after 5 messages
		if count >= 5 && count < 10 {
			atomic.AddInt64(&rejectedCount, 1)
			return errors.New("resource exhausted - rejecting message")
		}
		
		// Simulate resource recovery after message 10
		atomic.AddInt64(&processedCount, 1)
		time.Sleep(50 * time.Millisecond) // Simulate processing time
		return nil
	})

	sub, err := s.Subscribe(topic, subscriber)
	require.NoError(t, err)
	defer sub.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish messages to trigger exhaustion and recovery
	for i := 0; i < 15; i++ {
		msg := Message{
			Topic: topic,
			Data:  []byte(fmt.Sprintf("resource-test-%d", i)),
			Time:  time.Now(),
		}
		err := s.Publish(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Verify recovery from resource exhaustion
	processed := atomic.LoadInt64(&processedCount)
	rejected := atomic.LoadInt64(&rejectedCount)
	
	assert.Greater(t, processed, int64(0), "Should process messages before and after exhaustion")
	assert.Greater(t, rejected, int64(0), "Should reject messages during exhaustion")
	
	t.Logf("Resource exhaustion test: %d processed, %d rejected", processed, rejected)
}

// TestCascadingFailureContainment tests containment of cascading failures
func TestCascadingFailureContainment(t *testing.T) {
	s := CreateTestStream(t)

	// Multiple topics to simulate different services
	topics := []Topic{
		Topic("service.a"),
		Topic("service.b"), 
		Topic("service.c"),
	}
	
	// Track processing per service
	serviceCounts := make(map[string]*int64)
	serviceErrors := make(map[string]*int64)
	
	var subs []Subscription
	
	for _, topic := range topics {
		serviceName := string(topic)
		serviceCounts[serviceName] = new(int64)
		serviceErrors[serviceName] = new(int64)
		
		// Create subscriber for each service
		subscriber := SubscriberFunc(func(ctx context.Context, msg Message) error {
			serviceName := string(msg.Topic)
			
			// Service A fails after 3 messages (simulate cascading failure)
			if serviceName == "service.a" {
				count := atomic.LoadInt64(serviceCounts[serviceName])
				if count >= 3 {
					atomic.AddInt64(serviceErrors[serviceName], 1)
					return errors.New("service A cascading failure")
				}
			}
			
			atomic.AddInt64(serviceCounts[serviceName], 1)
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

	// Publish to all services
	for i := 0; i < 10; i++ {
		for _, topic := range topics {
			msg := Message{
				Topic: topic,
				Data:  []byte(fmt.Sprintf("message-%d", i)),
				Time:  time.Now(),
			}
			err := s.Publish(ctx, topic, msg)
			require.NoError(t, err)
		}
	}

	// Wait for processing
	time.Sleep(3 * time.Second)

	// Verify that failure in service A didn't affect other services
	assert.Equal(t, int64(3), atomic.LoadInt64(serviceCounts["service.a"]), 
		"Service A should process 3 messages before failing")
	assert.Equal(t, int64(10), atomic.LoadInt64(serviceCounts["service.b"]), 
		"Service B should process all messages despite A's failure")
	assert.Equal(t, int64(10), atomic.LoadInt64(serviceCounts["service.c"]), 
		"Service C should process all messages despite A's failure")
	
	assert.Greater(t, atomic.LoadInt64(serviceErrors["service.a"]), int64(0), 
		"Service A should have errors")
}

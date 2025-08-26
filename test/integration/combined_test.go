package stream

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a2y-d5l/go-stream"
	"github.com/a2y-d5l/go-stream/sub"
	"github.com/a2y-d5l/go-stream/test/helpers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCombined_HighLoadWithPanicRecovery tests that the system can handle
// high load even when some subscribers panic
func TestCombined_HighLoadWithPanicRecovery(t *testing.T) {
	s := helpers.CreateTestStream(t)
	top := stream.Topic("test.combined.highload.panic")

	// Set up multiple subscribers with different behaviors
	processedCount := int64(0)
	panicCount := int64(0)

	// Normal subscriber that processes messages
	normalSubscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		atomic.AddInt64(&processedCount, 1)
		return nil
	})

	// Panicking subscriber that panics on every 10th message
	panicSubscriber := stream.SubscriberFunc(func(ctx context.Context, msg stream.Message) error {
		count := atomic.AddInt64(&panicCount, 1)
		if count%10 == 0 {
			panic(fmt.Sprintf("intentional panic on message %d", count))
		}
		atomic.AddInt64(&processedCount, 1)
		return nil
	})

	// Subscribe with both normal and panicking subscribers
	normalSub, err := s.Subscribe(top, normalSubscriber,
		sub.WithConcurrency(5),
		sub.WithQueueGroupName("normal"))
	require.NoError(t, err)
	defer normalSub.Stop()

	panicSub, err := s.Subscribe(top, panicSubscriber,
		sub.WithConcurrency(3),
		sub.WithQueueGroupName("panic"))
	require.NoError(t, err)
	defer panicSub.Stop()

	// Wait for subscriptions to be ready
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Publish many messages under high load
	numMessages := 1000
	startTime := time.Now()

	for i := 0; i < numMessages; i++ {
		msg := stream.Message{
			Topic: top,
			Data:  []byte(fmt.Sprintf("high load message %d", i)),
			Time:  time.Now(),
		}
		err = s.Publish(ctx, top, msg)
		require.NoError(t, err)
	}

	publishDuration := time.Since(startTime)

	// Allow time for processing
	time.Sleep(5 * time.Second)

	finalProcessed := atomic.LoadInt64(&processedCount)
	finalPanicCount := atomic.LoadInt64(&panicCount)

	t.Logf("Published %d messages in %v", numMessages, publishDuration)
	t.Logf("Processed %d messages total", finalProcessed)
	t.Logf("Panic subscriber handled %d messages", finalPanicCount)

	// Should process most messages despite some panics
	assert.Greater(t, finalProcessed, int64(float64(numMessages)*0.8),
		"Should process at least 80% of messages despite panics")

	// Panic subscriber should have handled some messages (and panicked on some)
	assert.Greater(t, finalPanicCount, int64(50),
		"Panic subscriber should have processed many messages")

	// System should still be functional - publish one more message
	finalMsg := stream.Message{
		Topic: top,
		Data:  []byte("final test message"),
		Time:  time.Now(),
	}
	err = s.Publish(ctx, top, finalMsg)
	assert.NoError(t, err, "System should still be functional after high load with panics")
}

# JetStream Durable Messaging Example

This example demonstrates JetStream's durable messaging capabilities with persistent storage, replay functionality, and advanced consumer patterns.

## What It Does

- **Order Processing**: Durable consumer processes order events with acknowledgments
- **User Verification**: Handles user registration events  
- **Analytics Service**: Replays historical order events for analysis
- **Event Publishers**: Generates order events and user registrations
- **Persistent Storage**: All data stored on disk for replay across restarts

## Key Features Demonstrated

- ✅ **Durable Consumers**: Resume processing from where they left off after restart
- ✅ **Message Persistence**: All messages stored on disk in `./jetstream-store`
- ✅ **Replay Capability**: Analytics service replays historical messages
- ✅ **Acknowledgments**: Manual message acknowledgment with retry logic
- ✅ **Different Start Positions**: New messages vs. replay from timestamp
- ✅ **Concurrent Processing**: Configurable concurrency per consumer

## Architecture

```
Publishers ──┬── orders.events ──┬── Order Processor (durable: order-processor)
             │                   └── Analytics Service (durable: analytics-processor)
             │
             └── users.registrations ── User Verification Service
```

### Storage Layout
```
./jetstream-store/
├── jetstream/
│   ├── streams/
│   ├── consumers/
│   └── snapshots/
```

## Running the Example

```bash
# From the jetstream/ directory
go run main.go
```

## Expected Output

```
JetStream Durable Messaging Example
This example demonstrates:
- Durable message storage and replay
- Consumer acknowledgments  
- Stream persistence across restarts
- Different delivery semantics
Press Ctrl+C to stop

📦 Starting Order Processing Service (durable consumer)
👤 Starting User Verification Service
📤 Starting Order Event Publisher
📤 Starting User Registration Publisher
📊 Starting Analytics Service (replaying historical data)

📤 Published order event: order-1 (status: created)
📦 Processing order: order-1 (status: created, amount: $11.99)
   ✅ Order order-1 validated and ready for payment

📤 Published user registration: User 1 (needs verification)
👤 📧 Sending verification email to User 1 (user1@example.com)

📊 Analytics: Order order-1 processed (status: created, timestamp: 14:25:30)
...
```

## Code Structure

### Order Processing Service
- **Durable Consumer**: `order-processor` - survives restarts
- **Manual Acknowledgment**: Messages redelivered on failure
- **Retry Logic**: Up to 3 delivery attempts per message
- **Status Handling**: Processes different order lifecycle events

### User Verification Service  
- **Ephemeral Consumer**: Processes current user registrations
- **Concurrent Processing**: Handles 2 users simultaneously
- **Verification Logic**: Sends emails for unverified users

### Analytics Service
- **Historical Replay**: Processes messages from 1 hour ago
- **Durable Consumer**: `analytics-processor` - maintains position
- **Sequential Processing**: Ensures ordered analytics processing
- **Time-based Start**: Replays from specific timestamp

### Event Publishers
- **Order Events**: Creates order lifecycle events every 3 seconds
- **User Registrations**: Publishes new user signups every 5 seconds
- **Realistic Data**: Varied order amounts and statuses

## Key Concepts

### Durable Consumers
```go
stream.WithDurable("consumer-name")  // Consumer survives restarts
stream.WithAckPolicy(stream.AckManual)  // Manual acknowledgment required
```

### Message Replay
```go
stream.WithStartAt(stream.DeliverByStartTime)  // Replay from timestamp
stream.WithStartTime(startTime)               // Specify start time
```

### Acknowledgment Patterns
- **Auto Ack**: Messages automatically acknowledged (default)
- **Manual Ack**: Explicit acknowledgment required for message completion
- **Retry Logic**: Failed messages redelivered up to `MaxDeliver` times

### Start Positions
- **DeliverNew**: Only new messages (default)
- **DeliverByStartTime**: Replay from specific timestamp
- **DeliverLastPerSubject**: Last message per subject

## Persistence Benefits

### Durability
- **Message Storage**: All messages persisted to disk
- **Consumer State**: Consumer progress tracked and restored
- **Crash Recovery**: No message loss on application restart

### Replay Capabilities
- **Historical Analysis**: Process old messages for analytics
- **Error Recovery**: Replay messages after fixing bugs
- **Load Testing**: Replay production traffic for testing

### Operational Advantages
- **Zero Downtime**: Consumers resume seamlessly after restart
- **Message Guarantees**: At-least-once delivery with acknowledgments
- **Audit Trails**: Complete message history preserved

## Production Considerations

### Storage Management
```bash
# Monitor storage usage
du -sh ./jetstream-store

# Clean up old messages (configure retention in production)
# JetStream supports automatic retention policies
```

### Performance Tuning
- **Batch Acknowledgments**: Ack multiple messages together
- **Consumer Concurrency**: Balance throughput vs. ordering
- **Storage Limits**: Configure appropriate retention policies

### Monitoring
- **Consumer Lag**: Track how far behind consumers are
- **Acknowledgment Rates**: Monitor ack/nack ratios
- **Storage Growth**: Monitor disk usage trends

## Testing Restart Behavior

1. **Start the application**:
   ```bash
   go run main.go
   ```

2. **Let it run for 30 seconds** to generate some messages

3. **Stop with Ctrl+C** - notice the persistence message

4. **Restart immediately**:
   ```bash
   go run main.go
   ```

5. **Observe**: Durable consumers resume from where they left off

## Common Use Cases

JetStream is ideal for:
- **Event Sourcing**: Maintaining complete event history
- **Financial Systems**: Guaranteed message delivery for transactions
- **Audit Logging**: Persistent audit trails
- **Data Pipelines**: Reliable data processing workflows
- **Microservice Events**: Durable event-driven architecture

## Error Scenarios

### Message Processing Failures
```go
// Simulate processing failure
return fmt.Errorf("processing failed")  // Message will be retried
```

### Consumer Failures
- Failed messages are retried up to `MaxDeliver` times
- After max retries, messages can go to Dead Letter Queue (DLQ)
- Consumer state persisted for restart recovery

## Advanced Features

### Dead Letter Queues
```go
stream.WithDLQ("failed.orders.events")  // Route failed messages
```

### Backoff Strategies
```go
stream.WithBackoff([]time.Duration{
    1 * time.Second,
    5 * time.Second,
    30 * time.Second,
})  // Exponential backoff for retries
```

### Flow Control
```go
stream.WithBufferSize(1000)  // Control memory usage
stream.WithBackpressure(stream.BackpressureBlock)  // Handle overload
```

## Troubleshooting

### Consumer Not Resuming
- Check durable consumer name consistency
- Verify JetStream is enabled and configured
- Ensure storage directory is writable

### Messages Not Persisting
- Confirm `TopicModeJetStream` is set
- Check storage directory permissions
- Verify disk space availability

### High Storage Usage
- Configure retention policies in production
- Monitor and clean up old streams
- Consider message compaction for key-value data

## Next Steps

- Configure retention policies for production use
- Implement Dead Letter Queue handling
- Add monitoring and alerting for consumer lag
- Explore JetStream clustering for high availability
- Implement stream and consumer management via admin APIs

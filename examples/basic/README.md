# Basic Pub/Sub Example

This example demonstrates the fundamental publish/subscribe pattern using go-stream with Core NATS mode.

## What It Does

- **Publisher**: Sends user data every 2 seconds to the `user.events` topic
- **Subscriber**: Listens for messages on `user.events` and logs received users
- **Data Format**: JSON-encoded `User` struct with ID, Name, and Email fields

## Key Features Demonstrated

- ✅ **Simple Setup**: Default configuration with embedded NATS server
- ✅ **Type-Safe JSON**: Using `PublishJSON` and `SubscribeJSON` helpers
- ✅ **Graceful Shutdown**: Proper resource cleanup with signal handling
- ✅ **Error Handling**: Comprehensive error handling and logging
- ✅ **Core Mode**: Using NATS Core for simple pub/sub (no persistence)

## Running the Example

```bash
# From the basic/ directory
go run main.go
```

## Expected Output

```
Basic pub/sub example started
Press Ctrl+C to stop
Starting subscriber for user.events...
Starting publisher...
📤 Published user: user-1
📨 Received user: ID=user-1, Name=User 1, Email=user1@example.com
📤 Published user: user-2
📨 Received user: ID=user-2, Name=User 2, Email=user2@example.com
...
```

## Code Structure

### Publisher
- Creates `User` structs with incremental IDs
- Uses `PublishJSON()` for automatic JSON encoding
- Publishes every 2 seconds with a ticker
- Handles publish errors gracefully

### Subscriber
- Uses `SubscribeJSON()` for automatic JSON decoding
- Type-safe handler receives `User` structs directly
- Logs each received message with emoji indicators
- Runs concurrently with the publisher

### Shutdown
- Captures SIGINT/SIGTERM signals
- Gracefully closes the stream with proper cleanup
- Prevents resource leaks

## Common Use Cases

This pattern is ideal for:
- **Event Notifications**: Broadcasting system events
- **Real-time Updates**: Sending live data updates
- **Microservice Communication**: Decoupled service messaging
- **Activity Streams**: User activity broadcasting
- **Log Aggregation**: Distributing log events

## Performance Notes

- **Core Mode**: In-memory only, very fast but no persistence
- **Automatic Reconnection**: Built-in connection resilience
- **Type Safety**: Compile-time safety with generics
- **Minimal Overhead**: Direct NATS Core messaging

## Troubleshooting

### Port Already in Use
If you see port conflicts, the example uses a random port by default. Multiple instances can run simultaneously.

### Connection Issues
The example uses an embedded NATS server, so no external NATS installation is required.

### Memory Usage
This example creates new user objects continuously. In production, consider object pooling for high-frequency publishing.

## Next Steps

- Try the **request/reply** example for synchronous communication
- Explore the **jetstream** example for durable messaging
- Modify the `User` struct to add more fields
- Experiment with multiple subscribers for load distribution

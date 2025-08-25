# Go-Stream Examples

This directory contains examples demonstrating various usage patterns of the go-stream library.

## Examples

- **[basic/](basic/)** - Simple pub/sub patterns with error handling
- **[requestreply/](requestreply/)** - Request/reply patterns with timeouts
- **[jetstream/](jetstream/)** - Durable messaging with JetStream

## Prerequisites

1. Install Go 1.21+
2. Start NATS server (with JetStream for JetStream example):
   ```bash
   # Core NATS
   nats-server
   
   # Or with JetStream
   nats-server -js
   ```

## Running Examples

```bash
# Basic pub/sub
cd basic && go run main.go

# Request/reply
cd requestreply && go run main.go

# JetStream
cd jetstream && go run main.go
```

## Common Patterns

### Error Handling
Always handle errors from pub/sub operations:
```go
if err := stream.Publish(ctx, "topic", data); err != nil {
    log.Printf("Publish failed: %v", err)
}
```

### Graceful Shutdown
Use context cancellation for clean shutdown:
```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// Handle signals
quit := make(chan os.Signal, 1)
signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
go func() {
    <-quit
    cancel()
}()
```

### Configuration
Always configure timeouts and limits:
```go
stream, err := gostream.New(gostream.Config{
    NATS: nats.Options{
        Url:            nats.DefaultURL,
        MaxReconnect:   10,
        ReconnectWait:  time.Second,
    },
    RequestTimeout: 5 * time.Second,
    BufferSize:     1000,
})
```

## Troubleshooting

### Connection Issues
- Ensure NATS server is running on the expected port (4222 by default)
- Check firewall settings if connecting to remote NATS
- Verify NATS server configuration for JetStream examples

### Performance Tips
- Use buffering for high-throughput scenarios
- Configure appropriate timeouts for your use case
- Consider using JetStream for reliability requirements
- Monitor memory usage in long-running applications

### Common Errors
- `connection refused`: NATS server not running
- `context deadline exceeded`: Increase timeout values
- `subject invalid`: Check subject naming conventions
- `stream not found`: Ensure JetStream is enabled for JetStream examples

## Configuration Options

### Core Mode (Basic & Request/Reply)
```go
gostream.Config{
    NATS: nats.Options{
        Url:           "nats://localhost:4222",
        MaxReconnect:  10,
        ReconnectWait: time.Second,
    },
    Mode:           gostream.Core,
    RequestTimeout: 5 * time.Second,
    BufferSize:     1000,
}
```

### JetStream Mode
```go
gostream.Config{
    NATS: nats.Options{
        Url: "nats://localhost:4222",
    },
    Mode: gostream.JetStream,
    JetStream: &gostream.JetStreamConfig{
        StreamName: "MYSTREAM",
        Subjects:   []string{"events.>"},
        Retention:  nats.LimitsPolicy,
        MaxAge:     24 * time.Hour,
        MaxBytes:   1024 * 1024 * 1024, // 1GB
        MaxMsgs:    1000000,
        Replicas:   1,
    },
}
```

## Best Practices

1. **Always handle errors** - Check return values from all operations
2. **Use contexts** - Provide contexts for cancellation and timeouts
3. **Graceful shutdown** - Handle signals and clean up resources
4. **Type safety** - Use generics for type-safe message handling
5. **Resource management** - Close streams and cancel contexts appropriately
6. **Monitoring** - Log important events and errors
7. **Testing** - Test with both embedded and external NATS servers

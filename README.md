# go-stream

[![Go Reference](https://pkg.go.dev/badge/github.com/a2y-d5l/go-stream.svg)](https://pkg.go.dev/github.com/a2y-d5l/go-stream)
[![Go Report Card](https://goreportcard.com/badge/github.com/a2y-d5l/go-stream)](https://goreportcard.com/report/github.com/a2y-d5l/go-stream)

**go-stream** is a Go library that provides an embedded NATS server with type-safe, opinionated messaging patterns. It simplifies pub/sub, request/reply, and durable messaging by embedding NATS directly into your application—no external infrastructure required.

## 🎯 Why go-stream?

- **Zero Infrastructure**: Embedded NATS server starts automatically
- **Type Safety**: Generic-based APIs with compile-time safety
- **Opinionated**: Sensible defaults that "just work"
- **Production Ready**: Built on battle-tested NATS technology
- **Flexible**: Supports both Core NATS and JetStream modes

## ⚡ Quick Start

```bash
go get github.com/a2y-d5l/go-stream
```

### Basic Pub/Sub

```go
package main

import (
    "context"
    "fmt"
    
    stream "github.com/a2y-d5l/go-stream"
)

type User struct {
    ID   string `json:"id"`
    Name string `json:"name"`
}

func main() {
    // Create stream with embedded NATS server
    s, err := stream.New(context.Background())
    if err != nil {
        panic(err)
    }
    defer s.Close(context.Background())

    // Subscribe with type-safe JSON handling
    sub, err := stream.SubscribeJSON(s, "users", 
        func(ctx context.Context, user User) error {
            fmt.Printf("Received: %+v\n", user)
            return nil
        })
    if err != nil {
        panic(err)
    }
    defer sub.Stop()

    // Publish with automatic JSON encoding
    user := User{ID: "123", Name: "Alice"}
    err = s.PublishJSON(context.Background(), "users", user)
    if err != nil {
        panic(err)
    }
}
```

## 🏗️ Architecture

```
Your Application
├── Embedded NATS Server (automatic)
├── Client Connection (managed)
└── go-stream API (type-safe)
```

**No external dependencies.** **No configuration files.** **No separate processes.**

## 📚 Core Features

### 🔄 **Messaging Patterns**

| Pattern | Description | Use Case |
|---------|-------------|----------|
| **Pub/Sub** | Broadcast messages to multiple subscribers | Event notifications, real-time updates |
| **Request/Reply** | Synchronous request-response with load balancing | Service communication, RPC calls |
| **Queue Groups** | Distribute work across multiple workers | Load balancing, task processing |

### 💾 **Storage Modes**

| Mode | Persistence | Performance | Use Case |
|------|-------------|-------------|----------|
| **Core** | In-memory only | Ultra-fast | Real-time events, temporary data |
| **JetStream** | Disk-persistent | High-throughput | Durable messaging, event sourcing |

### 🛡️ **Built-in Features**

- ✅ **Automatic Reconnection** - Resilient to network issues
- ✅ **Graceful Shutdown** - Clean resource cleanup
- ✅ **Error Handling** - Comprehensive error propagation
- ✅ **Concurrency Control** - Configurable worker pools
- ✅ **Backpressure** - Multiple overflow policies
- ✅ **Type Safety** - Generic APIs prevent runtime errors

## 🚀 Usage Patterns

### Simple Pub/Sub

```go
// Publisher
err := s.PublishJSON(ctx, "events", MyEvent{Type: "user_created"})

// Subscriber  
sub, err := stream.SubscribeJSON(s, "events", handleEvent)
```

### Request/Reply with Timeout

```go
// Create a requester
requester := stream.NewJSONRequester[Request, Response](s, "add")

// Send request with timeout
response, err := requester.Request(ctx, Request{A: 5, B: 3}, 5*time.Second)
```

### Durable JetStream Messaging

```go
// Enable JetStream mode
s, err := stream.New(ctx, 
    stream.WithDefaultTopicMode(stream.TopicModeJetStream),
    stream.WithStoreDir("./data"))

// Durable consumer survives restarts
sub, err := stream.SubscribeJSON(s, "orders", processOrder,
    stream.WithDurable("order-processor"),
    stream.WithAckPolicy(stream.AckManual))
```

### Load Balancing with Queue Groups

```go
// Multiple workers share the load
for i := range 3 {
    stream.SubscribeJSON(s, "tasks", processTask,
        stream.WithQueueGroupName("workers"),
        stream.WithConcurrency(2))
}
```

### Examples

See [`examples/`](./examples/) for runnable examples:

| Example | Description | Key Concepts |
|---------|-------------|--------------|
| **[basic/](./examples/basic/)** | Simple pub/sub with JSON messages | Core concepts, error handling |
| **[requestreply/](./examples/requestreply/)** | Calculator service with load balancing | Request/reply, queue groups, timeouts |
| **[jetstream/](./examples/jetstream/)** | Order processing with persistence | Durable consumers, message replay |

```bash
# Run any example
cd examples/basic && go run main.go
cd examples/requestreply && go run main.go  
cd examples/jetstream && go run main.go
```

## ⚙️ Configuration

### Basic Configuration

```go
s, err := stream.New(ctx,
    stream.WithHost("127.0.0.1"),      // Server host
    stream.WithRandomPort(),           // Dynamic port
    stream.WithDefaultTopicMode(stream.TopicModeCore), // Core or JetStream
)
```

### Production Configuration

```go
s, err := stream.New(ctx,
    stream.WithStoreDir("/data/nats"),         // Persistent storage
    stream.WithMaxPayload(64*1024*1024),       // 64MB max message
    stream.WithConnectTimeout(10*time.Second), // Connection timeout
    stream.WithDrainTimeout(30*time.Second),   // Graceful shutdown timeout
)
```

### TLS Configuration

```go
tlsConfig := &tls.Config{
    Certificates: []tls.Certificate{cert},
    ClientAuth:   tls.RequireAndVerifyClientCert,
}

s, err := stream.New(ctx, stream.WithTLS(tlsConfig))
```

## 🔧 Advanced Features

### Custom Codecs

```go
// Default is JSON, but you can use custom codecs
s, err := stream.New(ctx, stream.WithDefaultCodec(MyCodec))
```

### Health Checks

```go
// Lightweight health check
if err := s.Healthy(ctx); err != nil {
    log.Printf("Stream unhealthy: %v", err)
}

// Deep health check with server status
status, err := s.DeepHealthCheck(ctx)
```

### Subscription Options

```go
sub, err := stream.SubscribeJSON(s, "topic", handler,
    stream.WithConcurrency(4),                    // 4 workers
    stream.WithBufferSize(1000),                  // Message buffer
    stream.WithBackpressure(stream.BackpressureDropOldest), // Overflow policy
    stream.WithMaxDeliver(3),                     // Retry limit
    stream.WithDurable("my-consumer"),            // Durable name
)
```

## 🏭 Production Considerations

### Performance

- **Core Mode**: ~1M+ msgs/sec for in-memory messaging
- **JetStream**: ~100K+ msgs/sec with persistence  
- **Memory**: Minimal overhead with embedded server
- **Latency**: Sub-millisecond for local messaging

### Monitoring

```go
// Built-in observability hooks
s, err := stream.New(ctx,
    stream.WithLogger(slog.New(handler)), // Custom logging
    // Add metrics/tracing via middleware
)
```

### Deployment

- **Single Binary**: Everything embedded, no external dependencies
- **Docker**: Works seamlessly in containers
- **Kubernetes**: Scales horizontally with persistent volumes
- **Cloud**: Deploy anywhere Go runs

## 🔍 Troubleshooting

### Common Issues

**Port conflicts?**
```go
stream.WithRandomPort() // Uses dynamic port allocation
```

**Messages not received?**
```go
// Ensure subscription is established before publishing
sub, err := stream.SubscribeJSON(s, "topic", handler)
time.Sleep(100 * time.Millisecond) // Allow subscription setup
```

**JetStream storage issues?**
```go
stream.WithStoreDir("./data") // Ensure directory is writable
```

For more troubleshooting, see the [examples README](./examples/README.md#troubleshooting).

## 🤝 Contributing

Contributions welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

Built on the amazing [NATS](https://nats.io/) messaging system. Thanks to the NATS team for creating such robust infrastructure.

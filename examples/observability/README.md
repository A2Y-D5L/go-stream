# go-stream Observability Integration

This example demonstrates the **observability package in its natural context** - integrated with real go-stream Stream, Publisher, and Subscriber operations in a complete e-commerce microservices scenario.

## What This Example Shows

### Real go-stream Integration
- **Actual Stream instance** with observability-enabled logging
- **Real Publishers** with publish operation logging and performance tracking
- **Real Subscribers** with message processing observability
- **Complete pipeline** showing order → payment → notification flow

### E-Commerce Microservices Flow
1. **Order Creation** - API Gateway publishes order events
2. **Order Processing** - Subscriber validates and processes orders
3. **Payment Processing** - Payment service handles transactions
4. **Notifications** - Completion notifications sent to users

### Observability Features in Action

#### Stream-Level Logging
```go
streamLogger := observability.NewStreamLogger(baseLogger, "ecommerce-stream")
streamLogger.LogConnection(ctx, "embedded://127.0.0.1", true)
streamLogger.LogSubscribe(ctx, "orders.created", "order-processors", err)
```

#### Publisher Observability
```go
publisherLogger := observability.NewPublisherLogger(streamLogger, "api-gateway")
publisherLogger.LogPublish(ctx, "orders.created", len(data), duration, err)
```

#### Subscriber Observability
```go
subscriberLogger := observability.NewSubscriberLogger(streamLogger, "order-processor")
subscriberLogger.LogMessageReceived(ctx, "orders.created", orderID, len(msg.Data))
subscriberLogger.LogMessageProcessed(ctx, "orders.created", orderID, duration, err)
```

## Running the Example

```bash
go run examples/observability/main.go
```

## Sample Output

The example produces structured JSON logs showing:

```json
{"time":"2025-08-25T17:02:51.516552-04:00","level":"INFO","msg":"stream connected","stream_id":"ecommerce-stream","nats.connection":"embedded://127.0.0.1","connected":true,"operation":"stream-connect"}

{"time":"2025-08-25T17:02:51.532731-04:00","level":"INFO","msg":"message published successfully","stream_id":"ecommerce-stream","publisher_id":"api-gateway","request_id":"req-001","nats.subject":"orders.created","bytes_processed":156,"publish_duration":16051041,"operation":"publish"}

{"time":"2025-08-25T17:02:51.583861-04:00","level":"INFO","msg":"message received","stream_id":"ecommerce-stream","subscriber_id":"order-processor","request_id":"req-001","order_id":"order-001","nats.subject":"orders.created","nats.message_id":"order-001","bytes_processed":156,"operation":"message-received"}
```

## Architecture Benefits

### Request Correlation
- Every log entry includes `request_id` enabling complete request tracing
- Same correlation flows through Stream → Publisher → Subscriber operations
- Critical for debugging distributed transaction flows

### Performance Monitoring
- Automatic logging of publish/subscribe operation timing
- Message size tracking for bandwidth analysis
- Processing duration monitoring across the pipeline

### Error Context
- Rich error information with retry strategies and impact assessment
- Failed payment scenarios demonstrate enhanced debugging capabilities
- Structured error fields enable automated alerting

### Operational Visibility
- Real-time monitoring of message processing pipelines
- Publisher and subscriber health tracking
- System-wide observability across microservices

## Real-World Value

This example demonstrates how the observability package transforms go-stream from a basic messaging library into an **enterprise-ready platform** with:

- **Production Observability** - Complete visibility into message flows
- **Performance Insights** - Automatic bottleneck detection and optimization guidance
- **Debugging Acceleration** - Rich context reduces Mean Time To Resolution (MTTR)
- **Operational Confidence** - Real-time health monitoring and alerting capabilities

The integration is **purpose-built for go-stream** with specialized loggers for Stream, Publisher, and Subscriber operations, providing the exact observability needed for production NATS-based microservices.

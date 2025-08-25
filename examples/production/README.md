# Production Example

This example demonstrates a production-ready streaming service using go-stream with comprehensive observability, error handling, and graceful shutdown.

## Features

- **Structured Logging**: JSON-formatted logs with contextual information
- **Metrics Collection**: In-memory metrics collector for monitoring message processing
- **Health Checks**: HTTP endpoints for health, readiness, and metrics
- **Graceful Shutdown**: Proper cleanup of resources on shutdown signals
- **Error Handling**: Comprehensive error handling and validation
- **Production Configuration**: Production-ready stream configuration

## Running the Example

```bash
cd examples/production
go run main.go
```

The service will start on port 8080 and provide the following endpoints:

- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint  
- `GET /metrics` - Metrics endpoint (JSON format)

## Architecture

The production example demonstrates:

1. **Service Structure**: Organized service with proper separation of concerns
2. **Event Processing**: Order event processing with status-based routing
3. **Observability**: Comprehensive logging and metrics collection
4. **HTTP Monitoring**: Health and metrics endpoints for external monitoring
5. **Configuration**: Production-ready stream configuration with timeouts and retry logic

## Order Processing Flow

The service processes order events through different stages:

1. **Order Created** → Publishes confirmation event
2. **Order Paid** → Triggers fulfillment process
3. **Order Shipped** → Sends customer notification
4. **Order Delivered** → Completes order and requests feedback
5. **Order Cancelled** → Processes refund

## Monitoring

The service provides comprehensive monitoring through:

- Structured JSON logs with correlation IDs
- Metrics for message processing times and counts
- Health checks for service availability
- Graceful shutdown with timeout handling

## Production Deployment

For production deployment, consider:

1. **External Monitoring**: Integrate with Prometheus/Grafana for metrics
2. **Log Aggregation**: Use ELK stack or similar for log collection
3. **Container Deployment**: Docker/Kubernetes deployment with health checks
4. **Load Balancing**: Multiple instances with proper load balancing
5. **Persistent Storage**: Configure JetStream for durable messaging

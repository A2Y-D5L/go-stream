# Request/Reply Example

This example demonstrates request/reply messaging patterns with load balancing using go-stream.

## What It Does

- **Multiple Servers**: 3 calculation servers handle math operations
- **Load Balancing**: Requests are distributed across servers using queue groups
- **Client Requests**: Sends calculation requests every second
- **Response Handling**: Waits for responses with timeout handling
- **Operations**: Supports addition, multiplication, and division

## Key Features Demonstrated

- ✅ **Request/Reply Pattern**: Asynchronous request-response communication
- ✅ **Load Balancing**: Queue groups distribute work across multiple servers
- ✅ **Concurrency**: Multiple workers per server for parallel processing
- ✅ **Timeout Handling**: Client-side timeouts for unresponsive requests
- ✅ **Error Handling**: Graceful error handling (e.g., division by zero)
- ✅ **Unique Request IDs**: Correlation between requests and responses

## Architecture

```
Client ──┬── calc.requests ──┬── Server 1 (calc-servers queue group)
         │                   ├── Server 2 (calc-servers queue group)  
         │                   └── Server 3 (calc-servers queue group)
         │
         └── calc.responses.{req-id} ←─── Response from any server
```

## Running the Example

```bash
# From the requestreply/ directory
go run main.go
```

## Expected Output

```
Request/Reply example started
This example demonstrates:
- Multiple calculation servers handling requests
- Load balancing across servers
- Request/reply pattern with timeouts
Press Ctrl+C to stop

🖥️  Starting calculation server: calc-server-1
🖥️  Starting calculation server: calc-server-2
🖥️  Starting calculation server: calc-server-3
📞 Starting client...
📤 Sending request: 45.23 add 7.89 (req: req-1)
🔢 [calc-server-2] Processed: 45.23 add 7.89 = 53.12 (req: req-1)
✅ Response: 53.12 (server: calc-server-2, req: req-1)
📤 Sending request: 12.34 multiply 5.67 (req: req-2)
🔢 [calc-server-1] Processed: 12.34 multiply 5.67 = 69.97 (req: req-2)
✅ Response: 69.97 (server: calc-server-1, req: req-2)
...
```

## Code Structure

### Calculation Servers
- **Queue Group**: `calc-servers` ensures load balancing
- **Concurrency**: Each server processes 2 requests simultaneously
- **Operations**: Handles add, multiply, divide operations
- **Processing Time**: Simulated random processing delay
- **Error Handling**: Validates operations and handles edge cases

### Client
- **Request Generation**: Creates random calculation requests
- **Response Subscription**: Subscribes to unique response topics
- **Timeout Handling**: 5-second timeout per request
- **Correlation**: Uses unique request IDs to match responses

### Message Flow
1. Client subscribes to response topic: `calc.responses.{req-id}`
2. Client publishes request to: `calc.requests`
3. Available server (from queue group) processes the request
4. Server publishes response to: `calc.responses.{req-id}`
5. Client receives response or times out

## Key Concepts

### Load Balancing
- **Queue Groups**: Only one server in the group receives each message
- **Fair Distribution**: NATS automatically distributes requests
- **Scalability**: Add more servers by joining the same queue group

### Request Correlation
- **Unique IDs**: Each request has a unique identifier
- **Response Topics**: Separate topic per request for response delivery
- **Isolation**: Responses don't interfere with each other

### Error Handling
- **Validation**: Servers validate operations and parameters
- **Timeouts**: Clients don't wait indefinitely for responses
- **Graceful Degradation**: Invalid requests return error responses

## Production Considerations

### Performance
- **Concurrent Workers**: Tune concurrency per server based on workload
- **Connection Pooling**: Use connection pooling for high-frequency requests
- **Batch Processing**: Consider batching for high-throughput scenarios

### Reliability
- **Retries**: Implement client-side retries for failed requests
- **Circuit Breakers**: Add circuit breakers for failing services
- **Health Checks**: Monitor server health and availability

### Monitoring
- **Request Tracing**: Track request latency and success rates
- **Server Metrics**: Monitor server utilization and processing times
- **Error Rates**: Track and alert on error patterns

## Common Use Cases

This pattern is ideal for:
- **Microservice APIs**: Service-to-service communication
- **Task Processing**: Distributed task execution
- **Data Processing**: Parallel data transformation
- **RPC Systems**: Remote procedure call implementations
- **Worker Pools**: Scalable background job processing

## Variations

### Synchronous Request/Reply
For true request/reply with built-in correlation, NATS provides:
```go
// Using NATS request/reply (not shown in this example)
response, err := nc.Request("calc.requests", requestData, 5*time.Second)
```

### Persistent Responses
For durable responses, use JetStream mode (see jetstream example).

## Troubleshooting

### Responses Not Received
- Check request ID correlation
- Verify response topic subscription timing
- Ensure servers are running and subscribed

### Load Balancing Not Working
- Verify all servers use the same queue group name
- Check that servers are successfully subscribed
- Monitor server health and connectivity

### High Latency
- Reduce simulated processing delay
- Increase server concurrency
- Check network connectivity and NATS performance

## Next Steps

- Try the **jetstream** example for durable request/reply
- Experiment with different queue group configurations
- Add authentication and authorization
- Implement request retries and circuit breakers
- Monitor performance with metrics and tracing

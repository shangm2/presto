# Design Document: Presto Coordinator Task Handling Migration

## Thread Pool to Event Loop Architecture

### Problem Statement

The current Presto coordinator task handling implementation uses a thread pool model that suffers from high
synchronization overhead, leading to excessive CPU usage when handling many concurrent tasks. This creates a bottleneck
in the coordinator, limiting query throughput and increasing query latency under high concurrency scenarios.

### Goals

- Reduce CPU usage on the Presto task scheduler by minimizing synchronization overhead
- Improve coordinator efficiency to handle more concurrent queries
- Decrease query queueing time and high-percentile latencies
- Maintain backward compatibility and system stability during migration

### Non-Goals

- Changing the core communication protocol between coordinator and worker
- Modifying worker-side task handling
- Rewriting other components of the query execution framework

### Current Architecture

The current `HttpRemoteTask` implementation:

- Uses a thread pool model for task handling
- Relies on explicit synchronization (`synchronized` methods and blocks) to protect shared state
- Creates many threads for task callbacks, error handling, and update scheduling
- Suffers from contention when multiple threads attempt to access the same task's state

Key issues with the current approach:

1. High synchronization overhead due to lock contention
2. Excessive context switching between threads
3. Thread creation costs when handling many concurrent tasks
4. Complex concurrency control with multiple synchronized blocks

### Proposed Solution

Migrate to an event loop architecture that serializes all operations on a single thread per task, eliminating the need
for explicit synchronization:

1. Create a new `HttpRemoteTaskWithEventLoop` implementation that:
    - Uses a dedicated event loop per task for all state access and modifications
    - Eliminates synchronized blocks by ensuring all operations happen on the event loop thread
    - Serializes all operations, removing race conditions without using locks
    - Uses non-blocking async operations for all network requests

2. Core architectural changes:
    - Replace direct thread execution with event loop submission
    - Convert synchronized methods to event loop operations
    - Ensure all mutable state access happens on the event loop thread
    - Implement safe error handling for event loop operations

3. Key components:
    - `SafeEventLoopGroup`: Manages a pool of event loops
    - `SafeEventLoop`: Serializes operations for a single task
    - `safeExecuteOnEventLoop()`: Entry point for all operations

### Implementation Details

#### Task State Management

All mutable state will be accessed only from the event loop thread:

- Remove `synchronized` keywords from methods and blocks
- Convert all state updates to event loop operations via `safeExecuteOnEventLoop()`
- Ensure proper ordering of operations through event loop serialization

#### Event Loop Safety

To prevent deadlocks and ensure reliability:

- Add failure handling for event loop operations
- Implement timeouts for long-running operations
- Create monitoring for event loop health

#### Backward Compatibility

Both implementations will coexist during migration:

- `HttpRemoteTaskFactory` will determine which implementation to use
- Configuration flag to enable/disable event loop implementation
- Gradual rollout to production with A/B testing

### Performance Considerations

Expected improvements:

- ~90% reduction in CPU time for task scheduling operations
- Significant reduction in thread context switching
- Lower memory usage from fewer thread stacks
- Improved throughput for high concurrency workloads

### Testing Strategy

1. Unit tests for event loop implementation
2. Integration tests comparing both implementations
3. A/B testing in pre-production environments
4. Gradual rollout with metrics tracking
5. Performance benchmarks comparing CPU usage, throughput, and latency

### Metrics and Monitoring

New metrics to track:

- Event loop execution time
- Event loop queue size
- Operation processing rate
- Error rates in event loop operations
- Comparative CPU usage between implementations

### Rollout Plan

1. Implement `HttpRemoteTaskWithEventLoop` and supporting classes
2. Add configuration flag to toggle between implementations
3. Deploy to test environments and validate metrics
4. A/B test in production on a small subset of clusters
5. Gradually increase usage based on performance data
6. Full rollout to all production clusters

### Conclusion

Migrating from thread pool to event loop architecture will significantly reduce coordinator CPU usage by eliminating
synchronization overhead. This architectural change addresses fundamental performance limitations in the current design
and will enable Presto to handle higher query concurrency with existing resources.
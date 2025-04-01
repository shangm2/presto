# Presto Event Loop Implementation Update

## Project Overview
I am working on replacing thread pools with event loops across three levels of Presto execution (task, stage, and query) to reduce locking and resource contention on the coordinator. This architectural change aims to make the Presto coordinator scheduler more efficient at assigning jobs to workers.

## Current Status
- ✅ Task-level event loop implementation is complete and behind a feature toggle
- 🧪 Successfully deployed to one production cluster
- 📊 Initial performance metrics are promising:
  - 40% reduction in scheduling CPU usage
  - Improvements in query execution time and wall time
  - Task update delivered latency metrics inconclusive (possibly due to varying cluster load patterns)

## Next Steps
1. **Short-term**: Deploy to a second cluster
   - Proposing deployment to a batch processing cluster
   - Batch workloads typically have more consistent load patterns
   - Will enable more accurate performance measurements

2. **Long-term**: Continue implementation of event loops at other levels
   - Stage-level event loop implementation
   - Query-level event loop implementation
   - These changes will further improve coordinator efficiency

## Strategic Alignment
This architectural improvement positions us well for future integration with Presto C++ workers. The event loop model will help the coordinator better leverage the performance capabilities of C++ workers, creating a more efficient end-to-end system.

## Metrics to Monitor
- Scheduling CPU usage
- Query execution time
- Wall time
- Task update delivered latency
- General cluster stability and performance

Would appreciate any feedback or suggestions on the deployment strategy and monitoring approach.
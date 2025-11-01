# Velox I/O Metrics Architecture

## Overview

This document explains how I/O metrics, particularly `ioWaitWallNanos`, flow through the Velox execution model from low-level I/O operations to high-level operator statistics.

## Architecture Hierarchy

```
Task
  └─ Pipeline (e.g., "TableScan → Filter → Project")
       └─ Driver (instance 1)
            └─ TableScan Operator (instance 1)
                 └─ HiveDataSource (instance 1)
                      └─ ioStats_ (IoStatistics)
                           └─ queryThreadIoLatency() (IoCounter)
       └─ Driver (instance 2)
            └─ TableScan Operator (instance 2)
                 └─ HiveDataSource (instance 2)
                      └─ ioStats_
       └─ Driver (instance 3)
            └─ TableScan Operator (instance 3)
                 └─ HiveDataSource (instance 3)
                      └─ ioStats_
```

## Key Components

### 1. HiveDataSource (NOT an Operator!)

**Location**: `/home/shangma/local/fbsource/fbcode/velox/connectors/hive/HiveDataSource.h`

- **Type**: `DataSource` (connector-level abstraction)
- **Base Class**: `connector::DataSource` (from `velox/connectors/Connector.h:231`)
- **Purpose**: Handles actual file I/O operations for reading Hive/warm storage data
- **Ownership**: Created and owned by `TableScan` operator

**Key Members**:
```cpp
std::shared_ptr<io::IoStatistics> ioStats_;  // Line 125
```

### 2. TableScan (The Actual Operator)

**Location**: `/home/shangma/local/fbsource/fbcode/velox/exec/TableScan.h`

- **Type**: Operator (inherits from `SourceOperator` which inherits from `Operator`)
- **Purpose**: Source operator that reads data from connectors
- **Relationship**: Each `TableScan` operator instance owns one `HiveDataSource` instance

**DataSource Creation** (from `TableScan.cpp:338-348`):
```cpp
if (dataSource_ == nullptr) {
  connectorQueryCtx_ = operatorCtx_->createConnectorQueryCtx(...);
  dataSource_ = createDataSource(...);  // Creates HiveDataSource
}
```

### 3. Driver

**Location**: `/home/shangma/local/fbsource/fbcode/velox/exec/Driver.h`

- **Purpose**: Execution instance that runs a pipeline
- **Parallelism**: Multiple drivers can execute the same pipeline in parallel
- **Isolation**: Each driver gets its own instances of all operators in the pipeline

### 4. IoStatistics

**Location**: `/home/shangma/local/fbsource/fbcode/velox/common/io/IoStatistics.h`

**Purpose**: Accumulates I/O-related metrics during data source operations

**Key Counter**:
```cpp
// Lines 170-172
// Time spent by a query processing thread waiting for synchronously issued IO
// or for an in-progress read-ahead to finish.
IoCounter queryThreadIoLatency_;
```

## Metric Flow: How ioWaitWallNanos is Collected

### Step 1: Low-Level I/O Operations

**Location**: `/home/shangma/local/fbsource/fbcode/velox/dwio/common/CacheInputStream.cpp`

When actual I/O happens (e.g., reading from storage, SSD cache, or waiting for cache loads):

```cpp
// Line 249-256: Reading from storage
uint64_t storageReadUs{0};
{
  MicrosecondTimer timer(&storageReadUs);
  input_->read(ranges, region.offset, LogType::FILE);  // Actual I/O
}
ioStats_->read().increment(region.length);
ioStats_->queryThreadIoLatency().increment(storageReadUs);  // ← Accumulated here!
```

Multiple scenarios increment this counter:
1. **Storage reads** (line 255): Direct reads from warm storage/S3/HDFS
2. **SSD cache loads** (line 321): Loading data from SSD cache
3. **Cache wait** (line 228): Waiting for another thread to load data into cache
4. **Coalesced loads** (line 354): Waiting for grouped I/O operations

### Step 2: HiveDataSource Reports Accumulated Stats

**Location**: `/home/shangma/local/fbsource/fbcode/velox/connectors/hive/HiveDataSource.cpp:421-438`

When `getRuntimeStats()` is called:

```cpp
std::unordered_map<std::string, RuntimeMetric>
HiveDataSource::getRuntimeStats() {
  auto res = runtimeStats_.toRuntimeMetricMap();
  res.insert(
      {{"ioWaitWallNanos",
        RuntimeMetric(
            ioStats_->queryThreadIoLatency().sum() * 1000,  // All I/O waits for this driver!
            RuntimeCounter::Unit::kNanos)},
       {"maxSingleIoWaitWallNanos",
        RuntimeMetric(
            ioStats_->queryThreadIoLatency().max() * 1000,
            RuntimeCounter::Unit::kNanos)},
       // ... other metrics
      });
  return res;
}
```

**Important**: At this point, the `RuntimeMetric` is created with:
- `sum` = total I/O wait time for this driver's data source
- `count` = 1 (one data source instance)
- `min` = `max` = `sum` (single value)

### Step 3: TableScan Operator Collects Stats

**Location**: `/home/shangma/local/fbsource/fbcode/velox/exec/TableScan.cpp:304-316`

When splits are finished:

```cpp
if (!split.hasConnectorSplit()) {
  noMoreSplits_ = true;
  if (dataSource_) {
    const auto connectorStats = dataSource_->getRuntimeStats();
    auto lockedStats = stats_.wlock();
    for (const auto& [name, metric] : connectorStats) {
      if (FOLLY_UNLIKELY(lockedStats->runtimeStats.count(name) == 0)) {
        lockedStats->runtimeStats.emplace(name, RuntimeMetric(metric.unit));
      } else {
        VELOX_CHECK_EQ(lockedStats->runtimeStats.at(name).unit, metric.unit);
      }
      lockedStats->runtimeStats.at(name).merge(metric);  // ← Merged into operator stats
    }
  }
  return false;
}
```

### Step 4: Aggregation Across Drivers

**Location**: `/home/shangma/local/fbsource/fbcode/velox/exec/PlanNodeStats.cpp:134-139`

Multiple drivers' `OperatorStats` are merged into `PlanNodeStats`:

```cpp
for (const auto& [name, customStats] : stats.runtimeStats) {
  if (UNLIKELY(this->customStats.count(name) == 0)) {
    this->customStats.insert(std::make_pair(name, customStats));
  } else {
    this->customStats.at(name).merge(customStats);  // ← Drivers merged here!
  }
}
```

### Step 5: RuntimeMetric Merge Logic

**Location**: `/home/shangma/local/fbsource/fbcode/velox/common/base/RuntimeMetrics.cpp:37-49`

```cpp
void RuntimeMetric::merge(const RuntimeMetric& other) {
  VELOX_CHECK_EQ(unit, other.unit);
  sum += other.sum;      // Sum all drivers' total I/O waits
  count += other.count;  // Count number of drivers
  min = std::min(min, other.min);  // Driver with least I/O wait
  max = std::max(max, other.max);  // Driver with most I/O wait
}
```

## Understanding the Metric Values

When you see `ioWaitWallNanos` in query output:

```
ioWaitWallNanos: sum: 16000ms, count: 3, min: 3000ms, max: 8000ms, avg: 5333ms
```

This means:
- **sum: 16000ms** - Total I/O wait time across all 3 drivers for this operator
- **count: 3** - Number of drivers that executed this operator
- **min: 3000ms** - The driver with the least total I/O wait time
- **max: 8000ms** - The driver with the most total I/O wait time (worst-case driver)
- **avg: 5333ms** - Average I/O wait per driver (16000 / 3)

### What "max" Represents

**The `max` value is the total I/O wait time of the slowest driver**, not a single I/O operation.

Example with 3 drivers:
- **Driver 1**: 100 I/O operations, total wait = 5000ms → Creates `RuntimeMetric(5000, kNanos)`
- **Driver 2**: 80 I/O operations, total wait = 3000ms → Creates `RuntimeMetric(3000, kNanos)`
- **Driver 3**: 120 I/O operations, total wait = 8000ms → Creates `RuntimeMetric(8000, kNanos)`

After merging:
- `max = 8000ms` ← **Driver 3 is the worst-case driver** (data skew indicator!)

## Granularity Summary

| Level | Description | ioWaitWallNanos Behavior |
|-------|-------------|--------------------------|
| **I/O Operation** | Individual storage read, cache load, etc. | Each operation increments `queryThreadIoLatency()` |
| **HiveDataSource** | One per driver, accumulates all I/O waits | Reports sum of all I/O waits for that driver |
| **TableScan Operator** | One per driver, owns one HiveDataSource | Collects stats from its HiveDataSource |
| **PlanNodeStats** | Aggregated across all drivers | **sum**: total across drivers<br>**count**: number of drivers<br>**min/max**: range across drivers |
| **Pipeline/Stage** | N/A | Velox doesn't aggregate at pipeline level |

## Key Takeaways

1. **`HiveDataSource` is NOT an operator** - it's a connector-level `DataSource` owned by the `TableScan` operator
2. **Each driver gets its own `HiveDataSource` instance** with its own `ioStats_`
3. **`ioWaitWallNanos` is per-operator**, aggregated across all drivers running that operator
4. **The `max` value identifies the worst-case driver** - useful for detecting data skew
5. **I/O waits include**: storage reads, SSD cache loads, cache wait times, and coalesced I/O operations

## Related Files

### Core Metric Collection
- `/home/shangma/local/fbsource/fbcode/velox/common/io/IoStatistics.h` - IoStatistics and IoCounter definitions
- `/home/shangma/local/fbsource/fbcode/velox/common/io/IoStatistics.cpp` - IoStatistics implementation
- `/home/shangma/local/fbsource/fbcode/velox/dwio/common/CacheInputStream.cpp` - Where I/O latencies are measured
- `/home/shangma/local/fbsource/fbcode/velox/dwio/common/DirectInputStream.cpp` - Direct I/O measurements

### DataSource and Operator
- `/home/shangma/local/fbsource/fbcode/velox/connectors/Connector.h` - DataSource interface definition
- `/home/shangma/local/fbsource/fbcode/velox/connectors/hive/HiveDataSource.h` - HiveDataSource class
- `/home/shangma/local/fbsource/fbcode/velox/connectors/hive/HiveDataSource.cpp` - getRuntimeStats() implementation
- `/home/shangma/local/fbsource/fbcode/velox/exec/TableScan.h` - TableScan operator
- `/home/shangma/local/fbsource/fbcode/velox/exec/TableScan.cpp` - Stats collection from DataSource

### Aggregation and Reporting
- `/home/shangma/local/fbsource/fbcode/velox/common/base/RuntimeMetrics.h` - RuntimeMetric definition
- `/home/shangma/local/fbsource/fbcode/velox/common/base/RuntimeMetrics.cpp` - merge() implementation
- `/home/shangma/local/fbsource/fbcode/velox/exec/OperatorStats.h` - Per-operator statistics
- `/home/shangma/local/fbsource/fbcode/velox/exec/PlanNodeStats.h` - Aggregated plan node statistics
- `/home/shangma/local/fbsource/fbcode/velox/exec/PlanNodeStats.cpp` - Stats aggregation logic
- `/home/shangma/local/fbsource/fbcode/velox/exec/TaskStats.h` - Task-level statistics hierarchy

### Documentation
- `/home/shangma/local/fbsource/fbcode/velox/docs/develop/debugging/metrics.rst` - Official metrics documentation

---
*Document created: 2025-10-31*
*Author: Architecture analysis of Velox I/O metrics subsystem*


namespace java com.facebook.presto.experimental
namespace cpp protocol

enum ThriftTaskState {
    PLANNED = 0,
    RUNNING = 1,
    FINISHED = 2,
    CANCELED = 3,
    ABORTED = 4,
    FAILED = 5,
}

enum ThriftErrorCause {
    UNKNOWN = 0,
    LOW_PARTITION_COUNT = 1,
    EXCEEDS_BROADCAST_MEMORY_LIMIT = 2;
}

struct ThriftLifespan {
  1: bool grouped;
  2: i32 groupId;
}

enum ThriftErrorType {
  USER_ERROR = 0,
  INTERNAL_ERROR = 1,
  INSUFFICIENT_RESOURCES = 2,
  EXTERNAL = 3,
}

struct ThriftErrorCode {
  1: i32 code;
  2: string name;
  3: ThriftErrorType type;
  4: bool retriable;
}

struct ThriftErrorLocation {
  1: i32 lineNumber;
  2: i32 columnNumber;
}

struct ThriftHostAddress {
  1: string host;
  2: i32 port;
}

struct ThriftExecutionFailureInfo {
  1: string type;
  2: string message;
  3: optional ThriftExecutionFailureInfo cause;
  4: list<ThriftExecutionFailureInfo> suppressed;
  5: list<string> stack;
  6: ThriftErrorLocation errorLocation;
  7: ThriftErrorCode errorCode;
  8: ThriftHostAddress remoteHost;
  9: ThriftErrorCause errorCause;
}

struct ThriftTaskStatus {
    1: i64 taskInstanceIdLeastSignificantBits;
    2: i64 taskInstanceIdMostSignificantBits;
    3: i64 version;
    4: ThriftTaskState state;
    5: string self;
    6: set<ThriftLifespan> completedDriverGroups;
    7: list<ThriftExecutionFailureInfo> failures;
    8: i32 queuedPartitionedDrivers;
    9: i32 runningPartitionedDrivers;
    10: double outputBufferUtilization;
    11: bool outputBufferOverutilized;
    12: i64 physicalWrittenDataSizeInBytes;
    13: i64 memoryReservationInBytes;
    14: i64 systemMemoryReservationInBytes;
    15: i64 peakNodeTotalMemoryReservationInBytes;
    16: i64 fullGcCount;
    17: i64 fullGcTimeInMillis;
    18: i64 totalCpuTimeInNanos;
    19: i64 taskAgeInMillis;
    20: i64 queuedPartitionedSplitsWeight;
    21: i64 runningPartitionedSplitsWeight;
}
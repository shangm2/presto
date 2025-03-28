namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Task.thrift"
include "TaskStatus.thrift"

enum ThriftBufferState {
  OPEN = 0,
  NO_MORE_BUFFERS = 1,
  NO_MORE_PAGES = 2,
  FLUSHING = 3,
  FINISHED = 4,
  FAILED = 5,
}

struct ThriftOutputBufferInfo {
  1: string type;
  2: ThriftBufferState state;
  3: bool canAddBuffers;
  4: bool canAddPages;
  5: i64 totalBufferedBytes;
  6: i64 totalBufferedPages;
  7: i64 totalRowsSent;
  8: i64 totalPagesSent;
  9: list<ThriftBufferInfo> buffers;
}

struct ThriftTaskInfo {
  1: Task.ThriftTaskId taskId;
  2: TaskStatus.ThriftTaskStatus taskStatus;
  3: i64 lastHeartbeatInMillis;
  4: ThriftOutputBufferInfo outputBuffers;
  5: set<ThriftPlanNodeId> noMoreSplits;
  6: ThriftTaskStats stats;
  7: bool needsPlan;
  8: ThriftMetadataUpdates metadataUpdates;
  9: string nodeId;
}
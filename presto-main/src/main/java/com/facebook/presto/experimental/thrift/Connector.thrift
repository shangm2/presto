namespace java com.facebook.presto.experimental
namespace cpp protocol

include "Common.thrift"
include "Union.thrift"

typedef string ThriftPlanNodeId
typedef string ThriftConnectorId

typedef i32 ThriftOutputBufferId

enum ThriftBufferType {
  BROADCAST = 0,
  PARTITIONED = 1,
  ARBITRARY = 2
}

struct ThriftOutputBuffers {
  1: ThriftBufferType type;
  2: i64 version;
  3: bool noMoreBufferIds;
  4: map<ThriftOutputBufferId, i32> buffers;
}

struct ThriftLifespan {
  1: bool grouped;
  2: i32 groupId;
}

struct ThriftScheduledSplit {
  1: i64 sequenceId;
  2: ThriftPlanNodeId planNodeId;
  3: ThriftSplit split;
}

struct ThriftSplit {
  1: ThriftConnectorId connectorId;
  2: Union.ThriftConnectorTransactionHandle transactionHandle;
  3: Union.ThriftConnectorSplit connectorSplit;
  4: ThriftLifespan lifespan;
  5: Union.ThriftSplitContext splitContext;
}

struct ThriftTaskSource {
  1: ThriftPlanNodeId planNodeId;
  2: set<ThriftScheduledSplit> splits;
  3: set<ThriftLifespan> noMoreSplitsForLifespan;
  4: bool noMoreSplits;
}

struct ThriftTableHandle {
  1: ThriftConnectorId connectorId;
  2: Union.ThriftConnectorTableHandle connectorHandle;
  3: Union.ThriftConnectorTransactionHandle transaction;
}

struct ThriftAnalyzeTableHandle {
  1: ThriftConnectorId connectorId;
  2: Union.ThriftConnectorTransactionHandle transactionHandle;
  3: Union.ThriftConnectorTableHandle connectorHandle;
}

struct ThriftDeleteScanInfo {
  1: ThriftPlanNodeId id;
  2: ThriftTableHandle tableHandle;
}

struct ThriftTableWriteInfo {
  1: optional Union.ThriftExecutionWriterTarget writerTarget;
  2: optional ThriftAnalyzeTableHandle analyzeTableHandle;
  3: optional ThriftDeleteScanInfo deleteScanInfo;
}
namespace java com.facebook.presto.common.experimental
namespace cpp protocol

enum ThriftConnectorTableHandleType {
    BASE_HIVE_HANDLE = 1,
    HIVE_HANDLE = 2
}

struct ThriftConnectorTableHandle {
  1: ThriftConnectorTableHandleType type;
}

struct ThriftBaseHiveTableHandle extends ThriftConnectorTableHandle {
  2: string schemaName;
  3: string tableName;
}

struct ThriftHiveTableHandle extends ThriftBaseHiveTableHandle {
  4: optional list<list<string>> analyzePartitionValues;
}
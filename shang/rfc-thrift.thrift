## 1. Union Type Pattern
// ConnectorTableHandle.thrift
union ConnectorTableHandleType {
  1: HiveTableHandle hiveTableHandle;
  2: JdbcTableHandle jdbcTableHandle;
  3: MemoryTableHandle memoryTableHandle;
  // Add other implementations as needed
}

// AnalyzeTableHandle.thrift
struct AnalyzeTableHandle {
  1: string connectorId;
  2: ConnectorTableHandleType connectorHandle;
}

## 2. Base Type + Extensions Pattern
// BaseConnectorTableHandle.thrift
struct BaseConnectorTableHandle {
  1: string type;
  // Common fields
}

// HiveConnectorTableHandle.thrift
struct HiveConnectorTableHandle {
  1: BaseConnectorTableHandle base;
  2: string schema;
  3: string table;
  // Hive-specific fields
}

// AnalyzeTableHandle.thrift
struct AnalyzeTableHandle {
  1: string connectorId;
  2: BaseConnectorTableHandle connectorHandle;
}

## 3. Type Field Pattern
// ConnectorTableHandleTypes.thrift
enum ConnectorTableHandleType {
  HIVE = 0,
  JDBC = 1,
  MEMORY = 2
  // Add other types as needed
}

// HiveTableHandleData.thrift
struct HiveTableHandleData {
  1: string schema;
  2: string table;
  // Hive-specific fields
}

// AnalyzeTableHandle.thrift
struct AnalyzeTableHandle {
  1: string connectorId;
  2: ConnectorTableHandleType connectorHandleType;
  3: optional HiveTableHandleData hiveTableHandleData;
  4: optional JdbcTableHandleData jdbcTableHandleData;
  5: optional MemoryTableHandleData memoryTableHandleData;
  // Add other implementations as needed
}

## 4. Serialized Blob Pattern
// AnalyzeTableHandle.thrift
struct AnalyzeTableHandle {
  1: string connectorId;
  2: string connectorHandleType;  // Class name
  3: binary serializedConnectorHandle;  // Serialized data
}
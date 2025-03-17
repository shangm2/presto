Handling Java Polymorphism with Thrift IDL

// Define a union for all possible ConnectorTableHandle implementations
union ThriftConnectorTableHandle {
  1: JdbcTableHandle jdbcHandle;
  2: HiveTableHandle hiveHandle;
  3: MemoryTableHandle memoryHandle;
  // Add other implementations as needed
}

// Define the concrete implementations
struct JdbcTableHandle {
  1: string catalogName;
  2: string schemaName;
  3: string tableName;
  // Other fields...
}

struct HiveTableHandle {
  1: string schemaName;
  2: string tableName;
  // Other fields...
}

// Similar for ConnectorTransactionHandle
union ThriftConnectorTransactionHandle {
  1: JdbcTransactionHandle jdbcHandle;
  2: HiveTransactionHandle hiveHandle;
  // Other implementations...
}

// Main TableHandle struct
struct TableHandle {
  1: ConnectorId connectorId;
  2: ThriftConnectorTableHandle connectorHandle;
  3: ThriftConnectorTransactionHandle transactionHandle;
  4: optional TableLayoutHandle layout;
}

public class TableHandleConverter {
    // Convert from Java interface to Thrift union
    public static ThriftConnectorTableHandle toThrift(ConnectorTableHandle handle) {
        ThriftConnectorTableHandle thriftHandle = new ThriftConnectorTableHandle();
        
        if (handle instanceof JdbcTableHandle) {
            JdbcTableHandle jdbcHandle = (JdbcTableHandle) handle;
            thriftHandle.setJdbcHandle(new com.facebook.presto.thrift.JdbcTableHandle(
                jdbcHandle.getCatalogName(),
                jdbcHandle.getSchemaName(),
                jdbcHandle.getTableName()
                // Set other fields...
            ));
        }
        else if (handle instanceof HiveTableHandle) {
            // Similar conversion for Hive
        }
        // Add other implementations
        else {
            throw new IllegalArgumentException("Unsupported handle type: " + handle.getClass());
        }
        
        return thriftHandle;
    }
    
    // Convert from Thrift union to Java interface
    public static ConnectorTableHandle fromThrift(ThriftConnectorTableHandle thriftHandle) {
        if (thriftHandle.isSetJdbcHandle()) {
            com.facebook.presto.thrift.JdbcTableHandle jdbcThrift = thriftHandle.getJdbcHandle();
            return new JdbcTableHandle(
                jdbcThrift.getCatalogName(),
                jdbcThrift.getSchemaName(),
                jdbcThrift.getTableName()
                // Get other fields...
            );
        }
        else if (thriftHandle.isSetHiveHandle()) {
            // Similar conversion for Hive
        }
        // Add other implementations
        else {
            throw new IllegalArgumentException("Unknown handle type in Thrift union");
        }
    }
}




namespace java com.facebook.presto.common.type.thrift

/**
 * Represents a type signature in the Presto type system.
 * A type signature consists of a base type and optional parameters.
 */
struct ThriftTypeSignature {
  /**
   * The base type of this signature
   */
  1: required ThriftTypeSignatureBase base;
  
  /**
   * Parameters for the type (if any)
   */
  2: required list<ThriftTypeSignatureParameter> parameters;
  
  /**
   * Whether this type signature contains calculated parameters
   */
  3: required bool calculated;
}

/**
 * Represents the base of a type signature, which can be a standard type name or a qualified name.
 */
union ThriftTypeSignatureBase {
  1: string standardTypeBase;
  2: ThriftQualifiedObjectName qualifiedObjectName;
  3: ThriftUserDefinedType userDefinedType;
  4: ThriftDistinctTypeInfo distinctTypeInfo;
}

/**
 * Represents a qualified object name with catalog, schema, and object components.
 */
struct ThriftQualifiedObjectName {
  1: required string catalogName;
  2: required string schemaName;
  3: required string objectName;
}

/**
 * Represents a user-defined type.
 */
struct ThriftUserDefinedType {
  1: required ThriftQualifiedObjectName name;
  2: required ThriftTypeSignature physicalTypeSignature;
}

/**
 * Represents information about a distinct type.
 */
struct ThriftDistinctTypeInfo {
  1: required ThriftQualifiedObjectName name;
  2: required ThriftTypeSignature baseType;
  3: required bool orderable;
  4: optional ThriftQualifiedObjectName topMostAncestor;
  5: required list<ThriftQualifiedObjectName> otherAncestors;
}

/**
 * Represents a parameter for a type signature.
 */
union ThriftTypeSignatureParameter {
  1: ThriftTypeSignature typeSignature;
  2: ThriftNamedTypeSignature namedTypeSignature;
  3: i64 longLiteral;
  4: string varcharLiteral;
  5: ThriftLongEnumMap longEnum;
  6: ThriftVarcharEnumMap varcharEnum;
  7: ThriftDistinctTypeInfo distinctTypeInfo;
}

/**
 * Represents a named type signature parameter.
 */
struct ThriftNamedTypeSignature {
  1: required string name;
  2: required ThriftTypeSignature typeSignature;
}

/**
 * Represents a mapping for a bigint enum type.
 */
struct ThriftLongEnumMap {
  1: required string typeName;
  2: required map<string, i64> enumMap;
}

/**
 * Represents a mapping for a varchar enum type.
 */
struct ThriftVarcharEnumMap {
  1: required string typeName;
  2: required map<string, string> enumMap;
}
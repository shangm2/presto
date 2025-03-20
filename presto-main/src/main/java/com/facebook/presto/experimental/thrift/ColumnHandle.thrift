namespace java com.facebook.presto.experimental
namespace cpp protocol

include "Common.thrift"
include "Type.thrift"
include "RowExpression.thrift"

enum ThriftColumnHandleType {
    HIVE = 1,
    JDBC = 2,
}

enum ThriftColumnType {
    PARTITION_KEY = 1,
    REGULAR = 2,
    SYNTHESIZED = 3,
    AGGREGATED = 4,
}

enum ThriftPathElementType {
    NESTED_FIELD = 1,
    LONG_SUBSCRIPT = 2,
    STRING_SUBSCRIPT = 3,
    ALL_SUBSCRIPTS = 4,
    NO_SUBFIELD = 5
}

struct ThriftNestedFieldElement {
  1: string name;
}

struct ThriftLongSubscriptElement {
  1: i64 index;
}

struct ThriftStringSubScriptElement {
  1: string index;
}

struct ThriftAllSubscriptsElement {
}

struct ThriftNoSubfieldElement {
}

struct ThriftPathElement {
  1: ThriftPathElementType type;
  2: binary serializedPathElement;
}

struct ThriftSubfield {
  1: string name;
  2: list<ThriftPathElement> path;
}

struct ThriftBaseHiveColumnHandle {
  1: string name;
  2: optional string comment;
  3: ThriftColumnType columnType;
  4: list<ThriftSubfield> requiredSubfields;
}

typedef string ThriftHiveType

struct ThriftHiveColumnHandle {
  1: ThriftBaseHiveColumnHandle base;
  2: ThriftHiveType hiveType;
  3: ThriftTypeSignature typeName;
  4: i32 hiveColumnIndex;
  5: optional ThriftAggregation partialAggregation;
}

struct ThriftAggregation {
  1: ThriftCallExpression call;
  2: optional ThriftRowExpression filter;
  3: optional ThriftOrderingScheme orderingScheme;
  4: bool isDistinct;
  5: optional ThriftVariableReferenceExpression mask;
}

struct ThriftColumnHandle {
  1: ThriftColumnHandleType type;
  2: binary serializedColumnHandle;
}
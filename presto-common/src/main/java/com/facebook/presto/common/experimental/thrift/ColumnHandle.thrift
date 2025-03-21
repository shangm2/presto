namespace java com.facebook.presto.experimental
namespace cpp protocol

include "Common.thrift"
include "Type.thrift"
include "RowExpression.thrift"
include "TypeSignature.thrift"

enum ThriftPathElementType {
    NESTED_FIELD = 1,
    LONG_SUBSCRIPT = 2,
    STRING_SUBSCRIPT = 3,
    ALL_SUBSCRIPTS = 4,
    NO_SUBFIELD = 5
}

struct ThriftPathElement {
  1: ThriftPathElementType type;
}

struct ThriftNestedFieldElement extends ThriftPathElement {
  2: string name;
}

struct ThriftLongSubscriptElement extends ThriftPathElement  {
  2: i64 index;
}

struct ThriftStringSubScriptElement extends ThriftPathElement {
  2: string index;
}

struct ThriftAllSubscriptsElement extends ThriftPathElement {
}

struct ThriftNoSubfieldElement extends ThriftPathElement {
}

struct ThriftSubfield {
  1: string name;
  2: optional list<ThriftPathElement> path;
}

enum ThriftColumnType {
    PARTITION_KEY = 1,
    REGULAR = 2,
    SYNTHESIZED = 3,
    AGGREGATED = 4,
}

struct ThriftColumnHandle {
  1: ThriftColumnType type;
}

struct ThriftBaseHiveColumnHandle extends ThriftColumnHandle{
  2: string name;
  3: optional string comment;
  4: ThriftColumnType columnType
  5: list<ThriftSubfield> requiredSubfields;
}

typedef string ThriftHiveType

struct ThriftHiveColumnHandle extends ThriftBaseHiveColumnHandle {
  6: ThriftHiveType hiveType;
  7: TypeSignature.ThriftTypeSignature typeName;
  8: i32 hiveColumnIndex;
  9: optional ThriftAggregation partialAggregation;
}

struct ThriftAggregation {
  1: RowExpression.ThriftCallExpression call;
  2: optional RowExpression.ThriftRowExpression filter;
  3: optional RowExpression.ThriftOrderingScheme orderingScheme;
  4: bool isDistinct;
  5: optional RowExpression.ThriftVariableReferenceExpression mask;
}
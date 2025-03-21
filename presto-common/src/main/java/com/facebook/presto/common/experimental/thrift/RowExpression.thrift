namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "Block.thrift"
include "Type.thrift"
include "FunctionHandle.thrift"
include "TypeSignature.thrift"

enum ThriftRowExpressionType {
  CALL = 1,
  SPECIAL_FORM = 2,
  LAMBDA_DEFINITION = 3,
  INPUT_REFERENCE = 4,
  VARIABLE_REFERENCE = 5,
  CONSTANT = 6
}

struct ThriftRowExpression {
  1: ThriftRowExpressionType type;
  2: optional Common.ThriftSourceLocation sourceLocation;
}

struct ThriftCallExpression extends ThriftRowExpression {
  3: string displayName;
  4: FunctionHandle.ThriftFunctionHandle functionHandle;
  5: Type.ThriftType returnType;
  6: list<ThriftRowExpression> arguments;
}

enum ThriftForm
{
    IF = 1,
    NULL_IF = 2,
    SWITCH = 3,
    WHEN = 4,
    IS_NULL = 5,
    COALESCE = 6,
    IN = 7,
    AND = 8,
    OR = 9,
    DEREFERENCE = 10,
    ROW_CONSTRUCTOR = 11,
    BIND = 12
}

struct ThriftSpecialFormExpression extends ThriftRowExpression {
  3: ThriftForm form;
  4: Type.ThriftType returnType;
  5: list<ThriftRowExpression> arguments;
}

struct ThriftLambdaDefinitionExpression extends ThriftRowExpression {
  3: list<Type.ThriftType> argumentTypes;
  4: list<string> arguments;
  5: ThriftRowExpression body;
  6: string canonicalizedBody;
}

struct ThriftInputReferenceExpression extends ThriftRowExpression {
  3: i32 field;
  4: Type.ThriftType type;
}

struct ThriftVariableReferenceExpression extends ThriftRowExpression {
  3: string name;
  4: Type.ThriftType type;
}

struct ThriftConstantExpression extends ThriftRowExpression {
  3: Block.ThriftBlock valueBlock;
  4: Type.ThriftType type;
}

enum ThriftSortOrder {
  ASC_NULLS_FIRST = 1,
  ASC_NULLS_LAST = 2,
  DESC_NULLS_FIRST = 3,
  DESC_NULLS_LAST = 4
}

struct ThriftOrdering
{
  1: ThriftVariableReferenceExpression variable;
  2: ThriftSortOrder sortOrder;
}

struct ThriftOrderingScheme
{
  1: list<ThriftOrdering> orderBy;
}
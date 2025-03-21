namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Common.thrift"
include "Signature.thrift"
include "Sql.thrift"
include "TypeSignature.thrift"

enum ThriftFunctionHandleType {
  BUILTIN_HANDLE = 1,
  HIVE_HANDLE = 2,
  NATIVE_HANDLE = 3,
  REST_HANDLE = 4,
  SESSION_HANDLE = 5,
  SQL_HANDLE = 6
}

struct ThriftFunctionHandle {
  1: ThriftFunctionHandleType type;
}

struct ThriftBuiltInFunctionHandle extends ThriftFunctionHandle {
  2: Signature.ThriftSignature signature;
}

struct ThriftHiveFunctionHandle extends ThriftFunctionHandle {
  2: Signature.ThriftSignature signature;
}

struct ThriftNativeFunctionHandle extends ThriftFunctionHandle {
  2: TypeSignature.ThriftSqlFunctionHandle base;
  3: Signature.ThriftSignature signature;
}

struct ThriftRestFunctionHandle extends ThriftFunctionHandle {
  2: TypeSignature.ThriftSqlFunctionHandle base;
  3: Signature.ThriftSignature signature;
}

struct ThriftSessionFunctionHandle extends ThriftFunctionHandle {
  2: Sql.ThriftSqlInvokedFunction sqlFunction;
}
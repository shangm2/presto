namespace java com.facebook.presto.common.experimental.auto_gen
namespace cpp protocol


struct ThriftTypeInfo {
    1: string type;
    2: binary serializedTypeInfo
}

struct ThriftHiveType {
  1: ThriftTypeInfo typeInfo;
}

struct ThriftPrimitiveTypeInfo {
  1: string typeName;
}

struct ThriftVarcharTypeInfo {
  1: i32 length;
}
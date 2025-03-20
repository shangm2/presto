namespace java com.facebook.presto.experimental
namespace cpp protocol

include "Type.thrift"
include "ColumnHandle.thrift"

enum ValueSetType {
  EQUATABLE = 1,
  SORTABLE = 2,
  ALL_OR_NONE = 3
}

struct ThriftValueEntry {

}

struct EquatableValueSet {
  1: Type.ThriftType type;
  2: bool whiteList;
  3: set<ThriftValueEntry> entries;
}

struct ThriftValueSet {
  1: optional ThriftAllOrNoneValueSet allOrNoneValueSet;
  2: optional ThriftEquatableValueSet equatableValueSet;
  3: optional ThriftRangeValueSet rangeValueSet;
}

struct ThriftDomain {
  1: ThriftValueSet valueSet;
  2: bool nullAllowed;
}

struct ThriftColumnHandleTupleDomain {
  1: optional map<ThriftColumnHandle, ThriftDomain> domains;
}
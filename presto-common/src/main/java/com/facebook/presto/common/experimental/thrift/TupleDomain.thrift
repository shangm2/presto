namespace java com.facebook.presto.common.experimental
namespace cpp protocol

include "Type.thrift"
include "Block.thrift"
include "Marker.thrift"
include "ColumnHandle.thrift"

enum ThriftValueSetType {
  EQUATABLE = 1,
  SORTABLE = 2,
  ALL_OR_NONE = 3
}

struct ThriftValueEntry {
  1: Type.ThriftType type;
  2: Block.ThriftBlock block;
}

struct ThriftValueSet {
  1: ThriftValueSetType type;
}

struct ThriftEquatableValueSet extends ThriftValueSet {
  2: Type.ThriftType type;
  3: bool whiteList;
  4: set<ThriftValueEntry> entries;
}

struct ThriftSortedRangeSet extends ThriftValueSet {
  2: Type.ThriftType type;
  3: map<Marker.ThriftMarker, Marker.ThriftRange> lowIndexedRanges;
}

struct ThriftAllOrNoneValueSet extends ThriftValueSet {
  2: Type.ThriftType type;
  3: bool all;
}

struct ThriftDomain {
  1: ThriftValueSet valueSet;
  2: bool nullAllowed;
}

struct ThriftTupleDomain {
  1: optional map<binary, ThriftDomain> domains;
  2: string keyClassName;
}
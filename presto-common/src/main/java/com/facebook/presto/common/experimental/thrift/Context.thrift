namespace java com.facebook.presto.common.experimental
namespace cpp protocol

struct ThriftSplitContext {
  1: bool cacheable;
  2: optional ThriftTupleDomain dynamicFilterPredicate;
}
I'll help you write an RFC for the Thrift serialization design. Let me first look at the existing RFC format.

Read file: /Users/ms/prestodb_rfcs/RFC-0000-template.md
Let me also look at a recent RFC to understand the style and depth of content.

Read file: /Users/ms/prestodb_rfcs/RFC-0003-native-spi.md
Based on the template and example, I'll draft an RFC for the Thrift serialization design. Here's the proposed RFC:

```markdown
# **RFC-XXXX for Presto - Thrift Serialization for Polymorphic Types**

See [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on creating your RFC and the process surrounding it.

## Thrift Serialization Design for Polymorphic Types

Proposers
* [Your Name]

## Related Issues
* Current JSON serialization design in presto-spi binds polymorphic types to JSON format
* Need for Thrift serialization support without tight coupling

## Summary
This RFC proposes a new design for serialization of polymorphic types in Presto that provides true serialization format independence. While maintaining backward compatibility with the current JSON serialization, it introduces a new architecture that allows adding Thrift serialization and future formats without modifying the SPI interfaces.

## Background
Currently, Presto uses Jackson annotations directly in SPI interfaces for polymorphic type serialization. While this works, it creates a tight coupling between the SPI and JSON serialization. As we add support for Thrift serialization and potentially other formats in the future, we need a more flexible design that maintains clean separation between core interfaces and serialization concerns.

### Goals
* Enable Thrift serialization support for polymorphic types
* Provide a clean separation between SPI interfaces and serialization concerns
* Allow multiple serialization formats to coexist
* Support future serialization formats without SPI changes
* Maintain backward compatibility with existing JSON serialization
* Allow gradual migration from current design to new design

### Non-goals
* Rewriting existing JSON serialization immediately
* Supporting every possible serialization format
* Changing existing polymorphic type interfaces

## Proposed Implementation

### Current Design Analysis
```java
// Current design in SPI
public interface ConnectorSplit {
    @JsonProperty
    Object getInfo();
    
    @ThriftField(1)  // Adding this would bind SPI to Thrift
    NodeSelectionStrategy getNodeSelectionStrategy();
}
```

Issues with current approach:
1. SPI interfaces are tightly coupled to serialization formats
2. Adding new formats requires SPI changes
3. All implementations must support all formats
4. Difficult to evolve serialization independently

### New Design
```java
// In presto-spi (clean interface)
public interface ConnectorSplit {
    Object getInfo();
    NodeSelectionStrategy getNodeSelectionStrategy();
    List<HostAddress> getPreferredNodes(NodeProvider nodeProvider);
}

// In presto-spi (serialization bridge)
public interface SplitSerializer<T extends ConnectorSplit> {
    String getType();
    byte[] serialize(T split);
    T deserialize(byte[] data);
}

// Implementation example for Hive
public class HiveSplitJsonSerializer implements SplitSerializer<HiveSplit> {
    @Override
    public String getType() { return "hive"; }
    
    @Override
    public byte[] serialize(HiveSplit split) {
        // Use Jackson
    }
    
    @Override
    public HiveSplit deserialize(byte[] data) {
        // Use Jackson
    }
}

public class HiveSplitThriftSerializer implements SplitSerializer<HiveSplit> {
    @Override
    public String getType() { return "hive"; }
    
    @Override
    public byte[] serialize(HiveSplit split) {
        // Use Thrift
    }
    
    @Override
    public HiveSplit deserialize(byte[] data) {
        // Use Thrift
    }
}
```

### Configuration Toggle
```properties
connector.${name}.serialization-format=thrift
```

The new design will be controlled by configuration, allowing:
* Gradual adoption of new serialization design
* A/B testing between formats
* Fallback to existing JSON serialization
* Per-connector serialization format selection

## Metrics
* Serialization/deserialization performance
* Memory usage for different formats
* Error rates in serialization
* Usage metrics for different formats

## Other Approaches Considered

### 1. Annotation-based Approach
```java
public interface ConnectorSplit {
    @SerializeWith(format = "json")
    @SerializeWith(format = "thrift")
    Object getInfo();
}
```
Rejected because:
* Still couples SPI to serialization concerns
* Requires SPI changes for new formats
* Complicates annotation processing

### 2. Service Provider Interface
```java
public interface SerializationProvider {
    byte[] serialize(Object obj);
    Object deserialize(byte[] data, Class<?> type);
}
```
Rejected because:
* Too generic
* Type safety issues
* Performance concerns with reflection

## Adoption Plan

### Phase 1: Infrastructure
* Implement `SplitSerializer` interface
* Add configuration support
* Create Thrift serialization implementations
* Add metrics collection

### Phase 2: Migration Support
* Create migration tools
* Add documentation
* Provide examples
* Release beta version

### Phase 3: JSON Migration
* Create JSON implementation using new design
* Gradually migrate existing JSON serialization
* Maintain backward compatibility

### Documentation
* New design documentation
* Migration guides
* Configuration reference
* Best practices guide

## Test Plan

### Unit Tests
* Serializer implementation tests
* Configuration parsing tests
* Format conversion tests
* Error handling tests

### Integration Tests
* Cross-format compatibility tests
* Performance benchmarks
* Memory usage tests
* Error recovery tests

### Migration Tests
* Backward compatibility tests
* Configuration toggle tests
* Format conversion tests

### Product Tests
* End-to-end connector tests
* Performance impact tests
* Memory impact tests
```

This RFC follows the format of existing RFCs while providing comprehensive coverage of the serialization design changes. Would you like me to expand on any particular section?

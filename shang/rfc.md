I'll update the RFC to reflect our refined understanding, focusing on the extensibility benefits rather than the incorrect lock-in concerns. Let me draft a new version.

```markdown
# RFC-XXXX: Extensible Split Serialization Framework

## Summary
Introduce a new serialization framework for Split objects that provides a clean, extensible way to support multiple serialization formats (starting with Thrift) through a format-agnostic interface.

## Background and Motivation
Currently, Split serialization in Presto is implemented using Jackson annotations directly in the implementation classes. While this approach works, adding support for new serialization formats requires adding new annotations or creating parallel implementations. We need a more flexible approach that allows easy addition of new serialization formats without modifying existing code.

## Goals
- Introduce Thrift serialization support for Split objects
- Create a clean abstraction for serialization that makes it easy to add new formats
- Maintain backward compatibility with existing JSON serialization
- Provide a configuration-based mechanism to choose serialization formats
- Set up a pattern that could be applied to other serializable types in the future

## Non-Goals
- Changing the existing Split class hierarchy or behavior
- Modifying the plugin architecture or class loading mechanism
- Replacing the existing JSON serialization immediately

## Design
### Current Architecture
```java
// Current implementation with direct annotations
@JsonTypeInfo(...)
public class Split {
    @JsonProperty
    private final ConnectorSplit connectorSplit;
    // ... other fields and methods
}
```

### Proposed Architecture
```java
// Clean Split class without serialization concerns
public class Split {
    private final ConnectorSplit connectorSplit;
    // ... other fields and methods
}

// Generic serialization interface
public interface SplitSerializer<T extends Split> {
    String getType();  // e.g., "json", "thrift"
    byte[] serialize(T split);
    T deserialize(byte[] data);
}

// JSON implementation
public class JsonSplitSerializer implements SplitSerializer<Split> {
    @Override
    public String getType() {
        return "json";
    }
    // ... implementation
}

// Thrift implementation
public class ThriftSplitSerializer implements SplitSerializer<Split> {
    @Override
    public String getType() {
        return "thrift";
    }
    // ... implementation
}
```

### Configuration
```properties
split.serialization.type=json|thrift
```

## Benefits
1. **Clean Separation of Concerns**
   - Split classes focus on business logic
   - Serialization logic is isolated in dedicated classes
   - Each format has its own implementation without affecting others

2. **Easy Extension**
   - New formats can be added by implementing SplitSerializer
   - No modification needed to existing Split classes
   - No recompilation of core classes required

3. **Runtime Flexibility**
   - Formats can be switched via configuration
   - Multiple formats can coexist
   - Easy A/B testing of different formats

4. **Better Testing**
   - Serialization logic can be tested independently
   - Format-specific tests don't pollute Split class tests
   - Easy to test new formats without modifying existing tests

## Detailed Design

### Implementation Phases
1. **Phase 1: Framework Setup**
   - Introduce SplitSerializer interface
   - Create JsonSplitSerializer implementation
   - Add configuration support
   - Maintain backward compatibility

2. **Phase 2: Thrift Implementation**
   - Implement ThriftSplitSerializer
   - Add Thrift-specific configuration options
   - Add migration utilities if needed

3. **Phase 3: Migration Support**
   - Add tools to help users migrate between formats
   - Provide documentation and examples
   - Add monitoring and metrics

### Backward Compatibility
- Default to JSON serialization if no configuration is provided
- Support reading both formats during migration
- Provide utilities to convert between formats

### Performance Considerations
- Lazy loading of serializers
- Caching of frequently used instances
- Monitoring of serialization performance

### Security Considerations
- Validation of serialized data
- Safe handling of malformed input
- Proper error handling and logging

## Alternatives Considered

### 1. Multiple Annotation Sets
Could add Thrift annotations alongside JSON annotations. Rejected because:
- Clutters domain classes
- Makes adding new formats increasingly complex
- Harder to maintain and test

### 2. Format-Specific Classes
Could create separate classes for each format. Rejected because:
- Duplicates business logic
- Increases maintenance burden
- Complicates plugin architecture

### 3. Custom Binary Format
Could create a custom binary format. Rejected because:
- Requires maintaining custom serialization code
- Lacks ecosystem support and tools
- Increases learning curve for contributors

## Implementation Plan

### Phase 1 (Week 1-2)
- [ ] Create SplitSerializer interface
- [ ] Implement JsonSplitSerializer
- [ ] Add configuration framework
- [ ] Write tests for basic functionality

### Phase 2 (Week 3-4)
- [ ] Implement ThriftSplitSerializer
- [ ] Add Thrift configuration options
- [ ] Write Thrift-specific tests
- [ ] Add performance benchmarks

### Phase 3 (Week 5-6)
- [ ] Create migration utilities
- [ ] Write documentation
- [ ] Add monitoring and metrics
- [ ] Create example implementations

## Future Work
1. Apply similar pattern to other serializable types
2. Add support for Protocol Buffers
3. Create performance optimization tools
4. Add automatic format negotiation

## Questions and Discussion Topics
1. Should we support multiple active formats simultaneously?
2. How should we handle version migrations?
3. What metrics should we collect?
4. Should we provide a plugin system for custom formats?

## References
- Existing Split implementation
- Thrift documentation
- Similar patterns in other systems
- Performance benchmarks

## Appendix

### Example Configuration
```properties
# Basic configuration
split.serialization.type=thrift

# Advanced options
split.serialization.thrift.compression=true
split.serialization.thrift.protocol=binary
```

### Migration Guide
```java
// Example migration utility
public class SplitMigrationUtil {
    public static byte[] convertFormat(
        byte[] data,
        SplitSerializer<?> sourceFormat,
        SplitSerializer<?> targetFormat
    ) {
        Split split = sourceFormat.deserialize(data);
        return targetFormat.serialize(split);
    }
}
```

### Monitoring Metrics
- Serialization time per format
- Deserialization time per format
- Data size per format
- Error rates per format
- Format usage statistics
```

This updated RFC:
1. Focuses on the extensibility benefits rather than lock-in concerns
2. Provides more detailed implementation examples
3. Includes concrete migration strategies
4. Adds monitoring considerations
5. Provides a clear path for future formats

Would you like me to expand on any particular section or add more details to specific aspects?

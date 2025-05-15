# Improving Presto Performance with Thrift Migration: A Journey from JSON to Thrift

## TL;DR

Successfully migrated Presto coordinator-worker communication from JSON to Thrift serialization using a phased approach.
First phase complete with simple fields migrated, showing ~3% CPU reduction and 10% less Java old gen GC. Second phase (
migrating complex connector data) in review with significantly larger performance gains expected.

## Context

Presto's coordinator can become a bottleneck when handling multiple heavy queries due to high CPU usage from JSON
serialization/deserialization. Our project aimed to replace JSON with Thrift to improve serialization efficiency, reduce
CPU usage, and enable the coordinator to handle more queries concurrently.

The migration involved numerous deeply nested data classes (e.g., TaskUpdateRequest, TaskInfo) where one class contains
fields of other classes, which themselves contain fields of other classes, creating a complex object hierarchy. This
nested structure made a complete, one-time migration impractical and risky.

## Methodology

We adopted a two-phase strategy:

**Phase 1 (Completed):**

- Used Thrift serialization for simple fields within complex data classes
- Preserved JSON serialization for complex connector-related fields involving Java polymorphic classes
- Maintained backward compatibility
- Added configuration flags to enable/disable Thrift transport

**Phase 2 (In Review):**

- Migrating complex connector-related data fields (e.g., splits)
- Expected to deliver significantly larger performance gains

## Improvements

The first phase alone delivered measurable performance benefits:

- ~3% reduction in overall CPU usage
- ~10% reduction in Java old gen garbage collection
- Improved coordinator efficiency, allowing for higher query throughput

These improvements enable:

1. More efficient resource utilization at the coordinator level
2. Potential to simplify multi-coordinator setups to single-coordinator architecture, improving cache hit rates
3. Reduced bottlenecks in large Presto clusters

## Challenges

1. **Complex Object Hierarchy**: Managing numerous data classes with deep nesting where one class contains fields of
   other classes, creating a complex web of dependencies.

2. **Java Polymorphism**: Thrift doesn't natively support Java polymorphic classes, requiring creative solutions for
   connector-related data structures.

3. **Technology Selection**: Conducted extensive analysis between Apache Thrift and Facebook Drift libraries. After
   thorough evaluation and team discussions, we selected Drift for its better integration with Presto.

4. **Drift Enhancements**: Required modifications to the Drift library itself to support all our requirements.

5. **Backward Compatibility**: Ensuring seamless operation during the phased rollout.

6. **Cross-Language Support**: Coordinating with C++ worker implementations to ensure protocol compatibility.

## Open Source Collaboration

As Presto is an open source project, we created and presented an RFC to the Presto Technical Steering Committee. This
step was crucial to:

- Gather feedback from the broader Presto community
- Ensure our approach aligned with Presto's architectural vision
- Discuss potential impacts on various Presto deployments
- Gain insights from community members with different use cases

The RFC process helped refine our approach and ensured the changes would benefit the broader Presto ecosystem.

## Teamwork

This project was a collaborative effort among:

- Our team leading the Java coordinator migration work
- Vivian handling the C++ worker implementation
- Andrii providing overall technical guidance and code reviews

The collaboration required careful coordination across Java and C++ codebases, synchronized feature releases, and
thorough testing to ensure protocol compatibility.

## Next Steps

We're excited about the completion of Phase 2, which focuses on migrating complex connector-related data structures.
Based on profiling data, this will deliver substantially larger performance gains than Phase 1, as these structures
represent the bulk of coordinator-worker communication.

The improved serialization efficiency will further reduce coordinator CPU usage, enabling Presto to scale more
effectively for high-query-volume workloads while maintaining a simpler and more efficient architecture.
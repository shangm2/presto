Presto Coordinator Task-Level Event Loop Migration: A Major Performance Win

TL;DR

We successfully migrated Presto coordinator task handling from an executor/thread pool model to an event loop model. This change led to a 90% reduction in CPU usage on the Presto task scheduler across both Java and C++ clusters for adhoc and batch workloads. By eliminating lock contention and improving system efficiency, this foundational change marks a major step forward in Presto’s concurrency model. The feature is being rolled out gradually, with full deployment expected in the coming weeks.

Context

Presto’s execution model breaks queries into stages, further divided into tasks that are distributed to worker nodes. The HTTP remote task component—critical for managing distributed tasks—previously relied on a thread pool-based concurrency model with explicit synchronization, leading to significant lock contention and CPU overhead.

The shift to an event loop model represents a fundamental change in how Presto handles task-level concurrency. It’s part of a broader initiative to modernize and streamline Presto’s concurrency architecture, with future improvements planned for stage and query-level components.

Improvements

	•	CPU Efficiency: 90% reduction in CPU usage on the Presto task scheduler
	•	Lock Elimination: Removed synchronization overhead by serializing operations through event loops
	•	Scalability: Improved ability to handle high task concurrency with minimal resource overhead
	•	Architecture Simplification:
	•	Dedicated event loop thread manages all access to mutable state
	•	External threads submit tasks to the event loop
	•	Operations are serialized, eliminating the need for explicit locks and reducing complexity

Challenges and Mitigation

The path to production was carefully managed to ensure stability. While our initial rollout on February 2nd coincided with a SEV, thorough investigation confirmed that the incident was unrelated to the event loop migration. Still, it underscored the need for robust deployment and rollback strategies when introducing foundational changes.

Following this, we implemented a more cautious and resilient deployment plan. We began with extensive testing on verifier clusters to validate functionality and performance in a controlled setting. Additionally, we introduced feature toggles to enable or disable the event loop dynamically, allowing fine-grained control during rollout.

We’ve adopted a phased rollout strategy. The event loop changes are now live in 6 clusters, with 20 more in progress. This approach allows us to monitor impact across different workloads and environments, enabling rapid iteration if needed. We expect full deployment across all Presto clusters within the next two weeks.

Team Effort and Acknowledgments

This milestone would not have been possible without strong collaboration. Special thanks to David, Tom, and Jim for their guidance and deep architectural insight throughout the migration. Their support was instrumental in navigating the complexities of this change.

The successful implementation and rollout reflect not only technical innovation but also thoughtful execution—demonstrating how cross-functional teamwork can deliver meaningful performance improvements while managing risk.
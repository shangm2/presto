# Why Reducing Unidash Dashboard Loading Time Is Important for Meta's Business
    1. Productivity and Decision-Making
    Unidash is a critical internal analytics platform, with ~20K daily and ~60K monthly active users. Many teams rely on it for operational monitoring, business intelligence, and decision-making.
    When dashboards take minutes to load (e.g., p90 load times of 3 minutes for Tier 0 dashboards), users spend significant time waiting, which directly reduces productivity and delays data-driven decisions. Fast dashboards enable teams to react quickly to business and operational issues, improving agility and outcomes

    2. Business Impact and Opportunity Cost
    Slow dashboards create a "waiting tax"—every minute spent waiting is a minute not spent analyzing, acting, or collaborating. For high-traffic dashboards, this adds up to thousands of hours lost across the company each month.
    In audits and reviews, slow loading can add hours to processes (e.g., auditing 42 markets in Unidash can add over an hour just in loading time)

    If dashboards were as efficient as Excel, Unidash could save an additional 3-4 hours per audit, translating to substantial business value.

    3. User Experience and Adoption
    Poor performance discourages use. If dashboards are slow, users may avoid them, miss critical insights, or revert to less scalable tools (like Excel), undermining the value of centralized analytics

    Responsive dashboards make analytics feel accessible and encourage broader adoption, leading to more data-driven culture.

    4. Reliability and Trust
    Performance standards are now enforced: dashboards tagged as “Performance Guaranteed” must load in <7s (initial) and <5s (reload). SEVs can be filed if these standards are not met

    Meeting these standards is essential for reliability, especially for Tier 0 dashboards used by executives and for company-critical workflows.
    5. Technical and Cost Efficiency
    Slow dashboards often indicate inefficient queries, poor caching, or excessive widget counts, which not only slow users but also increase backend compute costs and risk system overloads
    Optimizing load times reduces infrastructure costs and improves system stability.

    6. Survey feedback
    I want to cross share the DevEx survey results and encourage everyone to read and dig into the data if you haven't already https://fb.workplace.com/groups/e/posts/7526332667422851, it includes a section around data tools and where people feel like they are being slowed down. Some of the feedback may not be new, but it's a good reminder for how our largest audience feels about using our tools

    Insights doc: https://docs.google.com/.../1G1Jj7eiH06rguptAgZ3.../edit...

    Some insights

    Satisfaction with Metamate increased by 17 ppt (43% to 61%), and while respondents voiced concerns about its accuracy and lack of integration with internal tools, they also noted that AI and automation (through Metamate and other forms) improved the efficiency of their workflows (specifically, the quality of auto complete and code suggestions in Daiquery, VS Code @ Meta and Android Studio).

    23% of Unidash users reported slowdowns, which was the highest among data tools that were asked about

    This was followed closely by 22% of Bento users, and 21% of Daiquery users reporting slowdowns.

    In open-ended responses, developers complained about Unidash’s slow loading and bugginess that led to difficulty in debugging. They found it painful to create and edit dashboard, and lack of flexibility in creating dynamic dashboards. The lack of documentation can delay the process of seemingly easy tasks.

    They complained about Daiquery’s slowness in query execution, difficulty to query across namespaces without Uhauls, and frequent time-out. Privacy and access control hampered productivity due to the long wait to obtain permission.


# Strategy Recap
    At the start of the year, we lacked a shared SLO, workload model, and visibility into latency attribution. To address these challenges we:
    - Defined supported workloads and latency targets based on end to end research with upstream teams
    - Built comprehensive real-time telemetry to model latency at each layer by adding runtime stats
    - Introduce a modern high performance front end for the interactive Warehouse working with product team
    - Worked with dependencies to improve guarantees for interactive workloads
        -- Tetris: we built stronger co-location guarantees to minimize cross-region latency while maintaining balanced resource utilization for interactive warehouse workloads.
            The main outcome is adding a method to GTR for table locality lookups. This will accept only a table name and return the current primary region for that table (considering DR events). It will not verify partition locality, do any resource balance scoring, etc.

            The intention is to provide a low-latency means for routing these queries without dropping Tetris completely, as the latter would require additional dependency on NSSR and other duplication. We also want to set GTR up for more robust Interactive support longer term.

            The driver for this change is that DI Analytics has taken on Tier-0 Dashboard SLOs: 5s in H1, aiming for 1s in H2. Routing is only a part of this SLO. So, we need p95 routing under 100ms; ideally, under 50ms. We think this is achievable for table locality lookups in the short-term.

            --- Presto Tetris Deep dive summary

            The Tetris and Presto team recently conducted a deep dive into several critical issues, resulting in a highly productive day. We explored various topics in detail, achieving a mutual understanding of both Tetris and Presto systems (Interactive and Batch). The main discussion points included:

            Tetris planner and router architecture, along with associated challenges.

            Operational and queueing issues in Presto Batch.

            Presto Interactive and the Interactive Data Consumption Initiative.

            Integration of Presto Interactive capacity with Tetris planner and its challenges.

            Challenges faced by latency-sensitive use cases due to unpredictable table movements across regions, leading to cross-region traffic.



            Key Takeaway and Action Items:


            Presto Batch and Its operational challenges:

            We reiterated that already planned items in the Tetris roadmap to improve compute balance is necessary but may not be sufficient to improve the operational balance. We also need to validate and ensure the quality of the datasets which planner is using to balance compute.

            Several classes of issues identified which affect Tetris planner decision making, resulting in imbalances across different regions.

            The Tetris team would look at BDP and Tetris planner/router holistically and analyze how it impacts the customer experience.

            We decided to collectively invest in a system, which provides insights into demand movements, and the impact to the end user experience.



            Presto Interactive

            Presto Interactive capacity is measured by the number of available slots. A holistic approach is needed to model concurrency slots in the Tetris planner. The current method of modeling does not support metrics like slots, which cannot be aggregated.

            The Presto Interactive team will provide criteria to identify all interactive workloads. These criteria will be based on factors such as product, predictability, and table sizes.

            The above criteria will be utilized to determine if there are any overlaps between Presto Interactive tables and ML tables. The outcome of this analysis will shape the overall strategy to address interactive challenges.

            The established criteria for Presto Interactive will guide the strategy for managing Presto Interactive table movements. The key principle is to avoid regression in cross-region traffic for these use cases.

            Like Presto batch, Tetris will offer insights into demand movements and their impact on Presto Interactive use cases.

            Tetris will also provide a fast, table-only region lookup API, with an initial SLA of p95 of 100ms and a goal to bring this down to P99 of 50ms. This API will return the DR region as an alternative region for query execution. In cases involving multiple tables, the regions for all tables will be returned.

            The implementation of the above mechanism is a prerequisite for the rollout of global namespaces for top interactive namespaces.


        -- Warmstorage: we moved to IO driver v2 while deprecating colocated proxies, which helped in improving WS API performance.
        -- Metastore: we built the project accelerator to make a step function change in the performance of the Metastore APIs.
        -- Permissions: we migrated to the service router, which helped improve the performance of the Permissions API.

        Presto and Tetris held a deep dive and reached alignment on multiple issues, including on the plan for Presto Interactive and global namespace rollout criteria. This alignment resolves an identified risk in the Feb update.
        Presto and Tetris also reached alignment on a new tetris API (ETA: April 15th) for simple region lookup with an initial SLA of p95 of 100ms and a goal to bring this down to P99 of 50ms. This work will enable Presto to improve gateways latencies by 1.5-2 seconds (P90).
        We have also resolved the issues with the Maven shaded library for memcache that were identified as a risk in the February update. We have prepared an implementation for the table cache that is ready to be merged. The memcache-based call shows significant improvement for the tested queries vs retrieving the table from the metastore.
        Continue to make progress on cluster restructuring, caching improvements, and observability.
        The Warmstorage team had kick started a discussion on the initial SLO proposal. More discussions need to happen to align on the SLOs and timelines.  https://fb.workplace.com/groups/714449877252892/permalink/788798763151336/

# early result
    Interactive Warehouse H1 Summary

    This is a 0 to 1 effort to enable end-to-end performance improvement and measurement for data consumption. In H1 we published interactive SLO in line with industry peers, and SLO attainment for exploratory traffic for HI PRI workloads improved >3x, to 60% from 20% baseline (targeting 90%+ by EOY).



    To ensure users can effectively interact with data, we have made significant investments in ensuring authors can build dashboards that will offer snappy interactivity (meaning under 5 seconds end to end) for supportable dashboards widgets. We established guidelines and SLO for authors; these clearly define what are supportable and unsupportable dashboard widgets. Authors can now use in-tool performance prioritization settings, review in-tool performance feedback for dashboard widgets and in-tool statistics about dashboard performance. In H2, we continue to speed up the e2e infrastructure as well as expand the set of queries that can be supported within the performance SLO. While data access has become faster, there’s lots more to do.

    Workstreams

    WS 1: Performance Program

    DE/DI Partnership - Working with DE partners, we built and published joint SLOs for interactive products, an updated product arc, and a pillar level adoption program with 12 DE pillars committed to H2 adoption plans & shared performance standards.

    Waste Reduction - DE pillars are committing to eliminating wasteful queries (>500GB). IG pillar has already eliminated all class D2 queries, cutting overall resource usage in half. Other DE pillars are on track for similar gains during H2. (see guidance)

    Customer Support - Defined a DI-wide “perf IMOC” oncall and WP group to provide customers with a centralized support strategy. Collected best practices into a central wiki.

    WS 2: Client Layer

    Platform Latency - Reduced platform overhead latency (i.e. access checks, query/data transforms, etc.) from 4s to ~1.1s (context) exceeding the original goal of 1.2s.

    Perf UX - Delivered compliance tools to support DE visibility/prioritization and efficient optimization, unblocking pillar signoff process.

    Measurement - Delivered E2E observability with detailed, system-level breakdown to provide real-time alerting/monitoring across the interactive data stack.

    WS 3: Gateway

    Onyx in Prod - Onyx fully rolled out to 100% of Quartz Unidash Presto traffic with reduced overhead by ~20% relative to (Quartz) baseline.

    Perf Classification and Class D Handling - Onyx leverages Crux to analyze queries and also analyzes Presto responses for perf classification, which enables SLO attainment tracking based on compliant perf class (A/B/C), and deprioritizing Class D queries (only D2 subclass so far).

    Saber Integration in Validation - Onyx Optimizer is largely code complete with E2E testing and validation now under way with registering and serving M360 queries with Saber to accelerate dashboards.

    WS 4: Presto

    Gateway Latency - Presto Gateway's latency improved significantly (P90: 2-3s to 50ms, 40-60x+ improvement). It was a result of great collaboration with the Tetris team.

    Queueing Latency - Improved the resourcing hierarchy and improved the queuing for tier0 queries (p95 from 300ish seconds to almost 0).

    Permission/Metadata Check Latency/Frequency - The number of checkCatalogAccess to DIPS has been reduced by 10x. checkCatalogAccess is one of the permission calls Presto makes during query execution. Reduced unnecessary Metastore calls for unidash queries with CTE by over 50% on average per query.

    Interactive Cluster Restructuring: Presto Interactive serves a variety of workloads, ranging from True Interactive to Programmatic. This project looks to provide semi isolation to the Interactive workload hence benefiting both Reliability and Latencies. This project is feature complete and has been rolled out in the VLL region.

    Caching Improvements

    Metastore: We initially started leveraging memcache for metastore caching. While the initial results were good, it did not yield good improvements due to memcache non configuration TTL option. Memcache is designed for heavy QPS and it keeps evicting metadata not used within hours. We are looking at alternative storage solutions for it. We are also working with the Metastore team to improve the performance of the key APIs.

    Warm Storage: We built weighted randomized scheduling to balance the affinity and cluster hotness. This is still pending for rollout.

    Global Namespace Support (Tetris) - Over this half we faced many unplanned activities due to various namespaces going global. The Data Management in the Global namespaces design do not consider data locality concepts, which are critical for low latencies.



    iWH Integration

    Dataswarm - Saber now leverages Dataswarm to run data ingest pipelines, periodically re-ingesting full tables. Original plan was to leverage Dataswarm for incrementally ingesting partitions after they land, but gaps in capabilities required Saber to invest directly in scheduling in both H1 (periodic) and H2 (event-driven).

    Metastore - Saber now polls Metastore for updates to determine data staleness. Engaging with the Metastore team to offer event subscriptions in H2.

    Onyx Optimizer - Saber supports query shape registration through the Onyx Optimizer. Saber provided support and eng cycles to enable this. Engaging with Onyx to support more query shapes in H2.

    Onyx Frontend - Onyx frontend now supports Saber as a compute engine. Saber provided support and eng cycles to enable shadow validation and enable low latency privacy checks.

    Hive Security - Saber now has superuser permissions for Hive in order to support transparent acceleration.

    Looking forward

    In H1 we goaled and provided SLO on HI PRI exploratory Unidash traffic (< 2% of overall Unidash traffic). In H2 we are going to switch our SLOs to compliant classes and go after all Unidash compliant traffic.

    Increase SLO coverage for perf-compliant traffic: <2% (Only HI-PRI traffic) ->100% (All perf compliant traffic)

    Increase % of overall Unidash traffic that is perf-compliant: 65% -> 70% (stretch: 75%)

    Improve SLO attainment for Unidash perf-compliant traffic: 80% -> 90% (stretch: 95%)

    We will continue to build interactive warehouse infrastructure to expand the supported class for Unidash in H2 and onboard new customers and use cases in 2025.



# What is Next
With Dashboard loads reaching satisfactory levels, our focus expands to data exploratory related requirements. Data Exploratory needs are more challenging due to the exploratory nature. To tackle this need, we kick started a cross collaboration effort (v-team) across Presto, DXI and Saber to make interactive data consumption snappy. Some details for this sync are shared here. Please note that the content of this doc is still work in progress.

- Query and Data Complexity

    One of the observations is that current tier0 dashboards are marked tier0 based on criticality. However, many of these dashboards do not adhere to best practices and result in expensive queries. It’s not practical to expect snappy performance from dashboards scanning trillions of rows. We are defining various classifications for tier0 traffic based on data and query complexity.

- Query Isolation/Dedicated clusters

    Presto currently provides dedicated cluster experience for most pnb region queries, however the dedicated clusters are not available in all regions. Procuring dedicated capacity in all regions is a complex exercise. We want to move to a more balanced (dedicated vs shared) approach in 2024 H1.

    We are also looking to have some sort of isolation for UER traffic to provide better reliability. We plan to have clusters in all regions serving UER and latency sensitive traffic. This does not provide complete isolation, but would be a good balance between performance, reliability, and maintenance.

- agentic workload
    AI agent queries have grown 10x in the last 90 days, now representing 10% of daily peak demand in Presto Interactive. More detailed analysis is here: https://fb.workplace.com/groups/1343653826385658/permalink/2087399072011126/

    Our agent ecosystem has expanded significantly, with over 90 unique agents (currently known) now leveraging Presto for data exploration.



    - Faster Interactivity via Interactive Warehouse (iWH)

        We made tremendous improvements with the interactive warehouse, which led to amazing improvements in various internal and external products. The details of these improvements were shared in this post.

        We successfully onboarded two major agents—Datamate and Analytics Agent—to the Interactive Warehouse (iWH). This transition yielded significant performance gains: Datamate’s P90 latency improved by 70%, while Analytics Agent’s P90 dropped by 72% (from 79s to 22s).

        The interactive Warehouse (iWH) previously required the query cost to be known upfront for workload onboarding. This proved challenging given the diverse and exploratory nature of agentic workloads. To overcome this limitation, we developed the NoCost mechanism, which enables the onboarding of various workloads to iWH. Due to this we have successfully onboarded the analytics agent and has also led to improved query execution times for Datamate queries that were previously mispredicted. With these mechanisms in place, we should be able to onboard all interactive agentic exploration to interactive warehouses easily.




    - Workload Optimization

        To support the growing needs of agentic exploration, we previously identified a critical goal: optimizing our engine and workloads to significantly reduce the per-query cost. Achieving this cost reduction is dependent on successfully deploying several key technologies.

        Materialized Views is also now in production. It's currently serving critical external analytics workloads. We’ve also onboarded few Unidash dashboards to materialize views. These views are refreshed on an hourly basis for 2 weeks. The performance improvements range from 5-13x.

        These improvements provide the essential headroom required for the anticipated surge in agentic exploration. Leveraging these technologies to scale alongside this rapid growth remains a top priority for Interactive Infrastructure.


    - Data Discovery & Query Authoring

        Fast and Efficient Metadata Access was identified as a key area to help us with faster data discovery and query authoring, eventually resulting in improved Time To Complete for datamate sessions. We launched the Table Insights service to provide agents with near-instant access to metadata and sampled data.

        We have integrated Table Insights into Datamate's Column Distinct Tool, rolling it out to 100% of datamate_dev and a select group of production users. The performance gains are significant: request execution times average 1.128s on cache hits (a 7x improvement vs 7.175s without the cache). On an average 48% queries hit the caches.

        This integration has reduced Datamate's overall benchmark execution time by 15%. Furthermore, Table Insights generates more efficient queries; for example, our optimized query is 8.4x faster than the original query, despite fetching additional columns. We are continuing to migrate users and expect to reach 100% adoption across Datamate within the next few weeks.

        Advanced ML via Meta AI Functions in Presto

        Building upon the enhanced agentic exploration, we've also facilitated deeper insights by directly integrating Python User-Defined Functions (UDFs) into Presto. This powerful feature enables the execution of Meta AI functions (such as summarization, sentiment analysis, translation, and embeddings) directly within standard SQL. As a result, agents can now transform raw data into valuable insights without ever needing to leave the data platform environment.

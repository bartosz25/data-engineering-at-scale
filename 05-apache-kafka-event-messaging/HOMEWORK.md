# Homework

## Exercise 1 — End-to-end architecture with Apache Kafka

Look around your own professional or personal experience and identify a real system — one you work with,
have worked with, or use as a consumer — where Apache Kafka would be a natural 
fit as the central messaging layer. Examples could be a payment processing pipeline,
a ride-sharing dispatch system, an IoT sensor network, a social media feed,
a logistics tracking platform, or anything else you find credible.

### Task

Design a complete data architecture for your chosen domain, using Apache Kafka as the backbone. The architecture must cover all stages from raw event ingestion to at least two distinct output consumers with different latency requirements (e.g. a real-time operational view and a historical analytics or ML use case).

Your answer should include:

1. **Domain description** — one short paragraph explaining the system you chose, what events it generates, and why Kafka is a good fit.
Be specific: name the entities, the event types, and the rough volume you would expect.

2. **A component diagram** — either a textual description, draw.io or an ASCII sketch — showing every system involved:
producers, Kafka topics, consumers, storage layers, and output systems. Label each arrow with the data format (JSON, Avro, Parquet, etc.) 
and the approximate latency target.

## Exercise 2 — Reprocessing strategy for `scaling_consumers_worker`

The `scaling_consumers_worker.py` consumer currently uses `auto.offset.reset=latest`, which means each worker only reads
messages produced *after* it joins the group. The consumer group is `scaling-consumer-group` and the topic
`scaling-demo` has 3 partitions.

Suppose a bug was discovered in the processing logic (the part that prints partition/offset/user/page). The bug 
silently dropped records where `page == 'contact'`. All workers have been running for 48 hours. The topic retention 
is 7 days, so all messages are still available.

### Task

Describe step by step how you would reprocess all 48 hours of data through the fixed worker code. Your answer must address:

1. How do you make the consumers re-read data that has already been committed? Mention the exact Kafka mechanism or tool you would use and how you would use it.
2. Do you need to stop the live workers first, or can you reprocess in parallel? Justify your choice.
3. How do you know when reprocessing is complete?
4. After reprocessing, how do you return the consumer group to normal live-consumption mode?

Every reprocessing strategy has drawbacks. Identify at least four concrete problems with the approach you described in Part A. For each one, explain *why* it is a problem and, where possible, suggest a mitigation.

To get you started, consider the following angles (you are not limited to these):

- **Duplicate side effects** — what happens to external systems (databases, APIs, notification services) that the worker writes to during normal processing?
- **Ordering guarantees** — does Kafka guarantee that reprocessed messages arrive in the same order across partitions as they did originally?
- **Latency** — if the live producers are still producing date, will the reprocessing consumers ever catch-up?
- **Offset reset scope** — the `kafka-consumer-groups.sh --reset-offsets` tool resets offsets for the entire group. What is the risk if the group has consumers that own partitions you did NOT intend to reprocess?
- **Immutability assumption** — the strategy assumes the source data in Kafka is identical to what was originally processed. Under what circumstances could that assumption be false even within the retention window?
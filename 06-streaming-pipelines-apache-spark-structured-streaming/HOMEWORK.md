# Homework

## Exercise 1 — The Noisy Neighbor Problem in Micro-Batch Processing

Micro-batch processing is the default execution model in Spark Structured Streaming. It is simple to reason about, but it comes with a well-known drawback called the **noisy neighbor problem**.

**Tasks:**

1. **Explain** what the noisy neighbor problem means in the context of Spark Structured Streaming micro-batch execution. Your explanation should cover:
   - What a "noisy neighbor" is in this context.
   - How a slow or resource-heavy micro-batch affects the batches that follow it.

2. **Demonstrate** the problem by modifying [`kafka_consumer_micro_batch.py`](kafka_consumer_micro_batch.py).
   Simulate a noisy neighbor by introducing artificial slowness inside `print_visit_rows_from_foreach_partition`. 


## Exercise 2 — Beyond Windows: Other Stateful Operations

The windowed aggregation shown in [`browsers_stats_generation_stateful_job.py`](browsers_stats_generation_stateful_job.py) is one way to maintain state across micro-batches. 
Spark Structured Streaming supports several other stateful processing capabilities.

**Tasks:**

1. **List** other stateful operations available in Apache Spark Structured Streaming (besides windowed aggregations). For each one, briefly describe what kind of problem it solves. Examples to explore:
2. **Implement** a new streaming job using the same input dataset as `browsers_stats_generation_stateful_job.py` (Kafka topic `visits`, schema: `eventTime TIMESTAMP, browser STRING`). Choose **one** of the stateful operations you identified above (not windowed aggregation) and build a job that uses it. Write the output to a new Kafka topic of your choice.
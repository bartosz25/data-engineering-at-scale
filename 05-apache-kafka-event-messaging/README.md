# Apache Kafka Event Messaging


## Prerequisites
1. Install the dependencies:
```bash
uv lock
uv sync
```

2. Start Apache Kafka broker:
```bash
# colima start ; if your Docker is not running
cd docker
docker-compose down --volumes; docker-compose up
```


## Demo 1 — Topic Composition
Files: `[topics_composition.py](topics_composition.py)`

**Concept:** A Kafka topic is physically divided into partitions. 
Each record within a partition is identified by a monotonically
increasing *offset*. A record's full address is therefore `(topic, partition, offset)`. 
The message key determines which partition a record lands on via a deterministic hash — 
the same key always goes to the same partition.

1. Start the demo:
```bash
uv run topics_composition.py
```

The script is fully self-contained: it produces 10 messages and immediately reads them back.


## Demo 2 — Multi-Reader
Files: `[multi_reader_consumer_a.py](multi_reader_consumer_a.py)`, `[multi_reader_consumer_b.py](multi_reader_consumer_b.py)`
`[multi_reader_producer.py](multi_reader_producer.py)`

**Concept:** Kafka is a log, not a queue. A message is not removed after it is consumed. 
Two consumers with *different* `group.id` values each maintain independent
read positions (offsets) and therefore each receive every message in the topic
— the second reader does not see a "depleted" topic.

1. Produce some data
```bash
uv run multi_reader_producer.py
```

2. Start both consumers in separate terminals simultaneously:

```bash
# terminal 1
uv run multi_reader_consumer_a.py
```

```bash
# terminal 2
uv run multi_reader_consumer_b.py
```


Both terminals print all 30 events. The `event_id` column runs from 0 to 29 in both outputs. Consumer A and Consumer
B have independent committed offsets stored under `reader-group-a` and `reader-group-b`
respectively. Stopping one consumer does not affect the other.

Apache Kafka retains messages regardless of how many consumers have read them.
The retention period — not consumption — determines when a message is deleted. It's the opposite
of a message queue where the message is gone after consuming it.


## Demo 3 — Scaling Consumers (`scaling_consumers_`)
Files: `[scaling_consumers_producer.py](scaling_consumers_producer.py)`, `[scaling_consumers_worker.py](scaling_consumers_worker.py)`

Partitions are the unit of parallelism in Kafka. A consumer group distributes partitions 
among its members. When a new consumer joins (or leaves), Kafka triggers a *rebalance* — partitions
are redistributed across all active members. With 3 partitions, 1 consumer owns all 3; 
with 2 consumers, one gets 2 and the other gets 1; with 3 consumers, each gets exactly 1.

1. Start the producer and keep it running:

```bash
uv run scaling_consumers_producer.py
```

2. Add workers one at a time, observing the rebalance lines:

```bash
# terminal 1
uv run scaling_consumers_worker.py --worker-id 1
```

Wait for the consumer to read the first batch of records, then in a new terminal:

```bash
# terminal 2
uv run scaling_consumers_worker.py --worker-id 2
```

Wait again and start another consumer:

```bash
# terminal 3
uv run scaling_consumers_worker.py --worker-id 3
```


As each new worker joins, **all existing workers** print a rebalance notice. 
The partition assignments converge to one partition per worker.


3. Stop worker 3 (`Ctrl+C`) and watch workers 1 and 2 rebalance again to cover all 3
partitions.

You can never have more active consumers than partitions in a single group — the fourth
consumer would sit idle with no partitions assigned. Scale partitions at topic creation
time; scale consumers at runtime.

## Demo 4 — Progress Tracking
Files: `[progress_tracking_consumer.py](progress_tracking_consumer.py)`,
`[progress_tracking_producer.py](progress_tracking_producer.py)`

Kafka tracks consumer progress by storing *committed offsets* in the 
internal `__consumer_offsets` topic. When a consumer restarts after a crash, it 
reads those offsets and resumes from the last committed position — only messages with an offset
greater than or equal to the committed offset are replayed.

1. Populate the demo topic:
```bash
uv run progress_tracking_producer.py
```

2. Reads the first 10 records then crash 50 events and commits each one individually:
```bash
uv run progress_tracking_consumer.py --fail-after 10
```
After each commit you will see the offset state for every partition.
The consumer commits 10 messages and then raises an exception. Note the final committed offsets printed 
before the crash.

3. Recovery run:

```bash
uv run progress_tracking_consumer.py
```

The consumer starts from the committed offsets — the first 10 messages are **not** reprocessed. 
The `[OFFSETS IN __consumer_offsets]` line on the very first poll confirms it 
resumes mid-stream.

The committed offset is the next offset to read, not the last one read. Committing offset
5 means "I have processed everything up to and including offset 4; resume from offset 5 on restart."

---

## Demo 5 — Queues (`queues_`)
Files: `[queues_consumer.py](queues_consumer.py)`, `[queues_producer.py](queues_producer.py)`

Apache Kafka 4.0 introduced *Share Groups* (KIP-932) — a new consumer protocol that 
delivers true queue semantics. Unlike a regular consumer group where each consumer owns 
dedicated partitions and reads their full stream, a share group hands individual 
messages to whichever worker polls next. Each message is delivered to **exactly one** 
worker in the group, regardless of which partition it came from.

The key API difference: share consumers call `acknowledge(msg)` instead of `commit()`. 
An unacknowledged message (e.g. after a worker crash) is automatically redelivered to
another worker.

Kafka and client version requirements for this new feature are:
- **Kafka 4.0+** with `group.coordinator.rebalance.protocols=classic,consumer,share` (already set in `docker-compose.yaml`)
- **confluent-kafka >= 2.8.0** (already in `pyproject.toml`)

1. Start the producer and keep it running:
```bash
uv run queues_producer.py
```

2. Start two workers in separate terminals:

```bash
# terminal 1
uv run queues_consumer.py --worker-id 1

# terminal 2
uv run queues_consumer.py --worker-id 2
```

Each `job_id` appears in **only one** worker's output. The load is distributed across
workers without any partition assignment — the broker decides which worker gets the 
next message.


Notice that `job_id=0` is on `partition=0 offset=0` in worker 1's output and never appears in workers 2 or 3. 
This is the defining difference from a regular consumer group.

> If you replaced `ShareConsumer` 
with a regular `Consumer` using the same `group.id`, each worker would be 
assigned dedicated partitions and would read every message on those partitions 
in order — the messages would NOT be shared between workers.


> Use share groups when you want to parallelize processing of a 
high-volume stream and message ordering is not required. Use regular consumer groups when
you need strict per-key ordering or partition-level isolation.

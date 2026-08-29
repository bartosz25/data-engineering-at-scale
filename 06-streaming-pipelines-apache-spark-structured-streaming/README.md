# Apache Spark Structured Streaming

## Prerequisites:
uv and `uv sync` to install all required dependencies.
```bash
uv sync
uv lock
```

## Demo 1: Micro-batch processing model 
File: `kafka_consumer_micro_batch.py`

This demo reads website visit events from a Kafka topic and prints them to the console in 
micro-batches. It illustrates how Spark Structured Streaming divides an unbounded stream into small, 
bounded chunks of work.

1. Start Kafka and the visit data generator:
```bash
colima start
cd docker
docker rm -f kafka
docker-compose down --volumes && docker-compose up
```

Wait until you see `Successfully created the following topics` in the logs before proceeding.

2. Run the job:
```bash
uv run kafka_consumer_micro_batch.py
```
You should see the job processes a bunch of records each time and do not start the next 
execution till the previous one is not completed.

## Demo 2: Triggers and checkpoints
Files: `triggers_processing_time.py` and `triggers_available_now.py`
1. These two jobs demonstrate the most common trigger strategies. Both read plain text 
files from `{BASE_DIR}/input` instead of Kafka, which keeps the infrastructure simple 
and makes the trigger behavior easy to observe directly in the console.

| | `triggers_processing_time.py` | `triggers_available_now.py` |
|---|---|---|
| Trigger type | `processingTime='30 seconds'` | `availableNow=True` |
| Files per batch | 2 | 3 |
| Lifetime | Runs until stopped | Stops when all data is processed |
| Use case | Continuous real-time streaming | Batch-style run with checkpoint |

1. Create the input files
```bash
mkdir -p /tmp/data-engineering-at-scale/06-streaming-pipelines-apache-spark-structured-streaming/data/input
for i in $(seq 1 8); do echo $i > /tmp/data-engineering-at-scale/06-streaming-pipelines-apache-spark-structured-streaming/data/input/file${i}.txt; done
```
2. Run `triggers_processing_time.py`
```bash
uv run triggers_processing_time.py
```

The job fires every 30 seconds, consuming at most 2 files per batch.
Observe the `processing_time` column: the timestamps confirm the 
30-second cadence between batches.

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|2    |2026-08-29 05:48:23.311|
|3    |2026-08-29 05:48:23.311|
+-----+-----------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+-----------------------+
|value|processing_time        |
+-----+-----------------------+
|1    |2026-08-29 05:48:30.054|
|4    |2026-08-29 05:48:30.054|
+-----+-----------------------+
...
```

Stop it with `Ctrl+C`.

3. Run `triggers_available_now.py`
```bash
uv run triggers_available_now.py
```

`availableNow` tells Spark to process all data that exists at the moment the job starts,
then stop. It still respects the `maxFilesPerTrigger=3` limit, so it runs multiple 
micro-batches internally — but the job terminates on its own once the backlog is drained.

```
-------------------------------------------
Batch: 0
-------------------------------------------
+-----+
|value|
+-----+
|    2|
|    3|
|    1|
+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----+
|value|
+-----+
|    4|
|    5|
|    7|
+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----+
|value|
+-----+
|    6|
|    8|
+-----+
```

The job then exits cleanly. Run it again immediately: it will finish instantly with no 
output because the checkpoint records that all 8 files were already processed. 

Add new files to the input directory and run it once more — only the new files are 
processed. This is the key advantage over a plain batch job: the checkpoint resumes the work
without reprocessing already processed files.

4. Add new files:
```bash
for i in $(seq 11 15); do echo $i > /tmp/data-engineering-at-scale/06-streaming-pipelines-apache-spark-structured-streaming/data/input/file${i}.txt; done
```

5. Restart the job:
```bash
uv run triggers_available_now.py
```

Thanks to the checkpoint recovery, the job only processed records added after the 
previous job's completion:
```
-------------------------------------------
Batch: 3
-------------------------------------------
+-----+
|value|
+-----+
|   11|
|   12|
|   13|
+-----+

-------------------------------------------
Batch: 4
-------------------------------------------
+-----+
|value|
+-----+
|   14|
|   15|
+-----+
```

> Note: if the job fails mid-batch, the entire in-progress batch is replayed on restart, 
> not just the remaining rows.

## Demo 3: Stateless vs. stateful windowing 
Files: `browsers_stats_generation_stateless_job.py`, `browsers_stats_generation_stateful_job.py`)

Both jobs compute per-browser counts inside 5-minute event-time windows from the same 
Kafka `visits` topic. On the surface they look identical, but the aggregation scope is 
fundamentally different.

| | `browsers_stats_generation_stateless_job.py` | `browsers_stats_generation_stateful_job.py` |
|---|---|---|
| Aggregation location | Inside `foreachBatch` — runs on each micro-batch DataFrame independently | In the streaming plan — Spark accumulates results across micro-batches |
| State between batches | None — each batch starts from zero | Maintained by Spark — batch _n_ results feed into batch _n+1_ |
| Watermark | None | `withWatermark('eventTime', '20 minutes')` |
| Output topic | `browser-stats-stateless` | `browser-stats-stateful` |

The practical consequence: if Firefox appears in batch 1 and again in batch 2, 
the stateless job emits `count=1` twice. The stateful job emits `count=1` after
batch 1 and then updates to `count=2` after batch 2.


1. Start the Apache Kafka broker:
```bash
cd docker
docker-compose down --volumes && docker-compose up
```
2. Run the job stateless job
```bash
uv run browsers_stats_generation_stateless_job.py
```

3. Open a Kafka producer in a second terminal to send events manually:

```bash
docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic visits-manual-stateless-demo
```

Send the first 3 visits:

```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:01:00Z", "browser": "Chrome"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:02:00Z", "browser": "Safari"}
```

4. Open a consumer in a third terminal to watch the output topic:

```bash
docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic browser-stats-stateless --from-beginning
```

You should see one record per browser, each with `count=1`:

```json
{"browser":"Firefox","window":{"start":"2024-10-06T10:00:00.000Z","end":"2024-10-06T10:05:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T10:00:00.000Z"}
{"browser":"Chrome","window":{"start":"2024-10-06T10:00:00.000Z","end":"2024-10-06T10:05:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T10:01:00.000Z"}
{"browser":"Safari","window":{"start":"2024-10-06T10:00:00.000Z","end":"2024-10-06T10:05:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T10:02:00.000Z"}
```

Now wait for the next trigger (15 seconds) and send 2 more Firefox visits in the producer:

```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:03:00Z", "browser": "Firefox"}
```

The consumer emits a new Firefox record with `count=2` — counting only the two visits from this batch, not the one from the previous batch:

```json
{"browser":"Firefox","window":{"start":"2024-10-06T10:00:00.000Z","end":"2024-10-06T10:05:00.000Z"},"count":2,"last_event_time_in_window":"2024-10-06T10:03:00.000Z"}
```

This is the defining characteristic of stateless processing: the aggregation scope 
is bounded by the micro-batch. Stop the job with `Ctrl+C`.

5. Reset the environment so state does not carry over from the previous run:

```bash
cd docker
docker-compose down --volumes && docker-compose up
```

6. Run the stateful job:
```bash
uv run browsers_stats_generation_stateful_job.py
```

7. Open a producer and consumer as before, this time pointing to the `browser-stats-stateful` topic:
Producer:
```bash
# producer
docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic visits-manual-stateful-demo
```
Consumer:
```bash
# consumer
docker exec -ti kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic browser-stats-stateful --from-beginning
```

Send the same first 3 visits. The output looks identical to the stateless case — one record per browser with `count=1`:

```json
{"browser":"Firefox","window":{"start":"2024-10-06T10:00:00.000Z","end":"2024-10-06T10:05:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T10:00:00.000Z"}
{"browser":"Chrome","window":{"start":"2024-10-06T10:00:00.000Z","end":"2024-10-06T10:05:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T10:01:00.000Z"}
{"browser":"Safari","window":{"start":"2024-10-06T10:00:00.000Z","end":"2024-10-06T10:05:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T10:02:00.000Z"}
```

Now send the same 2 additional Firefox visits:

```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:00:00Z", "browser": "Firefox"}
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:03:00Z", "browser": "Firefox"}
```

This time the count reflects all three Firefox visits across both batches:

```json
{"browser":"Firefox","window":{"start":"2024-10-06T10:00:00.000Z","end":"2024-10-06T10:05:00.000Z"},"count":2,"last_event_time_in_window":"2024-10-06T10:03:00.000Z"}
```

8. Observing the watermark.
The stateful job uses `withWatermark('eventTime', '20 minutes')`. The watermark
defines the oldest event time Spark will still accept into an open window. It advances 
as the maximum observed event time increases. Any event older than `(max_observed_event_time - 20 minutes)` is
silently dropped.

**Late event within the watermark** — send an event timestamped at 09:40. The latest event seen so far is 10:03, so the watermark is at 09:43. 09:40 is just within the allowed range:

```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T09:40:00Z", "browser": "Firefox"}
```

A new window opens and is emitted:

```json
{"browser":"Firefox","window":{"start":"2024-10-06T09:40:00.000Z","end":"2024-10-06T09:45:00.000Z"},"count":1,"last_event_time_in_window":"2024-10-06T09:40:00.000Z"}
```

**Late event beyond the watermark** — send an event at 09:30, which is older than the current watermark:

```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T09:30:00Z", "browser": "Firefox"}
```

No new output appears. The event is dropped because its window has already been evicted.

**Advancing the watermark** — send an event far ahead in time to push the watermark forward:

```
{"userId": 1, "page": "index.html", "eventTime": "2024-10-06T10:40:00Z", "browser": "Firefox"}
```

The watermark moves to 10:20 (`10:40 - 20 minutes`). This closes and evicts all windows ending before 10:20. You will see the new window emitted, and any subsequent event for the 10:00 window will be dropped since that window is now behind the watermark.

## Demo 4: Spark UI
Files: `spark_ui_demo_job.py`

This job is a stateful page-view aggregation — structurally identical to the browser 
stats stateful job, but grouping by `page` instead of `browser`. Its purpose is 
to give you a long-running stateful job to observe in the Spark UI.

1. Start the infrastructure
```bash
cd docker
docker-compose down --volumes && docker-compose up
```

2. Run the job
```bash
uv run spark_ui_demo_job.py
```

The job reads from the `visits` topic, computes 5-minute event-time windows per 
page with a 20-minute watermark, and writes updates to the `page-stats` topic every 
15 seconds.

3. Explore the Spark UI

Open [http://localhost:4040](http://localhost:4040) in a browser. Wait about 5 minutes for enough 
micro-batches to accumulate before exploring the views below.

4. Micro-batch timeline

Navigate to the **Structured Streaming** tab. The micro-batch chart shows 
processing time and input rows per batch. Notice that the first batch takes 
longer and processes significantly more rows than subsequent ones — it behaves like 
a backfill, consuming all events that accumulated in Kafka before the job started. 
Later batches are much more uniform.

5. Micro-batch detail

Click on any batch to see a breakdown of time spent in each internal step. The 
first batch has a higher initialization cost. From batch 1 onward, the bulk of 
time is spent on actual data processing, with the metadata steps being negligible.

6. State store metrics

Scroll down to the state store section. It tracks how many rows are held in state 
at each trigger. Early on, the count grows as new windows are opened. Once the 
watermark advances past older windows, you will see rows dropped — 
the state store shrinks as Spark evicts windows it no longer needs to maintain.

7. Query plan

Open the **SQL / DataFrame** tab and inspect the query plan for the streaming query. 
Unlike a batch plan, it contains several streaming-specific nodes:

- **`MicroBatchScan`** — the streaming source node that reads one bounded chunk of Kafka offsets per trigger.
- **`EventTimeWatermark`** — tracks the maximum observed event time and computes the watermark threshold.
- **`StateStoreRestore`** — loads the existing window aggregates from the state store at the start of each batch.
- **`StateStoreSave`** — persists the updated aggregates back to the state store at the end of each batch.

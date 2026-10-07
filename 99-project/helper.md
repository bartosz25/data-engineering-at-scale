# Useful code snippets

---

## Ingesting Data

### REST API: basic GET request

```python
import requests

def fetch(url, timeout=15):
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()
```

### REST API: ETag-based conditional requests

Avoids re-downloading unchanged data; the server returns `304 Not Modified` when
nothing has changed.

```python
import requests

last_etag = None

def fetch_with_etag(url, headers=None):
    global last_etag
    req_headers = headers or {}
    if last_etag:
        req_headers["If-None-Match"] = last_etag

    r = requests.get(url, headers=req_headers, timeout=15)

    if r.status_code == 304:
        return None          # nothing new

    r.raise_for_status()
    last_etag = r.headers.get("ETag")
    return r.json()
```

### REST API: parallel fetching with ThreadPoolExecutor

Fetching N URLs sequentially takes N × latency. Parallelize when the API allows it.

```python
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

def fetch_one(url):
    r = requests.get(url, timeout=10)
    r.raise_for_status()
    return r.json()

def fetch_all(urls, max_workers=10):
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_one, u): u for u in urls}
        for future in as_completed(future_to_url):
            try:
                results.append(future.result())
            except Exception as e:
                print(f"Error fetching {future_to_url[future]}: {e}")
    return results
```

You can also use `ProcessPoolExecutor`.

### SSE stream (Server-Sent Events)

```python
import requests, json

def stream_sse(url):
    with requests.get(url, stream=True, timeout=30) as r:
        for line in r.iter_lines():
            if line.startswith(b"data: "):
                yield json.loads(line[6:])

for event in stream_sse("https://example.com/stream"):
    print(event)
```

Or with `sseclient-py` (`pip install sseclient-py`):

```python
import sseclient, requests, json

r = requests.get("https://example.com/stream", stream=True)
for event in sseclient.SSEClient(r).events():
    if event.data:
        data = json.loads(event.data)
```

### WebSocket stream

```python
import websocket, json

def on_message(ws, message):
    data = json.loads(message)
    print(data)

ws = websocket.WebSocketApp(
    "wss://example.com/ws/stream",
    on_message=on_message,
)
ws.run_forever()
```

### Polling loop — write JSONL to landing directory

All projects use the same landing pattern: one file per poll, one JSON object per line.

```python
import os, time, json

LANDING_DIR = "/tmp/data-engineering-at-scale/99-project/<project>/landing"
os.makedirs(LANDING_DIR, exist_ok=True)

while True:
    polled_at = int(time.time())
    records = fetch_and_transform()          # returns list[dict]

    if records:
        path = f"{LANDING_DIR}/poll_{polled_at}.json"
        with open(path, "w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        print(f"Poll {polled_at}: {len(records)} records written")

    time.sleep(60)
```

### In-memory deduplication (polling)

Avoid writing the same record twice across polls by tracking seen IDs.

```python
seen_ids = set()

while True:
    records = fetch_all_records()
    new_records = [r for r in records if r["id"] not in seen_ids]
    seen_ids.update(r["id"] for r in records)
    # write new_records ...
```

---

## Spark + Delta Lake

### SparkSession with Delta Lake

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .appName("my-project")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
```

### Reading JSONL files

Always define the schema explicitly. Schema inference scans all files before
reading any data (slow, expensive) and can silently produce wrong types when
early files are missing a field or contain nulls.

When in doubt about a field's type, declare it as `StringType`. Spark will
never discard a value that doesn't fit a string, so no data is lost. Cast to
the correct numeric or timestamp type explicitly in the next transformation
step. The only exception is nested JSON objects those must be declared as
`StructType`, there is no string fallback for them.

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, BooleanType, TimestampType
)

schema = StructType([
    StructField("id",         StringType(),  nullable=False),
    StructField("value",      DoubleType(),  nullable=True),
    StructField("count",      LongType(),    nullable=True),
    StructField("flag",       BooleanType(), nullable=True),
    StructField("created_at", StringType(),  nullable=True),  # parse later with to_timestamp()
])

df = spark.read.schema(schema).json("/tmp/.../landing/*.json")
df.printSchema()
df.show(5, truncate=False)
```

> Nest structs with `StructType` inside a `StructField` for nested JSON objects:
> ```python
> StructField("location", StructType([
>     StructField("lat", DoubleType(), nullable=True),
>     StructField("lon", DoubleType(), nullable=True),
> ]), nullable=True)
> ```

### Writing to a Delta Lake table

```python
df.write.format("delta").mode("append").save("/tmp/.../delta/raw/")
```

Overwrite instead of append:

```python
df.write.format("delta").mode("overwrite").save("/tmp/.../delta/curated/")
```

### Reading from a Delta Lake table

```python
df = spark.read.format("delta").load("/tmp/.../delta/raw/")
```

### Adding an ingestion timestamp

```python
from pyspark.sql import functions as F

df = df.withColumn("ingested_at", F.current_timestamp())
```

### Timestamp conversions

```python
from pyspark.sql import functions as F

# ISO 8601 string  →  TimestampType
df = df.withColumn("event_time", F.to_timestamp(F.col("created_at")))

# Unix epoch in seconds (integer)  →  TimestampType
df = df.withColumn("event_time", F.to_timestamp(F.col("ts_seconds")))

# Unix epoch in milliseconds (integer)  →  TimestampType
df = df.withColumn("event_time", (F.col("ts_millis") / 1000).cast("timestamp"))

# Custom format string
df = df.withColumn(
    "event_time",
    F.to_timestamp(F.col("ts_str"), "yyyy-MM-dd HH:mm:ss")
)
```

### Accessing nested struct fields

Spark infers nested JSON objects as structs. Use dot notation:

```python
from pyspark.sql import functions as F

F.col("meta.dt")
F.col("length.new")
F.col("sensor.sensor_type.name")
```

### Casting column types

```python
from pyspark.sql.types import DoubleType, IntegerType, LongType

df = df.withColumn("price",    F.col("price_str").cast(DoubleType()))
df = df.withColumn("quantity", F.col("qty_str").cast(DoubleType()))
df = df.withColumn("latitude", F.col("lat").cast(DoubleType()))
```

---

## Data Quality

### Common quality checks with PySpark API

_They also work with SQL expressions_

```python
# Null or empty string
F.col("field").isNull() | (F.col("field") == "")

# Regex validation (e.g. 6 hex chars)
~F.col("icao24").rlike("^[0-9a-f]{6}$")

# Numeric bounds
F.col("value") < 0
F.col("value") > 3600

# Allowed-values list (enum check)
~F.col("mode").isin(["tube", "dlr", "overground"])

# Cross-field consistency
F.col("pm25") > F.col("pm10")
(F.col("bikes") + F.col("docks")) > F.col("capacity")

# Staleness: unix-integer timestamps, difference in seconds
(F.col("polled_at") - F.col("last_contact")) > 120

# Future timestamps
F.col("event_time") > F.current_timestamp() + F.expr("INTERVAL 5 SECONDS")
```

---

## Aggregations & Analytics

### Tumbling time window

```python
from pyspark.sql import functions as F

windowed = df.groupBy(
    F.col("key_column"),
    F.window(F.col("event_time"), "1 hour")
).agg(
    F.count("*").alias("count"),
    F.avg("value").alias("avg_value"),
    F.sum("value").alias("total_value"),
)

# Flatten the window struct
windowed = (
    windowed
    .withColumn("window_start", F.col("window.start"))
    .withColumn("window_end",   F.col("window.end"))
    .drop("window")
)
```

### Window function: lead / lag (e.g. headway between events)

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("station_id", "line_id").orderBy("time_to_arrival")

df = df.withColumn("next_arrival", F.lead("time_to_arrival").over(w))
df = df.withColumn("headway", F.col("next_arrival") - F.col("time_to_arrival"))
```

### Window function: rank (e.g. top N per group)

```python
from pyspark.sql.window import Window

counts = df.groupBy("category", "item").agg(F.count("*").alias("cnt"))

w = Window.partitionBy("category").orderBy(F.desc("cnt"))
top5 = (
    counts
    .withColumn("rank", F.rank().over(w))
    .filter(F.col("rank") <= 5)
    .drop("rank")
)
```

### Finding the mode (most frequent value) per group

Spark has no built-in `mode()`. Use rank over a count:

```python
counts = df.groupBy("group_col", "value_col").agg(F.count("*").alias("cnt"))

w = Window.partitionBy("group_col").orderBy(F.desc("cnt"))
mode_df = (
    counts
    .withColumn("rank", F.rank().over(w))
    .filter(F.col("rank") == 1)
    .select("group_col", F.col("value_col").alias("mode_value"))
)
```

### Rolling average over the last N rows

```python
from pyspark.sql.window import Window

w = Window.partitionBy("key").orderBy("window_start").rowsBetween(-6, -1)
df = df.withColumn("rolling_avg", F.avg("metric").over(w))
```

### Median / percentile (Spark has no built-in median)

```python
medians = df.groupBy("symbol").agg(
    F.percentile_approx("price", 0.5).alias("median_price")
)
```

### Conditional aggregation

```python
F.sum(F.when(F.col("flag") == True, 1).otherwise(0)).alias("flag_count")
F.avg(F.when(F.col("on_ground") == False, F.col("altitude"))).alias("avg_airborne_alt")
```

### Explode array column, then aggregate

```python
from pyspark.sql.functions import explode

exploded = df.select("id", "event_time", explode("tags").alias("tag"))

hourly = exploded.groupBy("tag", F.window("event_time", "1 hour")).agg(
    F.count("*").alias("mention_count")
)
```
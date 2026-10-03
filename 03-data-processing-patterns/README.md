# Data Processing Patterns

Demos for the common data processing design patterns in distributed data
engineering, implemented with PySpark and Delta Lake.

---

## Setup

```bash
uv sync
```

---

## Patterns

### 1. Full Loader — `full_load.py`

Transfers **all data** from source to target on every run. 
The target is completely replaced.

```bash
uv run full_load.py
```

```
Run 1  →  [order 1, order 2, order 3]  →  overwrite  →  [1, 2, 3]
Run 2  →  [order 1, order 2]           →  overwrite  →  [1, 2] 
```
> [!NOTE]
> Hard deletes at the source propagate automatically. The order 3 from our example is deleted.

> [!NOTE]
> In case of any mistake you can restore the previous version with Delta Lake's `RESTORE TABLE` command


---

### 2. Incremental Loader — `incremental_load.py`

Transfers **only new records** since the last run. Each batch is appended to the target.

```bash
uv run incremental_load.py
```

```
Batch 1  →  [order 1, order 2, order 3]  →  append  →  [1, 2, 3]
Batch 2  →  [order 4, order 5]           →  append  →  [1, 2, 3, 4, 5]
```

> [!NOTE]
> Cheap to run. The cost is proportional to the increment, not the full dataset.


> [!WARNING]
> Hard deletes are not supported. You only get new observations.


---

### 3. Change Data Capture — `cdc.py`

The source system logs every row-level change (insert / update / delete). 
The pipeline consumes that log and applies each operation to the target.


```bash
uv run cdc.py
```


> [!NOTE]
> Hard deletes and in-place updates are both propagated; 
> the before/after values are part of the event payload at no extra cost.

---

### 4. Horizontal Partitioner — `horizontal_partitioner.py`

Organises data into sub-directories by a column value, which is the `order_date` in our example. 
The engine skips irrelevant directories at read time (aka **partition pruning**).

```bash
uv run horizontal_partitioner.py
```

```
orders_by_date/
  order_date=2024-01-01/   ← only this directory is read when filtering on Jan 1
  order_date=2024-01-02/
  order_date=2024-01-03/
```

> [!NOTE]
> Incremental processing is a frequent use case as each partition is a natural, isolated increment.


The execution plan should show a `PartitionFilters`. They mean only one partition being targeted by the read:
```

== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=false
+- HashAggregate(keys=[order_date#548], functions=[sum(amount#549)], output=[order_date#548, total#546])
   +- Exchange hashpartitioning(order_date#548, 200), ENSURE_REQUIREMENTS, [plan_id=236]
      +- HashAggregate(keys=[order_date#548], functions=[partial_sum(amount#549)], output=[order_date#548, sum#1382])
         +- FileScan parquet spark_catalog.default.orders_by_date[amount#549,order_date#548] Batched: true, DataFilters: [], Format: Parquet, Location: PreparedDeltaFileIndex(1 paths)[file:/Users/bartosz/workspace/epit/data-engineering-at-scale/03-d..., PartitionFilters: [isnotnull(order_date#548), (order_date#548 = 2024-01-01)], PushedFilters: [], ReadSchema: struct<amount:double>
```

---

### 5. Vertical Partitioner — `vertical_partitioner.py`

Splits a record **by columns** across different storage locations. 
PII columns go to a restricted table; non-PII columns go to the general data lake.

```bash
uv run vertical_partitioner.py
```

```
Source row: [order_id, customer_name, email, amount, status]
                            ↓
  orders_public  →  [order_id, amount, status]          (open access)
  orders_pii     →  [order_id, customer_name, email]    (restricted access)
```


---

### 6. Aligned Fan-in — `aligned_fan_in.py`

Combines multiple data sources with compatible schemas into a single DataFrame before any 
processing begins.

```bash
uv run aligned_fan_in.py
```

```
orders_eu  ─┐
             ├─ union ─→ all_orders  →  orders_global
orders_us  ─┘
```


---

### 7. Parallel Split — `parallel_split.py`

Applies shared business logic once, then forks the result into multiple outputs with
different write strategies.

```bash
uv run parallel_split.py
```

```
raw_orders
    → filter(status != cancelled)   ← shared logic, computed once (cached)
        ├─ dropDuplicates  →  orders_latest_per_customer   (key/value store)
        └─ partitionBy     →  orders_analytics             (time-partitioned table)
```

> [!NOTE]
> The source is read only once; each output branch applies its own write strategy independently.



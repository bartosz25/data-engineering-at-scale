# 01 — Introduction to Distributed Data Processing

This module demonstrates three approaches to data processing:

| Approach | Description |
|---|---|
| Single-threaded | One process, one file at a time |
| Multi-threaded | One process, multiple threads sharing the work |
| Locally distributed | Three independent processes, each owning one file |

---

## Prerequisites

- Python 3.14+
- [uv](https://docs.astral.sh/uv/) package manager

---

## Setup

Install dependencies into a local virtual environment:

```bash
uv sync
```

This reads `pyproject.toml` and creates `.venv/` with all required packages.

---

## Generate data

The generator creates three CSV files under `data/`, one per region (300 000 rows total).

```bash
uv run python generate_data.py --rows N --num-files N --output-dir DIR
```

| Flag | Description |
|---|---|
| `--rows N` | Number of rows written to each file |
| `--num-files N` | Number of CSV files to generate |
| `--output-dir DIR` | Directory where CSV files will be written |

Examples:

```bash
# small dataset for quick testing
uv run python generate_data.py --rows 1000 --num-files 3 --output-dir data

# large dataset
uv run python generate_data.py --rows 1000000 --num-files 3 --output-dir data
```

---

## Approach 1 — Single-threaded

`process_single_threaded.py` reads every CSV file in a directory sequentially,
one after the other, in a single process. This is the baseline against which
the multi-threaded and distributed variants are measured.

```bash
uv run python process_single_threaded.py --input-dir DIR
```

---

## Approach 2 — Multi-threaded

`process_multi_threaded.py` processes all CSV files concurrently using a thread
pool. The per-file logic is identical to the single-threaded version — only the
orchestration changes.

```bash
uv run python process_multi_threaded.py --input-dir DIR --workers N
```

## Approach 3 — Locally distributed

`orchestrate.sh` discovers all CSV files in a directory and launches one
independent `process_single_threaded.py` process per file. All processes run in
parallel and are managed by the shell. This simulates what a real distributed
system does — independent workers, each owning a slice of the data.

```bash
./orchestrate.sh <input-dir>
```


---
# Batch vs. streaming demos
## Batch
1. Run the batch processing code:
```shell
uv run python batch_streaming/batch_processing.py
```

You should see 0 files processed.

2. Create a few files in the demo dir:
```shell
echo "a\nb\nc" >> /tmp/data-engineering-at-scale/01-introduction-distributed-data-processing/batch_streaming/batch/1.txt
echo "d\ne\nf" >> /tmp/data-engineering-at-scale/01-introduction-distributed-data-processing/batch_streaming/batch/2.txt
```

3. Run the batch processing code again:
```shell
uv run python batch_streaming/batch_processing.py
```

You should see 2 files processed.

*If you want to run the processing again, you need an explicit action. Of course, you can automate
the process with CRON jobs or more advanced data orchestrators, anyway, each invocation will process
a bunch of existing files*

## Streaming
1. Run the batch processing code:
```shell
uv run python batch_streaming/stream_processing.py
```

The code starts a continuous streaming reader that watches for new files created in the 
streaming directory.

2. Create some files first: 
```shell
echo "1\n2\n3" >> /tmp/data-engineering-at-scale/01-introduction-distributed-data-processing/batch_streaming/streaming/1.txt
echo "4\n5\n6" >> /tmp/data-engineering-at-scale/01-introduction-distributed-data-processing/batch_streaming/streaming/2.txt
```

After creating those files you should see the reader picking them almost instantaneously. And moreover, it reads 
only new changes every time.

3. Clean up:
```shell
rm -rf /tmp/data-engineering-at-scale/01-introduction-distributed-data-processing/batch_streaming/batch
rm -rf /tmp/data-engineering-at-scale/01-introduction-distributed-data-processing/batch_streaming/streaming
```

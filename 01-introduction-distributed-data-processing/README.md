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

| Flag | Description |
|---|---|
| `--input-dir DIR` | Directory containing the CSV files to process |

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
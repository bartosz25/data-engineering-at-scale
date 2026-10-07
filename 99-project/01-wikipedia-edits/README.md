# Project 01: Wikipedia Edit Stream

## Overview

Wikipedia is edited thousands of times per hour across hundreds of language editions.
The Wikimedia Foundation publishes every change as a real-time Server-Sent Events (SSE)
stream.

Your pipeline ingests this stream, validates the data, and builds an **edit quality
monitor** that tracks bot vs. human activity per wiki.

## Data Source

| Property | Value |
|----------|-------|
| URL | `https://stream.wikimedia.org/v2/stream/recentchange` |
| Protocol | Server-Sent Events (SSE) |
| Auth | None |
| Rate | ~20–60 events/second |
| Format | JSON per event |

**Quick test in your terminal:**
```bash
curl -s https://stream.wikimedia.org/v2/stream/recentchange | head -60
```

## Key schema fields

```json
{
  "meta": { "id": "abc123", "dt": "2024-01-15T10:30:00Z", "domain": "en.wikipedia.org" },
  "type": "edit",
  "wiki": "enwiki",
  "title": "Albert Einstein",
  "namespace": 0,
  "user": "SomeEditor",
  "bot": false,
  "comment": "added citation",
  "length": { "old": 45231, "new": 45398 },
  "revision": { "old": 1234567, "new": 1234568 },
  "server_name": "en.wikipedia.org"
}
```
---

## Phase A: Data Discovery

Explore the stream before writing any code. Write your answers in `notes.txt`.

1. Open `https://stream.wikimedia.org/v2/stream/recentchange` in your browser.
   Watch for 2 minutes. What happens?

2. Run the curl command above and copy 3 different events into `notes.txt`.

3. Answer these questions:
   - What values appear in the `type` field? (It is not only `edit`.)
   - What does `namespace` mean? Look up namespace 0 vs 4 vs 10.
     Hint: https://www.mediawiki.org/wiki/Manual:Namespace
   - Manually inspect 20 events: what fraction have `bot: true`?
   - Is `length.old` ever null? Under what circumstances?
   - Is `length.new` ever null? Is it ever negative?
   - Are there events where `length.new < length.old`? What does a shrinking page mean?
   - What language wikis appear? (look at `wiki`: `enwiki`, `frwiki`, `dewiki`, …)
   - How often is the `comment` field empty? What could that indicate?

**You are ready for Phase B when you can describe the schema without looking at it.**

---

## Phase B: Producer and Ingestion

### producer.py

Write a Python script that:
- Connects to the Wikimedia SSE stream using the `requests` library
- Collects events in batches of **100 events**
- Writes each batch as a single JSON file (one JSON object per line)
  to `/tmp/data-engineering-at-scale/99-project/01-wikipedia-edits/raw/`
- File names: `batch_<unix_timestamp>.json`
- Prints a progress line after each batch: `Batch 3 written: 100 events`
- Runs until stopped with Ctrl+C

### ingestion_job.py

Write a PySpark batch job that:
- Reads all JSON files from the raw directory
- Adds an `ingested_at` column (current timestamp at job run time)
- Writes to Delta Lake at `/tmp/data-engineering-at-scale/99-project/01-wikipedia-edits/raw-delta`
  using `mode("append")`

Run `producer.py` for **at least 5 minutes** before running this job.

---

## Phase C: Cleansing Job (45 min)

### cleansing_job.py

Read from `raw-delta` and apply the following data quality rules.

### Data quality rules

| # | Field | Rule | rejection_reason |
|---|-------|------|-----------------|
| 1 | `meta.dt` | Not null | `null_timestamp` |
| 2 | `title` | Not null and not empty string | `null_title` |
| 3 | `wiki` | Not null | `null_wiki` |
| 4 | `type` | One of: `edit`, `new`, `log`, `categorize` | `unknown_event_type` |
| 5 | `length.new` | Not negative | `negative_page_length` |
| 6 | `abs(length.new - length.old)` | Not greater than 1,000,000 bytes | `implausible_edit_size` |

**Output:**
- Records failing any rule + `rejection_reason` column → `error-delta`
- Records passing all rules → `valid-delta`
- Print a summary at the end:
  ```
  Total:   12,430 records
  Valid:   12,381
  Invalid:     49
    null_timestamp:       3
    null_title:          12
    unknown_event_type:   8
    implausible_edit_size: 26
  ```

> A record failing multiple rules should be written to `error-delta` only once.
> Choose the first matching rule as the rejection reason.

---

## Phase D: Business Job (90 min)

### business_job.py — Edit Quality Monitor

Read from `valid-delta`.

### Business rules

Compute **per wiki per 10-minute window**:

| Column | Description |
|--------|-------------|
| `wiki` | Wiki identifier (e.g. `enwiki`) |
| `window_start` | Start of the 10-minute window |
| `window_end` | End of the 10-minute window |
| `total_edits` | All events in this window |
| `bot_edits` | Events where `bot = true` |
| `human_edits` | Events where `bot = false` |
| `bot_ratio` | `bot_edits / total_edits` rounded to 4 decimal places |
| `avg_human_edit_delta` | Average `(length.new - length.old)` for human edits only |
| `high_bot_activity` | `true` when `bot_ratio > 0.8` |

Also compute a **top-pages table** — for each wiki and 10-minute window, the top 5 most
edited pages (by edit count):

| Column | Description |
|--------|-------------|
| `wiki` | Wiki identifier |
| `window_start` | Start of window |
| `title` | Page title |
| `edit_count` | Number of edits to this page |
| `rank` | 1 = most edited |

Write both tables to `business-delta`:
- `/tmp/data-engineering-at-scale/99-project/01-wikipedia-edits/business-delta/wiki_10min_stats/`
- `/tmp/data-engineering-at-scale/99-project/01-wikipedia-edits/business-delta/top_pages/`

---

## Sample deliverables

```
01-wikipedia-edits/
├── notes.txt
├── pyproject.toml
└── src/
    └── wikipedia_edits/
        ├── __init__.py
        ├── config.py          # raw/, raw-delta, valid-delta, error-delta, business-delta paths
        ├── producer.py
        ├── ingestion_job.py
        ├── cleansing_job.py
        └── business_job.py
```

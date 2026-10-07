# Project 06: Tech Community Pulse (Hacker News)

## Overview

Hacker News is a technology news and discussion community run by Y Combinator.
It publishes a public Firebase Realtime Database API that pushes live updates
when new items (stories, comments, jobs) are created or modified.

Your pipeline ingests new story submissions, validates them, and builds a
**tech trend tracker** that identifies which technologies and domains are gaining momentum.

## Data Sources

| Feed | URL | Notes |
|------|-----|-------|
| New story IDs (live) | `https://hacker-news.firebaseio.com/v0/newstories.json` | List of newest story IDs |
| Item detail | `https://hacker-news.firebaseio.com/v0/item/{id}.json` | Fetch by ID |
| Live updates | `https://hacker-news.firebaseio.com/v0/updates.json?print=pretty` | Changed item IDs (SSE) |

No authentication required.

**Quick test:**
```bash
# Get latest story IDs
curl -s "https://hacker-news.firebaseio.com/v0/newstories.json" | python3 -m json.tool

# Fetch one item (replace ID with a real one)
curl -s "https://hacker-news.firebaseio.com/v0/item/49967303.json" | python3 -m json.tool
```

## Key schema fields

```json
{
  "id": 39123456,
  "type": "story",
  "by": "some_user",
  "time": 1705312200,
  "title": "Show HN: I built a Rust-based database in a weekend",
  "url": "https://github.com/someuser/mydb",
  "text": null,
  "score": 42,
  "descendants": 17,
  "kids": [39123457, 39123461, 39123470],
  "dead": false,
  "deleted": false
}
```

| Field | Meaning |
|-------|---------|
| `type` | `story`, `comment`, `job`, `poll`, `pollopt` |
| `url` | Link submitted (null for text posts like Ask HN / Show HN) |
| `text` | Body text (for Ask HN posts, null for link posts) |
| `score` | Upvotes received so far |
| `descendants` | Total comment count |
| `dead` | `true` = flagged as spam/low quality |
| `deleted` | `true` = author deleted the post |

---

## Phase A: Data Discovery (30 min)

Fetch stories and explore. Write findings in `notes.txt`.

1. Fetch the newest stories list. How many IDs are returned?

2. Fetch 20 items by ID. Copy 3 interesting ones into `notes.txt`.

3. Answer these questions:
   - What fraction of the newest 50 items are `type = story` vs other types?
   - How many have `dead = true`? What kinds of posts tend to get flagged dead?
   - How many have `deleted = true`? Can you still see the content?
   - For stories with a `url`: extract the domain. What domains appear most frequently?
   - Stories without a `url` have a `text` field instead. What are "Ask HN" and "Show HN"?
   - How quickly do new stories get upvotes? Fetch the same item twice 5 minutes apart.
   - What does a `score` of 1 mean? (All new posts start at…?)
   - Can `by` (author) be null? Under what circumstances?

4. Look at 5 `job` type items. How are they different from stories? Who posts them?

5. Define your list of technology keywords to track in Phase D.
   Write at least 20 keywords in `notes.txt` (e.g. Python, Rust, AI, LLM, Kubernetes, etc.)

**You are ready for Phase B when you understand the difference between link posts and text posts.**

---

## Phase B: Producer and Ingestion

### producer.py

Write a Python script that:
- Every **60 seconds**, fetches `newstories.json` to get the latest story IDs
- Compares against IDs already seen (keep a set in memory) to find truly new IDs
- Fetches the item detail for each new ID (use threading to fetch in parallel — see Module 01)
- Filters to `type in ["story", "job"]` only (skip comments and polls)
- Writes fetched items as JSON to `.../raw/batch_<unix_ts>.json`
- Prints: `Batch 7: 14 new stories fetched`
- Runs continuously

> Note: Use a thread pool (`ThreadPoolExecutor`) to fetch multiple items in parallel.
> Fetching them one-by-one will be too slow.

### ingestion_job.py

Write a PySpark batch job that:
- Reads all JSON files from the landing directory
- Converts `time` (unix seconds) to `TimestampType`
- Extracts domain from `url` column: e.g. `"https://github.com/user/repo"` → `"github.com"`
  (null if `url` is null)
- Adds `ingested_at` column
- Writes to `raw-delta` using `mode("append")`

---

## Phase C: Cleansing Job (45 min)

### cleansing_job.py

Read from `raw-delta` and apply these validation rules:

### Data quality rules

| # | Field | Rule | rejection_reason |
|---|-------|------|-----------------|
| 1 | `id` | Not null | `null_id` |
| 2 | `time` | Not null, not in the future | `invalid_timestamp` |
| 3 | `type` | One of: `story`, `job` | `unexpected_type` |
| 4 | `deleted` | Must be `false` or null | `deleted_item` |
| 5 | `dead` | Must be `false` or null | `dead_item` |
| 6 | `by` | Not null (deleted is already caught above) | `null_author` |
| 7 | `type = story` AND `title` | Title not null, not empty | `story_without_title` |

**Output:**
- Failing records + `rejection_reason` → `error-delta`
- Passing records → `valid-delta`

Note: `deleted` and `dead` items are not data errors — they are legitimate states.
But they contain no useful content for analysis, so we route them to error-delta.

---

## Phase D: Business Analytics Job (90 min)

### business_job.py — Tech Trend Tracker

Read from `valid-delta`.

### Business rules

**Step 1 — Extract signals from title:**

For each story, check whether the title contains each of your tracked keywords
(case-insensitive). Create a column `matched_keywords` as an array of matched keywords.

Also add a `post_type` column:
- `"Ask HN"` — title starts with `"Ask HN:"`
- `"Show HN"` — title starts with `"Show HN:"`
- `"Link"` — has a `url`
- `"Text"` — has text, no url

**Step 2 — Hourly keyword trends:**

For each **keyword** and **1-hour window**, compute:

| Column | Description |
|--------|-------------|
| `keyword` | Technology keyword |
| `window_start` | Start of hour |
| `mention_count` | Stories mentioning this keyword |
| `avg_score` | Average score of stories mentioning this keyword |
| `total_comments` | Sum of `descendants` for these stories |

**Step 3 — Trending score:**

Compare this hour's `mention_count` to the average of the previous 6 hours.
`trending_score = current_hour_count / avg_last_6_hours` (if > 1.0, it is trending up).

Mark keywords as `trending = true` when `trending_score > 2.0`.

**Step 4 — Domain leaderboard:**

For each `domain` and 1-hour window:
- `story_count`, `avg_score`, `total_comments`, `unique_authors`
- Rank by `avg_score` descending

Write to:
- `business-delta/keyword_hourly_trends/`
- `business-delta/domain_leaderboard/`

---

## Sample deliverables

```
07-hacker-news/
├── notes.txt              # includes your keyword list from Phase A
├── pyproject.toml
└── src/
    └── hacker_news/
        ├── __init__.py
        ├── config.py
        ├── producer.py
        ├── ingestion_job.py
        ├── cleansing_job.py
        └── business_job.py
```

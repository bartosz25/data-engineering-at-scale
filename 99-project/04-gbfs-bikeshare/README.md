# Project 04: Bike Share Rebalancing (GBFS / Citi Bike NYC)

## Overview

Citi Bike is New York City's public bike share system with over 1,800 stations.
The GBFS (General Bikeshare Feed Specification) publishes real-time station status
updated approximately every 10 seconds. A separate static feed provides station metadata
(location, capacity, name).

Your pipeline ingests both feeds, validates the data, and builds a
**rebalancing priority dashboard** that helps operations teams decide which stations
need bikes moved to or from them.

## Data Sources

| Feed | URL | Update frequency |
|------|-----|-----------------|
| Station status (real-time) | `https://gbfs.citibikenyc.com/gbfs/en/station_status.json` | ~10 seconds |
| Station information (static) | `https://gbfs.citibikenyc.com/gbfs/en/station_information.json` | Hourly |

Both return JSON. No authentication required.

**Quick test:**
```bash
curl -s "https://gbfs.citibikenyc.com/gbfs/en/station_status.json" | python3 -m json.tool | head -40
```

## Key schema fields

**Station status** (from `data.stations[]`):
```json
{
  "station_id": "66db3b4b-0aca-11e7-82f6-3863bb44ef7c",
  "num_bikes_available": 12,
  "num_ebikes_available": 4,
  "num_bikes_disabled": 1,
  "num_docks_available": 8,
  "num_docks_disabled": 0,
  "is_installed": 1,
  "is_renting": 1,
  "is_returning": 1,
  "last_reported": 1705312180
}
```

**Station information** (from `data.stations[]`):
```json
{
  "station_id": "66db3b4b-0aca-11e7-82f6-3863bb44ef7c",
  "name": "W 21 St & 6 Ave",
  "lat": 40.741441,
  "lon": -74.000876,
  "capacity": 21,
  "region_id": "71"
}
```

---

## Phase A: Data Discovery  

Fetch both endpoints and explore. Write findings in `notes.txt`.

1. Fetch the station status endpoint. How many stations are in the response?

2. Fetch both endpoints and join them on `station_id`.
   - Are there `station_id` values in status that are NOT in information? How many?
   - Are there `station_id` values in information that are NOT in status? What does that mean?

3. Answer these questions:
   - Does `num_bikes_available + num_ebikes_available + num_docks_available + num_bikes_disabled + num_docks_disabled`
     always equal `capacity`? Compute this check manually for 5 stations.
   - What does `is_installed = 0` mean? Are there stations with bikes but `is_renting = 0`?
   - What fraction of stations have `num_bikes_available = 0`? (completely empty)
   - What fraction have `num_docks_available = 0`? (completely full)
   - How stale are `last_reported` timestamps? Find the oldest one in the current response.
   - Can `num_ebikes_available > num_bikes_available`? Why would that be impossible?
   - What are `region_id` values? How many distinct regions are there?

4. Poll the status endpoint 3 times with 30s between each. Which stations changed?

**You are ready for Phase B when you understand the relationship between status and information.**

---

## Phase B: Producer and Ingestion

### producer.py

Write a Python script that:
- On startup, fetches station information once and saves to `.../raw/station_info.json`
- Every **10 seconds**, polls station status and writes to `.../raw/status/`
  as `status_<unix_timestamp>.json`
- Each status file contains the full list of station statuses plus a `polled_at` field
- Runs for at least **15 minutes**

### ingestion_job.py

Write a PySpark batch job that:
- Reads all status JSON files from `.../raw/status/`
- Reads the station information file `.../raw/station_info.json`
- Joins the two DataFrames on `station_id` to enrich status records with
  `name`, `lat`, `lon`, `capacity`, `region_id`
- Converts `last_reported` to `TimestampType`
- Adds `ingested_at` column
- Writes the enriched records to `raw-delta` using `mode("append")`

> Hint: read the info file as a broadcast variable or a small DataFrame and join.

---

## Phase C: Cleansing Job (45 min)

### cleansing_job.py

Read from `raw-delta` and apply these validation rules:

### Data quality rules

| # | Field | Rule | rejection_reason |
|---|-------|------|-----------------|
| 1 | `station_id` | Not null | `null_station_id` |
| 2 | `capacity` | Greater than 0 (station info must have been joined) | `missing_station_info` |
| 3 | `num_bikes_available` | Not negative | `negative_bike_count` |
| 4 | `num_docks_available` | Not negative | `negative_dock_count` |
| 5 | `num_ebikes_available` | Not greater than `num_bikes_available` | `ebikes_exceed_bikes` |
| 6 | `num_bikes_available + num_docks_available` | Not greater than `capacity` | `counts_exceed_capacity` |
| 7 | `is_installed = 0` AND `num_bikes_available > 0` | Uninstalled station reporting bikes | `bikes_at_uninstalled_station` |
| 8 | `last_reported` | Within 1 hour of `polled_at` | `stale_station_data` |

**Output:**
- Failing records + `rejection_reason` → `error-delta`
- Passing records → `valid-delta`
- Print a summary.

---

## Phase D: Business Analytics Job (90 min)

### business_job.py — Rebalancing Priority Dashboard

Read from `valid-delta` and compute rebalancing signals.

### Business rules

First, compute a **fill rate** for each observation:
- `fill_rate = num_bikes_available / capacity` (0.0 = empty, 1.0 = full)

For each **station** and **30-minute window**, compute:

| Column | Description |
|--------|-------------|
| `station_id` | Station identifier |
| `name` | Station name |
| `lat`, `lon` | Location |
| `region_id` | Region |
| `window_start` | Start of 30-minute window |
| `avg_fill_rate` | Mean fill rate across observations in window |
| `min_fill_rate` | Minimum fill rate (worst emptiness) |
| `max_fill_rate` | Maximum fill rate (worst fullness) |
| `times_empty` | Observations where `num_bikes_available = 0` |
| `times_full` | Observations where `num_docks_available = 0` |
| `rebalancing_priority` | See below |

**Rebalancing priority logic:**
- `"NEEDS_BIKES"` — `avg_fill_rate < 0.2` (chronically empty)
- `"NEEDS_DOCKS"` — `avg_fill_rate > 0.8` (chronically full)
- `"MONITOR"` — `times_empty > 3` OR `times_full > 3` (unstable)
- `"OK"` — everything else

Write to:
- `business-delta/station_rebalancing/`

Also produce a regional summary: for each `region_id`, count stations by
`rebalancing_priority` category.

Write to:
- `business-delta/region_summary/`

---

## Sample deliverables

```
04-gbfs-bikeshare/
├── notes.txt
├── pyproject.toml
└── src/
    └── gbfs_bikeshare/
        ├── __init__.py
        ├── config.py
        ├── producer.py
        ├── ingestion_job.py
        ├── cleansing_job.py
        └── business_job.py
```

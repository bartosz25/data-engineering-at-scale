# Project 03: Live Flight Tracking (OpenSky Network)

## Overview

The OpenSky Network aggregates ADS-B signals from thousands of receivers worldwide
to track every aircraft in the sky in real time. The REST API returns the position
and state of all tracked flights, updated every 10 seconds.

Your pipeline polls this API, validates the data, and builds an
**airspace density monitor** — a spatial analysis of where aircraft are concentrated.

## Data Source

| Property | Value |
|----------|-------|
| URL | `https://opensky-network.org/api/states/all` |
| Protocol | REST (GET, JSON response) |
| Auth | None for anonymous (rate-limited), free account for higher limits |
| Poll interval | Every 10 seconds (API updates at this rate) |
| Format | JSON with a `states` array |

**Quick test:**
```bash
curl -s "https://opensky-network.org/api/states/all" | python3 -m json.tool | head -60
```

## Key schema fields

The response has `time` (unix) and `states` — a list of arrays:

```
states[n] = [
  icao24,           # [0]  string  — unique aircraft identifier (6 hex chars)
  callsign,         # [1]  string  — flight number (may be null or padded with spaces)
  origin_country,   # [2]  string
  time_position,    # [3]  int     — unix timestamp of last position update
  last_contact,     # [4]  int     — unix timestamp of last message received
  longitude,        # [5]  float   — WGS-84 longitude
  latitude,         # [6]  float   — WGS-84 latitude
  baro_altitude,    # [7]  float   — barometric altitude in metres (can be null)
  on_ground,        # [8]  bool
  velocity,         # [9]  float   — m/s
  true_track,       # [10] float   — heading in degrees (0=North, clockwise)
  vertical_rate,    # [11] float   — m/s (positive = climbing)
  sensors,          # [12] list    — IDs of receivers that observed this aircraft
  geo_altitude,     # [13] float   — geometric altitude in metres (GPS)
  squawk,           # [14] string  — transponder code
  spi,              # [15] bool    — special purpose indicator
  position_source   # [16] int     — 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
]
```

---

## Phase A: Data Discovery

Fetch the API several times and explore. Write findings in `notes.txt`.

1. Run the curl command above. How many aircraft are currently being tracked?

2. Fetch the API 3 times with 15 seconds between each call.
   - Which fields change between polls for the same `icao24`?
   - Which fields stay the same?

3. Answer these questions:
   - What fraction of entries have `null` latitude/longitude? When does this happen?
   - What fraction of entries have `on_ground = true`? Do they have altitude?
   - Are there entries with `baro_altitude < 0`? Is that always an error?
     (Hint: Dead Sea is at -430m. An aircraft on the ground there would show negative altitude.)
   - Callsign values often have trailing spaces (e.g. `"DLH440  "`). Why?
   - What `position_source` values appear? What is the most common one?
   - Are there duplicate `icao24` values in a single response? Why might that happen?
   - What is the distribution of `velocity`? What is the minimum non-zero value?

4. Pick an interesting `icao24` value and track it across 5 polls. Describe its movement.

**You are ready for Phase B when you understand what each array index represents.**

---

## Phase B: Producer and Ingestion

### producer.py

Write a Python script that:
- Polls `https://opensky-network.org/api/states/all` every **10 seconds**
- For each poll, converts each state array into a JSON object with named fields
  (i.e., map index 0 → `icao24`, index 1 → `callsign`, etc.)
- Adds a `polled_at` field (current unix timestamp) to each record
- Writes all records from each poll as a single JSON file to `.../raw/`
- File names: `poll_<unix_timestamp>.json`
- Runs for at least **10 minutes** before you stop it

### ingestion_job.py

Write a PySpark batch job that:
- Reads all JSON files from the landing directory
- Strips trailing/leading whitespace from `callsign`
- Converts `time_position` and `last_contact` to `TimestampType`
- Adds `ingested_at` column
- Writes to `raw-delta` using `mode("append")`

---

## Phase C: Cleansing Job (45 min)

### cleansing_job.py

Read from `raw-delta` and apply these validation rules:

### Data quality rules

| # | Field | Rule | rejection_reason |
|---|-------|------|-----------------|
| 1 | `icao24` | Not null, exactly 6 hex characters `[0-9a-f]{6}` | `invalid_icao24` |
| 2 | `longitude` | Between -180 and 180 | `invalid_coordinates` |
| 3 | `latitude` | Between -90 and 90 | `invalid_coordinates` |
| 4 | `baro_altitude` | Greater than -500 metres | `implausible_altitude` |
| 5 | `velocity` | Not negative | `negative_velocity` |
| 6 | `last_contact` | Within 120 seconds of `polled_at` | `stale_position` |
| 7 | `on_ground = false` AND `baro_altitude` is null | Airborne but no altitude | `airborne_no_altitude` |

**Output:**
- Failing records + `rejection_reason` → `error-delta`
- Passing records → `valid-delta`
- Print a summary.

---

## Phase D: Business Analytics Job (90 min)

### business_job.py — Airspace Density Monitor

Read from `valid-delta` and compute spatial density using a **5° × 5° grid**:

### Business rules

Assign each aircraft to a grid cell:
- `grid_lon = floor(longitude / 5) * 5`  (e.g., longitude 13.4 → grid cell 10)
- `grid_lat = floor(latitude / 5) * 5`

For each **grid cell** and **10-minute poll window**, compute:

| Column | Description |
|--------|-------------|
| `grid_lon` | Grid cell longitude (lower-left corner) |
| `grid_lat` | Grid cell latitude (lower-left corner) |
| `window_start` | Start of 10-minute window |
| `aircraft_count` | Number of distinct `icao24` values |
| `avg_altitude_m` | Mean barometric altitude (airborne only) |
| `avg_velocity_ms` | Mean velocity (airborne only) |
| `on_ground_count` | Aircraft where `on_ground = true` |
| `top_country` | Most frequent `origin_country` in this cell |

Also compute a **country ranking table** — for each `origin_country`, total aircraft
tracked, average altitude, and the grid cell with the highest concentration of that
country's aircraft.

Write to:
- `business-delta/grid_density/`
- `business-delta/country_stats/`

---

## Sample deliverables

```
03-opensky-flights/
├── notes.txt
├── pyproject.toml
└── src/
    └── opensky_flights/
        ├── __init__.py
        ├── config.py
        ├── producer.py
        ├── ingestion_job.py
        ├── cleansing_job.py
        └── business_job.py
```

# Project 05: Air Quality IoT Sensors (Sensor.Community)

## Overview

Sensor.Community (formerly Luftdaten.info) is a citizen science project with over
15,000 low-cost air quality sensors worldwide, all contributing measurements to a
public API. Each sensor reports particulate matter (PM2.5, PM10), and optionally
temperature, humidity, and pressure.

Your pipeline ingests these sensor readings, validates them, and builds a
**global Air Quality Index (AQI) dashboard** using the US EPA formula.

## Data Source

| Property | Value |
|----------|-------|
| URL | `https://data.sensor.community/airrohr/v1/filter/type=SDS011` |
| Protocol | REST (GET, JSON array) |
| Auth | None |
| Poll interval | Every 5 minutes (data refreshes at this rate) |
| Format | JSON array of sensor reading objects |

**Other sensor types:** Replace `SDS011` with `BME280` (temperature/humidity/pressure)
or `DHT22` (temperature/humidity).

**Quick test:**
```bash
curl -s "https://data.sensor.community/airrohr/v1/filter/type=SDS011" \
  | python3 -c "import sys,json; data=json.load(sys.stdin); print(f'{len(data)} sensors'); print(json.dumps(data[0], indent=2))"
```

## Key schema fields

```json
{
  "id": 19872345678,
  "sampling_rate": null,
  "timestamp": "2024-01-15 10:28:30",
  "location": {
    "id": 12345,
    "latitude": "48.1234",
    "longitude": "11.5678",
    "altitude": "520.0",
    "country": "DE",
    "indoor": 0,
    "exact_location": 0
  },
  "sensor": {
    "id": 67890,
    "pin": "1",
    "sensor_type": { "id": 14, "name": "SDS011", "manufacturer": "Nova Fitness" }
  },
  "sensordatavalues": [
    { "id": 11111, "value": "12.47", "value_type": "P1" },
    { "id": 11112, "value": "8.93",  "value_type": "P2" }
  ]
}
```

| Field | Meaning |
|-------|---------|
| `P1` | PM10 — particulate matter ≤10 µm (µg/m³) |
| `P2` | PM2.5 — particulate matter ≤2.5 µm (µg/m³) |
| `location.indoor` | 1 = indoor sensor (affects AQI interpretation) |
| `location.exact_location` | 0 = location fuzzy (privacy), 1 = exact |

---

## Phase A: Data Discovery

Fetch the endpoint and explore. Write findings in `notes.txt`.

1. How many active SDS011 sensors are there worldwide right now?

2. The `sensordatavalues` is a list of `{value_type, value}` pairs.
   What `value_type` values appear for SDS011 sensors?
   Do all sensors report both P1 and P2?

3. Answer these questions:
   - Values are strings (e.g. `"12.47"`), not numbers. What happens if you cast `"999999.00"` to float?
   - What fraction of sensors have `latitude = "0.000000"` AND `longitude = "0.000000"`?
     This is called the "Null Island" problem. What causes it?
   - Some P2 values are very high (> 500 µg/m³). Are these sensor errors or real pollution events?
     Look up the WHO 24-hour PM2.5 guideline for context.
   - What fraction of sensors are `indoor = 1`? How should indoor sensors be treated differently?
   - Fetch the BME280 type as well. What `value_type` fields does it report?
   - Some sensors report `value = ""` (empty string). How would you handle this?

4. Look up the US EPA AQI breakpoints for PM2.5:
   https://www.epa.gov/outdoor-air-quality-data/air-quality-index-aqi-basics
   Write down the breakpoint table in your `notes.txt` — you will need it in Phase D.

**You are ready for Phase B when you understand how to pivot `sensordatavalues` into columns.**

---

## Phase B: Producer and Ingestion

### producer.py

Write a Python script that:
- Polls `https://data.sensor.community/airrohr/v1/filter/type=SDS011` every **5 minutes**
- For each sensor reading, **pivots** `sensordatavalues` into flat fields:
  `pm10`, `pm25` (extracted from P1 and P2 value_type respectively)
- Keeps: `reading_id`, `sensor_id`, `sensor_type`, `timestamp`, `latitude`, `longitude`,
  `country`, `indoor`, `pm10`, `pm25`, `polled_at`
- Casts all numeric strings to float during extraction (use `None` if conversion fails)
- Writes records as JSON lines to `.../raw/poll_<unix_ts>.json`

### ingestion_job.py

Write a PySpark batch job that:
- Reads all JSON files from the landing directory
- Converts `timestamp` string to `TimestampType` (format: `"yyyy-MM-dd HH:mm:ss"`)
- Casts `latitude` and `longitude` to `DoubleType`
- Adds `ingested_at` column
- Writes to `raw-delta` using `mode("append")`

---

## Phase C: Cleansing Job (45 min)

### cleansing_job.py

Read from `raw-delta` and apply these validation rules:

### Data quality rules

| # | Field | Rule | rejection_reason |
|---|-------|------|-----------------|
| 1 | `sensor_id` | Not null | `null_sensor_id` |
| 2 | `latitude` AND `longitude` | Not both equal to 0.0 | `null_island_coordinates` |
| 3 | `latitude` | Between -90 and 90 | `invalid_coordinates` |
| 4 | `longitude` | Between -180 and 180 | `invalid_coordinates` |
| 5 | `pm25` | Not null, not negative, and ≤ 1000 µg/m³ | `invalid_pm25` |
| 6 | `pm10` | Not null, not negative, and ≤ 1500 µg/m³ | `invalid_pm10` |
| 7 | `pm25 > pm10` | PM2.5 cannot exceed PM10 by definition | `pm25_exceeds_pm10` |
| 8 | `timestamp` | Not null, not in the future | `invalid_timestamp` |

**Note:** keep `indoor = 1` sensors as valid — just flag them.
Add an `is_indoor` boolean column to `valid-delta`.

**Output:**
- Failing records + `rejection_reason` → `error-delta`
- Passing records → `valid-delta`
- Print a summary including the indoor vs outdoor split.

---

## Phase D: Business Analytics Job (90 min)

### business_job.py — Global AQI Dashboard

Read from `valid-delta` (outdoor sensors only: `is_indoor = false`).

### Business rules

**Step 1 — Compute AQI from PM2.5:**
Use the US EPA linear interpolation formula:

```
AQI = ((AQI_high - AQI_low) / (conc_high - conc_low)) * (pm25 - conc_low) + AQI_low
```

PM2.5 breakpoints (24-hour average, µg/m³):

| AQI range  | PM2.5 range     | Category |
|------------|-----------------|----------|
| 0–50       | 0.0–12.0        | Good |
| 51–100     | 12.1–35.4       | Moderate |
| 101–150    | 35.5–55.4       | Unhealthy for Sensitive Groups |
| 151–200    | 55.5–150.4      | Unhealthy |
| 201–300    | 150.5–250.4     | Very Unhealthy |
| 301–500    | 250.5–500.4     | Hazardous |

Add `aqi` and `aqi_category` columns.

**Step 2 — Country-level hourly summary:**

For each `country` and **1-hour window**, compute:

| Column | Description |
|--------|-------------|
| `country` | ISO country code |
| `window_start` | Start of 1-hour window |
| `active_sensors` | Number of distinct sensors reporting |
| `avg_pm25` | Mean PM2.5 across all sensors |
| `avg_aqi` | Mean AQI |
| `max_aqi` | Peak AQI in the country |
| `dominant_category` | Most frequent AQI category |
| `pct_unhealthy` | Percentage of sensors in Unhealthy or worse |
| `alert` | `true` when `pct_unhealthy > 30%` |

Write to:
- `business-delta/country_aqi_hourly/`

Also compute a simple worst-performing sensor table (top 20 sensors by average AQI):
- `business-delta/worst_sensors/`

---

## Sample deliverables

```
06-sensor-community/
├── notes.txt
├── pyproject.toml
└── src/
    └── sensor_community/
        ├── __init__.py
        ├── config.py
        ├── producer.py
        ├── ingestion_job.py
        ├── cleansing_job.py
        └── business_job.py
```

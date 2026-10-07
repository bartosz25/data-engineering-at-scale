# Project 02: Crypto Trade Stream (Binance)

## Overview

Binance is one of the world's largest cryptocurrency exchanges. It publishes a
public WebSocket stream of every executed trade — no authentication required.

Your pipeline ingests this stream, validates each trade record, and builds a
**VWAP (Volume Weighted Average Price) dashboard** — a standard metric used by
traders to assess whether they bought/sold at a good price relative to the market.

## Data Source

| Property | Value |
|----------|-------|
| URL | `wss://stream.binance.com:9443/ws/btcusdt@trade` |
| Protocol | WebSocket |
| Auth | None |
| Rate | ~5–20 trades/second for BTCUSDT |
| Format | JSON per message |

**Other symbols to explore:** replace `btcusdt` with `ethusdt`, `solusdt`, `bnbusdt`.

**Quick test — paste in Python:**
```python
import websocket, json
ws = websocket.create_connection("wss://stream.binance.com:9443/ws/btcusdt@trade")
print(json.loads(ws.recv()))
ws.close()
```

## Key schema fields

```json
{
  "e": "trade",
  "E": 1705312200123,
  "s": "BTCUSDT",
  "t": 3456789012,
  "p": "43215.67",
  "q": "0.00231",
  "b": 987654321,
  "a": 987654322,
  "T": 1705312200120,
  "m": false,
  "M": true
}
```

| Field | Meaning |
|-------|---------|
| `e` | Event type (always `trade`) |
| `E` | Event timestamp (milliseconds) |
| `s` | Symbol (e.g. `BTCUSDT`) |
| `t` | Trade ID |
| `p` | Price (string, needs cast to decimal) |
| `q` | Quantity traded (string) |
| `T` | Trade execution timestamp (milliseconds) |
| `m` | `true` = buyer is market maker (seller initiated) |

---

## Phase A: Data Discovery

Connect to the stream and observe before writing any code. Write findings in `notes.txt`.

1. Run the quick test above. Copy one raw event into `notes.txt`.

2. Connect and receive 200 events. Answer these questions:
   - What is the approximate trade rate (trades/second)?
   - `p` (price) and `q` (quantity) are strings, not numbers. Why might the exchange do this?
   - What is the price range you observed over 5 minutes? How volatile is it?
   - What does `m: true` mean vs `m: false`? Think about market makers and takers.
   - Are trade IDs (`t`) sequential? Try subscribing to 2 symbols simultaneously.
   - Are there duplicate trade IDs? How would you detect them?
   - Try subscribing to `btcusdt@aggTrade` instead of `btcusdt@trade`. What is different?

3. Estimate: if you store every BTC trade for one day, how many records is that?
   How large would the Delta table be?

**You are ready for Phase B when you understand what VWAP means and why `p` and `q` are strings.**

---

## Phase B: Producer and Ingestion

### producer.py

Write a Python script that:
- Connects to the Binance WebSocket for **3 symbols**: BTC, ETH, SOL
  (subscribe to all three in a single connection using combined streams:
  `wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade`)
- Collects events in batches of **200 events**
- Writes each batch as a JSON file to `.../raw/`
- File names: `batch_<symbol>_<unix_timestamp>.json`
- Prints progress after each batch

### ingestion_job.py

Write a PySpark batch job that:
- Reads all JSON files from the landing directory
- Casts `p` to `DoubleType` and `q` to `DoubleType`
- Converts `E` (milliseconds) to a proper `TimestampType` column called `event_time`
- Adds `ingested_at` column
- Writes to `raw-delta` using `mode("append")`

---

## Phase C: Cleansing Job (45 min)

### cleansing_job.py

Read from `raw-delta` and apply these validation rules:

### Data quality rules

| # | Field | Rule | rejection_reason |
|---|-------|------|-----------------|
| 1 | `p` (price as double) | Greater than 0 | `non_positive_price` |
| 2 | `q` (quantity as double) | Greater than 0 | `non_positive_quantity` |
| 3 | `event_time` | Not in the future (not > current time + 5 seconds) | `future_timestamp` |
| 4 | `t` (trade ID) | Not duplicated within the same symbol | `duplicate_trade_id` |
| 5 | `s` (symbol) | Matches pattern `[A-Z]+USDT` | `invalid_symbol` |

**Output:**
- Failing records + `rejection_reason` → `error-delta`
- Passing records → `valid-delta`
- Print a summary with counts per rejection reason and per symbol.

---

## Phase D: Business Analytics Job

### business_job.py — VWAP Dashboard

Read from `valid-delta`.

### Business rules

**Step 1** — compute for each **symbol** and **15-minute window**:

| Column | Description |
|--------|-------------|
| `symbol` | Trading pair (e.g. `BTCUSDT`) |
| `window_start` | Start of the 15-minute window |
| `window_end` | End of the 15-minute window |
| `vwap` | Volume Weighted Average Price = `sum(price × qty) / sum(qty)` |
| `total_volume` | Sum of all quantities traded |
| `trade_count` | Number of trades |
| `price_high` | Maximum price in window |
| `price_low` | Minimum price in window |
| `price_range_pct` | `(price_high - price_low) / vwap × 100` |
| `buyer_initiated_pct` | Percentage of trades where `m = false` (taker buys) |

Additionally, flag windows where `price_range_pct > 1.0` as `high_volatility = true`.

Write results to `business-delta/vwap_15min/`.

**Bonus:** compute a second table with cumulative VWAP (from midnight UTC) per symbol,
updated for each new 15-minute window.

---

## Sample deliverables

```
02-binance-crypto/
├── notes.txt
├── pyproject.toml
└── src/
    └── binance_crypto/
        ├── __init__.py
        ├── config.py
        ├── producer.py
        ├── ingestion_job.py
        ├── cleansing_job.py
        └── business_job.py
```

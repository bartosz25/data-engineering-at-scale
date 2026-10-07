# Practice Project

## Kappa Architecture Implementation

In this project you implement a complete **Kappa architecture** pipeline.

## Target architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│  External Data  │     │   Landing    │     │   Raw Layer     │
│  Source         │────►│   Zone       │────►│   Delta Lake    │
│  (SSE/WS/REST)  │     │  (JSON files)│     │     /raw/       │
└─────────────────┘     └──────────────┘     └────────┬────────┘
                                                      │
                                         ┌────────────▼──────────────┐
                                         │       cleansing_job.py    │
                                         └──────────┬─────────────┬──┘
                                                    │             │
                                         ┌──────────▼──┐  ┌───────▼─────┐
                                         │ valid-data  │  │  error-raw  │
                                         │ Delta Lake  │  │  Delta Lake │
                                         └──────────┬──┘  └─────────────┘
                                                    │
                                         ┌──────────▼──────────┐
                                         │   business_job.py   │
                                         └──────────┬──────────┘
                                                    │
                                         ┌──────────▼───────────┐
                                         │  business Delta Lake │
                                         └──────────────────────┘
```

The workflow is shared across topic proposals:
* you start by consuming an external dataset - it can be done with a regular Python file
  * the output is written in a raw format on local disk, e.g. `/tmp/data-engineering-at-scale/project/raw`
* next you convert the raw dataset with PySpark into a Delta Lake table
  * the table should also be located to local disk, e.g. `/tmp/data-engineering-at-scale/project/raw-delta`
* after this, you need to process the raw Delta Lake table with a PySpark job
  * the job should evaluate each row against business rules defined under _Data quality rules_ section and:
    * write valid rows to a Delta Lake table with valid data, e.g.  `/tmp/data-engineering-at-scale/project/valid-delta`
    * write invalid rows to a Delta Lake table with invalid data, e.g.  `/tmp/data-engineering-at-scale/project/error-delta`
* in the last step you need to write a PySpark job that is going to apply the _Business rules_ section
  * the output should be a new Delta Lake table on disk, e.g. `/tmp/data-engineering-at-scale/project/business-delta`

Recommendations:
* you don't have to keep all the jobs running at the same time, e.g. you can start by
running the ingestion job for 10 minutes, later start the data cleansing job, and so forth
* once you get into the code, you can find some quick helpers in [`helper.md`](helper.md)

## Your next task

Pick one of the available projects. Each uses a different publicly available data source.

**No API key required:**

| # | Project | Data Source | Protocol | Theme |
|---|---------|-------------|----------|-------|
| 01 | [Wikipedia Edits](01-wikipedia-edits/) | Wikimedia EventStreams | SSE | Collaborative editing |
| 02 | [Crypto Trades](02-binance-crypto/) | Binance | WebSocket | Financial markets |
| 03 | [Flight Tracking](03-opensky-flights/) | OpenSky Network | REST poll | Aviation |
| 04 | [Bike Share](04-gbfs-bikeshare/) | GBFS / Citi Bike NYC | REST poll | Urban mobility |
| 05 | [Air Quality](05-sensor-community/) | Sensor.Community | REST poll | IoT sensors |
| 06 | [Hacker News](06-hacker-news/) | HN Firebase API | REST poll | Tech community |
| 07 | [GitHub Events](07-github-events/) | GitHub Events API | REST poll | Open source activity |
| 08 | [London Tube Arrivals](08-tfl-arrivals/) | Transport for London | REST poll | Public transit |

## Proposed session schedule (4 hours)

| Block | Time   | What to do |
|-------|--------|------------|
| **A** | 15 min | Data discovery — explore the API manually, write findings in `notes.txt` |
| **B** | 60 min | `producer.py` + `ingestion_job.py` — land raw data, load to Delta |
| **C** | 45 min | `cleansing_job.py` — validate records, split into valid-data and error-raw |
| **D** | 90 min | `business_job.py` — derive analytical output from valid-data |
| —     | 30 min | Testing, debugging, review with instructor |

## Proposed code organization

Here only to simplify you the bootstrap.

```
your-project/
├── notes.txt                  # Phase A: data discovery findings (required)
├── pyproject.toml
└── src/
    └── project/               # rename to match your project, e.g. wikipedia_edits
        ├── __init__.py
        ├── config.py          # paths and source URL constants
        ├── producer.py        # reads from source, writes JSON to landing zone
        ├── ingestion_job.py   # PySpark: landing JSON → raw Delta Lake
        ├── cleansing_job.py   # PySpark: raw Delta → valid-data Delta + error-raw Delta
        └── business_job.py    # PySpark: valid-data Delta → serving Delta Lake
```

The `pyproject.toml` should declare the package so the module is importable when running jobs:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "pipeline"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = ["pyspark", "delta-spark", "requests"]

[tool.hatch.build.targets.wheel]
packages = ["src/pipeline"]
```

Install in editable mode once, then run any script directly:

```bash
uv sync
uv run src/pipeline/producer.py
uv run src/pipeline/ingestion_job.py
```

## Technical requirements

- SparkSession must include Delta Lake configuration (see [`helper.md`](helper.md) in the project root)
- All Delta tables written under `/tmp/data-engineering-at-scale/99-project/<project-name>/delta/`
- The `error-raw` table must include a `rejection_reason` string column
- Each job must be runnable independently and be re-runnable without errors

## Getting help

The project root contains a [`helper.md`](helper.md) with connection snippets,
schema hints, and phase-by-phase code guidance. Check that before asking your instructor.

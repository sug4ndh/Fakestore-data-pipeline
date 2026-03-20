# FakeStoreAPI Data Pipeline

A data pipeline that pulls from fakestoreapi.com and processes it through a bronze, silver and gold medallion architecture. 
All 3 endpoints (`/users`, `/products`, `/carts`) have been consuemd. Data is stored as Parquet files and exposed via a DuckDB warehouse.

---

The instructions below are for *nix systems.

## Requirements
- Docker + Docker Compose (make sure that Docker can run without sudo)
- Python 3.11+ (for running tests locally)
- DuckDB CLI (optional, for querying results)

## Setup
In the root directory, copy .env.example file to .env file and set your secret key:

```bash
cp .env.example .env
```

Open `.env` and set `PIPELINE_SECRET_KEY` to something random.

Make the run script executable:

```bash
chmod +x run.sh
```

## Running

```bash
./run.sh                  # runs for today
./run.sh 2026-03-15       # runs for a specific date (backfill)
```

This builds and runs all four containers: users, products, carts in parallel then the warehouse once all three finish.
All data persists in `./data/` after containers exit. Re-run with a different date or on a different date to append a new
dated partition.

---

## Querying the results

```bash

# Open the warehouse
duckdb data/warehouse.duckdb

# Show tables
SHOW TABLES;

# QUERY
SELECT * FROM gold_users LIMIT 5;
```


---

## Running Tests Locally

Install dev dependencies for the endpoint you want to test:

```bash
pip install -r pipelines/users/requirements-dev.txt
```

Run all tests from the repo root:

```bash
pytest
```

Run linter:

```bash
ruff check .
ruff format --check .
```

---

## Architecture

Each endpoint runs in its own container and writes through four layers:

```
API -> raw JSON -> bronze Parquet -> silver Parquet -> gold Parquet
```

The data from all bronze, silver and gold layers are finally dumped into duckdb warehouse which is
managed by a fourth container, designed specifically to write the data to duckdb. Duckdb is rebuilt
on each run and contains 9 tables.

```
Gold layers -> duckdb.warehouse
```

The 3 endpoint containers run in parallel.
The warehouse container uses `depends_on: condition: service_completed_successfully` meaninf
it only runs if all three pipelines succeed, and is the sole writer to warehouse.duckdb

**Raw** : exact API response saved as JSON untouched.

**Bronze**: flattened to a DataFrame with `_ingested_at` added. Appended on every run.

**Silver**: PII masked or dropped, types cast, deduplicated. Each run produces a complete snapshot by merging the latest silver file with the new bronze partition.

**Gold** :Reads from silver and generates the deliverable tables.

Partitions follow the Hive convention (`ingestion_date=YYYY-MM-DD/`).

All layers are append-only using Hive-style date partitions:
```
data/bronze/users/ingestion_date=2025-03-18/users.parquet
data/bronze/users/ingestion_date=2025-03-19/users.parquet
```

Silver process incrementally and gold becomes incremental by inheritance.

```
Silver reads:  latest prior Silver snapshot  +  Bronze partition for RUN_DATE
Silver writes: new deduplicated snapshot for RUN_DATE

For silver, always 2 files are needed regardless of how many historical partitions exist

Gold reads: today's Silver snapshot only
Gold writes: new snapshot for RUN_DATE after it applies type casts + rules
```

The folder structure acts as the watermark.

Deduplication happens at **Silver write time**.

| Endpoint | Dedup key |
|---|---|
| users | `id` |
| products | `id` |
| carts | `(id, productId)`|

Each Silver file is a complete deduplicated snapshot. The warehouse reads the
**latest Silver/Gold partition** for current state, and **globs all Bronze/Raw partitions**
for full history.

## PII Handling (Users)

| Field | Treatment |
|---|---|
| `email`, `username`, `name_firstname`, `name_lastname`, `phone` | HMAC-SHA256 with secret key (in-place replacement) |
| `password` | Dropped at Silver|
| `address_street`, `address_number`, `address_geolocation_*` | Dropped at Silver|
| `address_city`, `address_zipcode` | Retained|

## Validation

Two types of validation, spread across these files:
- Pydantic (fetch boundary):
   API response shape and types before any data is written 

- Pandera (Silver and Gold):
   PII masking correctness (64-char HMAC), dropped columns, dtypes, 
   rules: `price > 0`, `quantity > 0`, non-null foreign keys.

---

## Assumptions and Shortcuts

- The schema returned by the endpoints differed from what was documented. I used the actual response shape as the source of truth and built Pydantic models from that.
- In this setup, extract, transform and load all run inside the same container per endpoint. In production these would be separate jobs so each step can be retried, scaled or replaced independently.
- FakeStoreAPI has no ?updated_since= parameter so every run fetches the full dataset. The storage structure is designed as if incremental data arrives though, so swapping in a real API that supports it would need no changes to the pipeline layout.
- Data lives on the local filesystem via a bind mount.
- docker compose up is the run mechanism here.
- the DuckDB warehouse is always recreated from the latest partitions. Historical Parquet files are never deleted so the full history is still there.

--- 

## What I Would Improve With More Time

- Move Raw/Bronze to S3 with access-controlled bucket policies 
- Migrate `PIPELINE_SECRET_KEY` to AWS Secrets Manager
- Airflow DAG 
- Alerting for failures
- handling late arriving data
- comprehensive unit tests and integration tests that run the full pipeline end-to-end
- ci-cd with GitHub Actions workflow

---

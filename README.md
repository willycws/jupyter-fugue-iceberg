# Jupyter Fugue Iceberg Demo Project

This project loads e-commerce sample data into Iceberg via an Airflow DAG, then uses Fugue in Jupyter to query the data across multiple execution backends (Pandas, DuckDB, Spark, Dask, Ray).

## Architecture

```
Data Ingestion:
  Airflow DAG -> PySpark -> Iceberg REST Catalog -> MinIO (S3)

Data Exploration:
  Jupyter -> PyIceberg -> Arrow/Pandas DataFrames
       |
       v
  Fugue %%fsql magic -> [Pandas | DuckDB | Spark | Dask | Ray] -> DataFrame
```

## Execution Flow

```
Jupyter Notebook
       |
       v
PyIceberg loads tables from Iceberg REST Catalog
       |
       v
Arrow / Pandas DataFrames
       |
       v
Fugue %%fsql magic cell (choose backend per query)
       |
       v
[Pandas | DuckDB | Spark | Dask | Ray]
       |
       v
Data Scientist analyzes results
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Git

## Getting Started

```bash
# Clone the repository
git clone <repo-url>
cd jupyter-fugue-iceberg

# Start all services
./start.sh
```

The startup script will:
1. Build a custom Airflow image with pre-installed dependencies
2. Initialize the Airflow database and create an admin user
3. Start all services (Airflow, Spark, MinIO, Iceberg, Jupyter)

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Spark Master UI | http://localhost:9090 | - |
| Spark History Server | http://localhost:18080 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Iceberg REST Catalog | http://localhost:8181 | - |
| Jupyter Lab | http://localhost:8888 | no auth (token disabled) |

## Project Structure

```
jupyter-fugue-iceberg/
├── src/                          # DAG files (mounted as /opt/airflow/dags)
│   ├── Insert_Customer_Order_Data_To_Iceberg.py
│   └── Insert_Benchmark_Data_To_Iceberg.py
├── spark/
│   └── apps/                     # PySpark scripts
│       ├── generate_sample_data.py
│       └── verify_data.py
├── jupyter/
│   └── Dockerfile                # Custom Jupyter image with Fugue + PyIceberg
├── notebooks/
│   ├── sample_fugue_iceberg.ipynb       # Fugue multi-backend demo (Pandas/DuckDB/Spark/Dask/Ray)
│   ├── sample_datafusion_iceberg.ipynb  # Legacy DataFusion notebook
│   ├── benchmark_fugue_backends.ipynb   # Benchmark: Fugue-based SELECT * across 5 engines
│   └── benchmark_native_backends.ipynb  # Benchmark: Native API SELECT * across 5 engines (no Fugue)
├── docker-compose.yaml           # Full stack definition
├── Dockerfile                    # Custom Airflow image with Java + Python deps
├── start.sh                      # Startup script with graceful shutdown
├── CLAUDE.md                     # Project context for Claude Code
└── README.md
```

## How to run the demo
1. Access http://localhost:8080 via a browser. Enable the `Insert_Customer_Order_Data_To_Iceberg` DAG and run it. This seeds 5 e-commerce tables into Iceberg.
2. (Optional) Enable the `Insert_Benchmark_Data_To_Iceberg` DAG and run it. This seeds 4 benchmark dimension tables.
3. Access http://localhost:8888 via a browser.
   - Open `sample_fugue_iceberg.ipynb` — queries 5 tables across 5 different execution backends
   - Open `benchmark_fugue_backends.ipynb` — benchmarks SELECT * via Fugue across all 5 engines
   - Open `benchmark_native_backends.ipynb` — benchmarks SELECT * via native APIs (no Fugue) for comparison

## DAGs
### Insert_Customer_Order_Data_To_Iceberg
Seeds 5 e-commerce tables into Iceberg using PySpark with referential integrity.

**Pipeline:** `create_namespace -> seed_all_tables`

| Table | Rows | Description |
|-------|------|-------------|
| `ecommerce.products` | 10 | Product catalog with prices and costs |
| `ecommerce.customers` | 500 | Customer profiles with segments and regions |
| `ecommerce.orders` | 100K | Order line items referencing products and customers |
| `ecommerce.clickstream` | 1M | User browsing events with sessions |
| `ecommerce.recommendations` | 50K | Precomputed product recommendations with scores |

### Insert_Benchmark_Data_To_Iceberg
Seeds 4 benchmark dimension tables into Iceberg for engine performance comparison.

**Pipeline:** `create_namespace -> seed_all_tables`

| Table | Columns | Rows | Description |
|-------|---------|------|-------------|
| `benchmark.small_dim_100k` | 10 | 100K | Small dimension, medium row count |
| `benchmark.large_dim_100k` | 50 | 100K | Large dimension, medium row count |
| `benchmark.small_dim_200k` | 10 | 200K | Small dimension, large row count |
| `benchmark.large_dim_200k` | 50 | 200K | Large dimension, large row count |

Each table has 10 fixed columns (id, category, region, status, priority, created_date, amount, score, flag, description) plus additional columns cycling through int, float, string, date, and decimal types. Written in 50K-row batches to avoid OOM.

## Notebooks

### Fugue — Multi-Backend Demo (`sample_fugue_iceberg.ipynb`)

Demonstrates Fugue's backend portability: write SQL once, run on any engine.

| Table | Backend | Query |
|-------|---------|-------|
| products (10 rows) | Pandas | Margin analysis — zero overhead for tiny data |
| customers (500 rows) | DuckDB | Segmentation analytics — in-process OLAP |
| orders (100K rows) | Spark | Enrichment joins — distributed processing |
| clickstream (1M rows) | Dask | Sessionization — parallel Python |
| recommendations (50K rows) | Ray | Scoring filter — parallel compute |

Also includes: backend portability comparison (same query on 3 engines), Python+SQL hybrid transforms, and visualizations.

> **Prerequisite:** Run `Insert_Customer_Order_Data_To_Iceberg` DAG first.

### Benchmark — Fugue Backends (`benchmark_fugue_backends.ipynb`)

Benchmarks `SELECT *` query time across Fugue backends on the 4 benchmark dimension tables. Produces a consolidated timing table, grouped bar chart, and fastest-engine-per-table summary. Configurable `INCLUDE_DASK` and `INCLUDE_RAY` flags (both default `False`) to skip memory-heavy engines. Ray has a 3-retry limit with "Insufficient memory error" fallback.

### Benchmark — Native Backends (`benchmark_native_backends.ipynb`)

Same benchmark as above but using each engine's **native API** (no Fugue): Pandas `df.copy()`, DuckDB `duckdb.sql()`, Spark `spark.sql()`, Dask `dd.from_pandas().compute()`, Ray `ray.put()/ray.get()`. Same `INCLUDE_DASK`/`INCLUDE_RAY` config flags. Compare with the Fugue notebook to measure Fugue's abstraction overhead.

> **Prerequisite:** Run `Insert_Benchmark_Data_To_Iceberg` DAG first for both benchmark notebooks.

## Tech Stack

- **Apache Airflow 2.10.5** — Workflow orchestration
- **Apache Spark 3.5.6** — Distributed data processing (writes to Iceberg)
- **Apache Iceberg** — Open table format for analytics
- **Fugue** — Unified interface for multi-backend execution (Pandas, DuckDB, Spark, Dask, Ray)
- **DuckDB** — In-process OLAP engine
- **Dask** — Parallel computing with Python
- **Ray** — Distributed compute framework
- **PyIceberg** — Python client for Iceberg REST catalog
- **Apache Arrow** — In-memory columnar format
- **MinIO** — S3-compatible object storage
- **PostgreSQL 16** — Airflow metadata database
- **Redis 7** — Celery message broker
- **Jupyter Lab** — Interactive notebook environment
- **Docker Compose** — Container orchestration

## Stopping Services

Press `Ctrl+C` if running via `./start.sh` (graceful shutdown), or:

```bash
docker compose down       # Stop all services
docker compose down -v    # Stop and remove all data volumes
```

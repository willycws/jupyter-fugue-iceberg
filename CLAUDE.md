# Project: Jupyter + Fugue + Iceberg Demo

## Overview

Demonstrate Fugue's multi-backend capability by querying e-commerce Iceberg tables through different execution engines (Pandas, DuckDB, Spark, Dask, Ray) in Jupyter notebooks.

## Architecture

```
Data Ingestion:
  Airflow DAG -> PySpark (local mode) -> Iceberg REST Catalog -> MinIO (S3)

Data Exploration:
  Jupyter -> PyIceberg -> Arrow/Pandas DataFrames
       |
       v
  Fugue %%fsql magic -> [Pandas | DuckDB | Spark | Dask | Ray] -> DataFrame
```

## Iceberg Tables (ecommerce namespace)

| Table | Rows | Fugue Backend | Rationale |
|-------|------|---------------|-----------|
| `products` | 10 | Pandas | Tiny lookup table, zero overhead |
| `customers` | 500 | DuckDB | Analytical aggregations, in-process OLAP |
| `orders` | 100K | Spark | Join-heavy enrichment, distributed processing |
| `clickstream` | 1M | Dask | Parallel sessionization on large event data |
| `recommendations` | 50K | Ray | Parallel batch scoring |

All tables maintain referential integrity via shared product_id and customer_id values.

## Key Files

| File | Purpose |
|------|---------|
| `src/Insert_Customer_Order_Data_To_Iceberg.py` | Airflow DAG: seeds all 5 tables in one SparkSession |
| `spark/apps/generate_sample_data.py` | Standalone script: seeds all 5 tables |
| `spark/apps/verify_data.py` | Standalone script: verifies all 5 tables |
| `jupyter/Dockerfile` | Jupyter image with Fugue, DuckDB, PySpark, Dask, Ray, dask-sql |
| `notebooks/sample_fugue_iceberg.ipynb` | Fugue multi-backend demo (main notebook) |
| `notebooks/sample_datafusion_iceberg.ipynb` | Legacy DataFusion notebook (not part of main demo) |
| `docker-compose.yaml` | Full stack (Airflow, Spark, MinIO, Iceberg REST, Jupyter) |
| `Dockerfile` | Airflow image with Java + PySpark |

## Lessons Learned / Gotchas

### Fugue
- `%%fsql` magic requires separate `fugue-jupyter` package + `fugue-jupyter install startup` in Dockerfile
- FugueSQL `PRINT` takes no arguments — use `TAKE N ROWS` then `PRINT` to limit output
- `fa.fugue_sql()` returns PyArrow Table, not Pandas — use `fa.as_pandas(result)` to convert
- Dask SQL backend requires `dask-sql` package (not included in `dask[complete]`)
- Deprecation warning from `fugue_sql` import is harmless — comes from fugue-jupyter internals

### PySpark in Airflow
- Multiple PythonOperator tasks calling `spark.stop()` causes `JAVA_GATEWAY_EXITED` — JVM can't restart in same worker process
- Fix: consolidate all seeds into a single function with one SparkSession
- DAG pipeline: `create_namespace >> seed_all_tables`

### Docker / Infrastructure
- `JAVA_HOME` must use `/usr/lib/jvm/default-java` (architecture-agnostic symlink), not hardcoded `arm64`/`amd64` path
- docker-compose `environment` overrides Dockerfile `ENV` — check both when debugging env vars
- Ray needs `shm_size: 2g` in docker-compose (Docker defaults to 64MB /dev/shm)
- `RAY_OBJECT_STORE_MEMORY: '200000000'` limits Ray object store to ~200MB
- Jupyter container needs `deploy.resources.limits.memory: 6g` for Ray/Dask/Spark

## Tech Stack

- Apache Airflow 2.10.5 — Workflow orchestration
- Apache Spark 3.5.6 — Data processing (DAG writes to Iceberg)
- Apache Iceberg — Open table format
- Fugue + fugue-jupyter — Unified multi-backend SQL execution
- DuckDB — In-process OLAP engine
- Dask + dask-sql — Parallel computing with SQL support
- Ray — Distributed compute framework
- PyIceberg — Python client for Iceberg REST catalog
- Apache Arrow — In-memory columnar format
- MinIO — S3-compatible object storage
- PostgreSQL 16 — Airflow metadata DB
- Redis 7 — Celery broker
- Jupyter Lab — Interactive notebooks
- Docker Compose — Container orchestration

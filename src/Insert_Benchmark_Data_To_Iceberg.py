from datetime import datetime, timedelta, date
from decimal import Decimal
import random
import string

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Parameters
DAG_ID = 'Insert_Benchmark_Data_To_Iceberg'
SPARK_MASTER = 'local[*]'
ICEBERG_CATALOG_URI = 'http://iceberg-rest:8181'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'

ICEBERG_PACKAGES = (
    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,'
    'org.apache.iceberg:iceberg-aws-bundle:1.5.2'
)

NAMESPACE = 'benchmark'

# Table configs: (table_name, num_columns, num_rows)
TABLE_CONFIGS = [
    ('small_dim_100k', 10, 100_000),
    ('large_dim_100k', 50, 100_000),
    ('small_dim_200k', 10, 200_000),
    ('large_dim_200k', 50, 200_000),
]

# Column value pools for realistic data generation
CATEGORIES = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys', 'Food', 'Auto']
REGIONS = ['US-West', 'US-East', 'US-Central', 'EU-West', 'EU-East', 'APAC', 'LATAM', 'MEA']
STATUSES = ['active', 'inactive', 'pending', 'archived']
PRIORITIES = ['low', 'medium', 'high', 'critical']

BATCH_SIZE = 50_000


def _get_spark_session(app_name):
    from pyspark.sql import SparkSession
    return SparkSession.builder \
        .appName(app_name) \
        .master(SPARK_MASTER) \
        .config("spark.jars.packages", ICEBERG_PACKAGES) \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "rest") \
        .config("spark.sql.catalog.iceberg.uri", ICEBERG_CATALOG_URI) \
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.iceberg.s3.endpoint", S3_ENDPOINT) \
        .config("spark.sql.catalog.iceberg.s3.access-key-id", S3_ACCESS_KEY) \
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", S3_SECRET_KEY) \
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
        .config("spark.sql.catalog.iceberg.s3.region", "us-east-1") \
        .config("spark.executorEnv.AWS_REGION", "us-east-1") \
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", S3_ACCESS_KEY) \
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", S3_SECRET_KEY) \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()


def _build_schema(num_columns):
    """Build a schema with a mix of column types.

    Fixed columns: id, category, region, status, priority,
    created_date, amount, score, flag, description.
    Remaining columns cycle through: int, float, string, date, decimal.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        DecimalType, DateType, FloatType, BooleanType,
    )

    fields = [
        StructField("id", IntegerType(), False),
        StructField("category", StringType(), False),
        StructField("region", StringType(), False),
        StructField("status", StringType(), False),
        StructField("priority", StringType(), False),
        StructField("created_date", DateType(), False),
        StructField("amount", DecimalType(12, 2), False),
        StructField("score", FloatType(), False),
        StructField("flag", BooleanType(), False),
        StructField("description", StringType(), False),
    ]

    type_cycle = [
        ("int", IntegerType()),
        ("float", FloatType()),
        ("str", StringType()),
        ("date", DateType()),
        ("decimal", DecimalType(10, 2)),
    ]

    for i in range(10, num_columns):
        type_name, spark_type = type_cycle[i % len(type_cycle)]
        fields.append(StructField(f"col_{type_name}_{i}", spark_type, True))

    return StructType(fields)


def _generate_row(row_id, num_columns, rng):
    """Generate a single row matching the schema from _build_schema."""
    row = [
        row_id,
        rng.choice(CATEGORIES),
        rng.choice(REGIONS),
        rng.choice(STATUSES),
        rng.choice(PRIORITIES),
        date(2020 + rng.randint(0, 4), rng.randint(1, 12), rng.randint(1, 28)),
        Decimal(f"{rng.uniform(1.0, 10000.0):.2f}"),
        round(rng.uniform(0.0, 100.0), 2),
        rng.choice([True, False]),
        f"item_{row_id}_{rng.choice(string.ascii_lowercase)}",
    ]

    type_cycle_len = 5  # int, float, str, date, decimal
    for i in range(10, num_columns):
        mod = i % type_cycle_len
        if mod == 0:    # int
            row.append(rng.randint(0, 1_000_000))
        elif mod == 1:  # float
            row.append(round(rng.uniform(0.0, 1000.0), 2))
        elif mod == 2:  # str
            row.append(f"val_{rng.randint(0, 10000)}")
        elif mod == 3:  # date
            row.append(date(2020 + rng.randint(0, 4), rng.randint(1, 12), rng.randint(1, 28)))
        elif mod == 4:  # decimal
            row.append(Decimal(f"{rng.uniform(0.0, 5000.0):.2f}"))

    return tuple(row)


def seed_all_tables():
    """Seed all 4 benchmark tables using a single SparkSession."""
    spark = _get_spark_session("SeedBenchmarkTables")

    for table_name, num_columns, num_rows in TABLE_CONFIGS:
        fqn = f"iceberg.{NAMESPACE}.{table_name}"
        print(f"Seeding {fqn} ({num_rows:,} rows, {num_columns} cols)...")

        schema = _build_schema(num_columns)
        rng = random.Random(hash(table_name) & 0xFFFFFFFF)

        # Write in batches to avoid OOM on large tables
        first_batch = True
        for batch_start in range(0, num_rows, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, num_rows)
            rows = [
                _generate_row(i, num_columns, rng)
                for i in range(batch_start + 1, batch_end + 1)
            ]
            df = spark.createDataFrame(rows, schema)

            if first_batch:
                df.writeTo(fqn).using("iceberg").createOrReplace()
                first_batch = False
            else:
                df.writeTo(fqn).append()

            print(f"  wrote rows {batch_start + 1:,} - {batch_end:,}")

        count = spark.read.table(fqn).count()
        print(f"  {fqn}: {count:,} rows verified")

    spark.stop()
    print("Done — all benchmark tables seeded and verified.")


default_args = {
    'owner': 'Willy',
    'start_date': datetime(2024, 1, 1),
    'email': ['test@test.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Seed benchmark dimension tables into Iceberg (varying width and row count)',
    schedule_interval=None,
    catchup=False,
)

create_namespace = BashOperator(
    task_id='create_namespace',
    bash_command='curl -s -X POST http://iceberg-rest:8181/v1/namespaces '
                 '-H "Content-Type: application/json" '
                 f'-d \'{{"namespace": ["{NAMESPACE}"]}}\' || true',
    dag=dag,
)

seed_all_task = PythonOperator(
    task_id='seed_all_tables',
    python_callable=seed_all_tables,
    dag=dag,
)

create_namespace >> seed_all_task

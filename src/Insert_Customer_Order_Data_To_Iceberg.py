from datetime import datetime, timedelta, date
from decimal import Decimal
import random

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Parameters
DAG_ID = 'Insert_Customer_Order_Data_To_Iceberg'
SPARK_MASTER = 'local[*]'
ICEBERG_CATALOG_URI = 'http://iceberg-rest:8181'
S3_ENDPOINT = 'http://minio:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'

ICEBERG_PACKAGES = (
    'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,'
    'org.apache.iceberg:iceberg-aws-bundle:1.5.2'
)

# Shared reference data for referential integrity
PRODUCT_CATALOG = [
    ("PROD-001", "Wireless Mouse", "Peripherals", Decimal("29.99"), Decimal("12.50"), "TechCorp"),
    ("PROD-002", "Mechanical Keyboard", "Peripherals", Decimal("89.99"), Decimal("35.00"), "KeyMaster"),
    ("PROD-003", "USB-C Hub", "Accessories", Decimal("45.50"), Decimal("18.00"), "ConnectPro"),
    ("PROD-004", "Monitor Stand", "Furniture", Decimal("34.99"), Decimal("14.00"), "DeskWorks"),
    ("PROD-005", "Webcam HD", "Peripherals", Decimal("59.99"), Decimal("22.00"), "VisionTech"),
    ("PROD-006", "Laptop Sleeve", "Accessories", Decimal("19.99"), Decimal("6.50"), "BagCo"),
    ("PROD-007", "Desk Lamp", "Furniture", Decimal("24.99"), Decimal("9.00"), "LightHouse"),
    ("PROD-008", "Headphones", "Audio", Decimal("79.99"), Decimal("30.00"), "SoundWave"),
    ("PROD-009", "Mouse Pad", "Accessories", Decimal("12.99"), Decimal("3.50"), "PadMaster"),
    ("PROD-010", "Cable Organizer", "Accessories", Decimal("9.99"), Decimal("2.80"), "TidyDesk"),
]

CUSTOMER_SEGMENTS = ["premium", "standard", "basic"]
CUSTOMER_REGIONS = ["US-West", "US-East", "US-Central", "EU-West", "EU-East", "APAC"]
FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack",
               "Karen", "Leo", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Rosa", "Sam", "Tina"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson", "Moore"]

ORDER_STATUSES = ["completed", "completed", "completed", "pending", "cancelled"]
EVENT_TYPES = ["page_view", "page_view", "page_view", "add_to_cart", "add_to_cart", "purchase"]
DEVICES = ["desktop", "mobile", "tablet"]
RECOMMENDATION_REASONS = ["similar_purchase", "trending", "frequently_bought_together", "browsing_history"]

NUM_CUSTOMERS = 500
NUM_ORDERS = 100_000
NUM_CLICKSTREAM = 1_000_000
NUM_RECOMMENDATIONS = 50_000


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
        .getOrCreate()


def seed_all_tables():
    """Seed all 5 e-commerce tables using a single SparkSession.

    Uses one JVM to avoid JAVA_GATEWAY_EXITED errors when running
    multiple PySpark tasks in the same Airflow worker process.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        DecimalType, DateType, FloatType, TimestampType,
    )

    spark = _get_spark_session("SeedAllTables")

    # ── Products (10 rows) ──────────────────────────────
    print("Seeding products...")
    df = spark.createDataFrame(PRODUCT_CATALOG, StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("price", DecimalType(10, 2), False),
        StructField("cost", DecimalType(10, 2), False),
        StructField("supplier", StringType(), False),
    ]))
    df.writeTo("iceberg.ecommerce.products").using("iceberg").createOrReplace()
    print(f"  products: {df.count()} rows")

    # ── Customers (500 rows) ────────────────────────────
    print("Seeding customers...")
    random.seed(42)
    customer_rows = []
    for i in range(1, NUM_CUSTOMERS + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        customer_rows.append((
            f"CUST-{i:04d}",
            f"{first} {last}",
            f"{first.lower()}.{last.lower()}{i}@example.com",
            random.choice(CUSTOMER_SEGMENTS),
            random.choice(CUSTOMER_REGIONS),
            date(2023, random.randint(1, 12), random.randint(1, 28)),
        ))
    df = spark.createDataFrame(customer_rows, StructType([
        StructField("customer_id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("region", StringType(), False),
        StructField("signup_date", DateType(), False),
    ]))
    df.writeTo("iceberg.ecommerce.customers").using("iceberg").createOrReplace()
    print(f"  customers: {df.count()} rows")

    # ── Orders (100K rows) ──────────────────────────────
    print("Seeding orders...")
    random.seed(43)
    product_ids = [p[0] for p in PRODUCT_CATALOG]
    product_prices = {p[0]: p[3] for p in PRODUCT_CATALOG}
    order_rows = []
    for i in range(1, NUM_ORDERS + 1):
        pid = random.choice(product_ids)
        order_rows.append((
            f"ORD-{i:06d}",
            f"CUST-{random.randint(1, NUM_CUSTOMERS):04d}",
            pid,
            random.randint(1, 5),
            product_prices[pid],
            date(2024, random.randint(1, 12), random.randint(1, 28)),
            random.choice(ORDER_STATUSES),
        ))
    df = spark.createDataFrame(order_rows, StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("order_date", DateType(), False),
        StructField("status", StringType(), False),
    ]))
    df.writeTo("iceberg.ecommerce.orders").using("iceberg").createOrReplace()
    print(f"  orders: {df.count()} rows")

    # ── Clickstream (1M rows) ───────────────────────────
    print("Seeding clickstream...")
    random.seed(44)
    click_rows = []
    session_counter = 0
    for i in range(1, NUM_CLICKSTREAM + 1):
        if i % 10 == 1:
            session_counter += 1
        ts = datetime(2024, random.randint(1, 12), random.randint(1, 28),
                      random.randint(0, 23), random.randint(0, 59), random.randint(0, 59))
        click_rows.append((
            f"EVT-{i:07d}",
            f"SESS-{session_counter:06d}",
            f"CUST-{random.randint(1, NUM_CUSTOMERS):04d}",
            random.choice(product_ids),
            random.choice(EVENT_TYPES),
            ts,
            random.choice(DEVICES),
        ))
    df = spark.createDataFrame(click_rows, StructType([
        StructField("event_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("device", StringType(), False),
    ]))
    df.writeTo("iceberg.ecommerce.clickstream").using("iceberg").createOrReplace()
    print(f"  clickstream: {df.count()} rows")

    # ── Recommendations (50K rows) ──────────────────────
    print("Seeding recommendations...")
    random.seed(45)
    rec_rows = []
    for i in range(1, NUM_RECOMMENDATIONS + 1):
        rec_rows.append((
            f"CUST-{random.randint(1, NUM_CUSTOMERS):04d}",
            random.choice(product_ids),
            round(random.uniform(0.1, 1.0), 2),
            random.choice(RECOMMENDATION_REASONS),
            date(2024, random.randint(6, 12), random.randint(1, 28)),
        ))
    df = spark.createDataFrame(rec_rows, StructType([
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("score", FloatType(), False),
        StructField("reason", StringType(), False),
        StructField("generated_at", DateType(), False),
    ]))
    df.writeTo("iceberg.ecommerce.recommendations").using("iceberg").createOrReplace()
    print(f"  recommendations: {df.count()} rows")

    # ── Verify ──────────────────────────────────────────
    print("\nVerifying all tables...")
    for table in ["products", "customers", "orders", "clickstream", "recommendations"]:
        fqn = f"iceberg.ecommerce.{table}"
        count = spark.read.table(fqn).count()
        print(f"  {fqn}: {count} rows")

    spark.stop()
    print("Done — all tables seeded and verified.")


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
    description='Seed e-commerce tables into Iceberg for Fugue demo',
    schedule_interval=None,
    catchup=False,
)

create_namespace = BashOperator(
    task_id='create_namespace',
    bash_command='curl -s -X POST http://iceberg-rest:8181/v1/namespaces '
                 '-H "Content-Type: application/json" '
                 '-d \'{"namespace": ["ecommerce"]}\' || true',
    dag=dag,
)

seed_all_task = PythonOperator(
    task_id='seed_all_tables',
    python_callable=seed_all_tables,
    dag=dag,
)

# Pipeline: create namespace, then seed + verify all tables in one SparkSession
create_namespace >> seed_all_task

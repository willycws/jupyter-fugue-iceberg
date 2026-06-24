#!/usr/bin/env bash
set -e

cleanup() {
    echo ""
    echo "Caught interrupt. Stopping all services..."
    docker compose down
    echo "All services stopped."
    exit 0
}

trap cleanup SIGINT SIGTERM

# Create required directories
mkdir -p ./logs ./plugins ./config ./spark/apps

# Set Airflow user ID
echo "AIRFLOW_UID=$(id -u)" > .env

# Build custom Airflow image with pre-installed Python packages
echo "Building custom Airflow image..."
docker compose build

# Initialize the database & create admin user
docker compose up airflow-init

# Start all services in foreground (so Ctrl+C triggers cleanup)
echo ""
echo "Starting all services... (Ctrl+C to stop)"
echo ""
echo "  Airflow UI:           http://localhost:8080  (airflow/airflow)"
echo "  Spark Master UI:      http://localhost:9090"
echo "  MinIO Console:        http://localhost:9001  (minioadmin/minioadmin)"
echo "  Iceberg REST Catalog: http://localhost:8181"
echo "  Jupyter Notebook:     http://localhost:8888  (no token required)"
echo ""

docker compose up

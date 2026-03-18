#!/bin/bash
set -e

echo "=== dbt runner starting ==="

echo "Waiting for PostgreSQL..."
until pg_isready -h postgres -p 5432 -U cdc_user -d cdc_db > /dev/null 2>&1; do
    sleep 2
done
echo "PostgreSQL is ready."

echo "Waiting 60s for Flink to populate staging tables..."
sleep 60

echo "Running initial dbt run..."
cd /usr/app/dbt_project
dbt run --profiles-dir /usr/app/dbt_project 2>&1
echo "Initial dbt run complete."

echo "Starting dbt run loop (every 30s)..."
while true; do
    sleep 30
    echo "$(date): Running dbt..."
    dbt run --profiles-dir /usr/app/dbt_project 2>&1
done

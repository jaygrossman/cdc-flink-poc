# CDC Flink PoC

PostgreSQL (JSONB) -> Debezium CDC -> Kafka -> Flink SQL -> PostgreSQL (normalized tables)

A fully dockerized proof-of-concept that demonstrates Change Data Capture from a PostgreSQL JSONB column, streamed through Kafka via Debezium, flattened into normalized relational tables by Flink SQL, and written back to PostgreSQL.

## Prerequisites

- Docker Desktop for Mac (or Docker Engine + Docker Compose on Linux)
- No other local installs required

## Quick Start

```bash
cd cdc-flink-poc
docker compose up --build
```

That's it. Everything starts automatically.

## What Happens on Startup

1. **PostgreSQL** starts with logical replication enabled, creates the source `policy` table and five `output_*` tables, then seeds 4 sample auto insurance policy records (JSONB)
2. **Zookeeper + Kafka** start and become healthy
3. **Kafka Connect (Debezium)** starts and waits for Kafka
4. **Setup container** registers the Debezium PostgreSQL connector via REST API — Debezium begins capturing changes from the `policy` table and publishing them to the `cdc.public.policy` Kafka topic
5. **Flink JobManager** starts, waits for TaskManager registration, then submits the SQL job
6. **Flink SQL job** reads CDC events from Kafka, extracts and flattens the nested JSONB into 5 normalized tables, and writes them to PostgreSQL via JDBC (upsert mode)
7. **PGAdmin** is available for browsing the database

The initial seed data flows through the entire pipeline automatically. Within ~30-60 seconds of all services being healthy, the output tables will be populated.

## Verify It Works

### Check the Output Tables

Open **PGAdmin** at [http://localhost:5050](http://localhost:5050)
- Email: `admin@admin.com`
- Password: `admin`

Connect to the pre-configured "CDC Postgres" server, open a query tool on `cdc_db`, and run:

```sql
SELECT * FROM output_policy;
SELECT * FROM output_coverage;
SELECT * FROM output_vehicle;
SELECT * FROM output_driver;
SELECT * FROM output_claim;
```

You should see 4 policies, their coverages, vehicles, drivers, and claims — all flattened from the original JSONB.

### Check the Flink Dashboard

Open [http://localhost:8081](http://localhost:8081) — you should see one running job with the statement set.

### Test Real-Time CDC

Insert a new policy record in PGAdmin (or via `psql`):

```sql
INSERT INTO policy (data) VALUES ('{
  "policy_number": "POL-2024-99999",
  "status": "active",
  "effective_date": "2024-06-01",
  "expiration_date": "2025-06-01",
  "policyholder": {
    "first_name": "Test",
    "last_name": "User",
    "date_of_birth": "1990-01-01",
    "contact": {
      "email": "test@example.com",
      "phone": "+1-555-9999",
      "address": {
        "street": "123 Test St",
        "city": "Testville",
        "state": "CA",
        "zip": "90210"
      }
    }
  },
  "coverages": [
    {"type": "liability", "limit": 300000, "deductible": 500, "premium": 900.00}
  ],
  "vehicles": [
    {
      "vin": "TEST12345678901234",
      "year": 2023,
      "make": "Tesla",
      "model": "Model 3",
      "drivers": [
        {"name": "Test User", "license_number": "T000-0000-0000", "is_primary": true}
      ]
    }
  ],
  "claims_history": []
}'::jsonb);
```

Within ~15 seconds, query the output tables again — the new record should appear flattened across all relevant tables.

### Run the Verification Script

A comprehensive end-to-end verification script is included. It checks every component of the pipeline (services, Debezium, Kafka, Flink, output tables) and performs a live CDC insert test.

```bash
./verify-pipeline.sh
```

The script validates all 7 steps: Docker services, seed data, Debezium connector, Kafka topic, Flink job, output table contents, and a live insert round-trip. All 26 checks should pass with a "Pipeline is fully operational" message.

## Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| PGAdmin | http://localhost:5050 | admin@admin.com / admin |
| Flink Dashboard | http://localhost:8081 | — |
| Kafka Connect REST | http://localhost:8083 | — |
| PostgreSQL | localhost:5432 | cdc_user / cdc_pass / cdc_db |

## Tear Down

```bash
docker compose down -v
```

The `-v` flag removes volumes so the next `docker compose up --build` starts fresh.

## Troubleshooting

### Flink job not showing as running
- Check Flink JobManager logs: `docker compose logs flink-jobmanager`
- The JobManager waits for the TaskManager to register before submitting jobs. If the TaskManager is slow to start, the job submission may be delayed.
- Verify the TaskManager is running: `docker compose logs flink-taskmanager`

### Debezium connector not registering
- Check setup container logs: `docker compose logs setup`
- Verify Kafka Connect is healthy: `curl http://localhost:8083/connectors`
- Check connector status: `curl http://localhost:8083/connectors/policy-connector/status`

### Output tables are empty
- Check the Flink job is running at http://localhost:8081
- Verify the Kafka topic has data: `docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic cdc.public.policy --from-beginning --max-messages 1`
- Check Flink JobManager logs for SQL errors: `docker compose logs flink-jobmanager`

### Port conflicts
If any port is already in use locally, edit the port mapping in `docker-compose.yml` (left side of the `:` is the host port).

## Architecture

```
┌──────────┐    CDC     ┌───────┐  Debezium  ┌───────────────┐
│PostgreSQL├───────────>│ Kafka ├───────────>│  Flink SQL    │
│ (JSONB)  │  WAL logs  │       │  JSON msgs │  (flatten +   │
└──────────┘            └───────┘            │   normalize)  │
                                             └───────┬───────┘
                                                     │ JDBC upsert
                                             ┌───────▼───────┐
                                             │  PostgreSQL    │
                                             │ (5 normalized  │
                                             │   tables)      │
                                             └────────────────┘
```

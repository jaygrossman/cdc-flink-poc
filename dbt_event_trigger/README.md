# CDC Flink PoC -- dbt Event Trigger Variant

PostgreSQL (JSONB) -> Debezium CDC -> Kafka -> Flink SQL -> Staging Tables -> PG NOTIFY -> dbt -> Output Tables

This variant replaces the dbt polling loop with an **event-driven trigger**. PostgreSQL LISTEN/NOTIFY fires when Flink writes to staging tables, and a Python listener service debounces the notifications and runs `dbt run` only when new data arrives.

## Prerequisites

- Docker Desktop for Mac (or Docker Engine + Docker Compose on Linux)
- No other local installs required

## Quick Start

```bash
cd dbt_event_trigger
docker compose up --build
```

## Architecture

```
┌──────────────┐       ┌──────────┐       ┌───────┐       ┌───────────┐
│  PostgreSQL  │──CDC──>│ Debezium │──────>│ Kafka │──────>│ Flink SQL │
│  (JSONB)     │       │          │       │       │       │ (flatten) │
└──────────────┘       └──────────┘       └───────┘       └─────┬─────┘
                                                                │ JDBC append
                                                          ┌─────▼─────┐
                                                          │ stg_*     │──NOTIFY──┐
                                                          │ tables    │          │
                                                          └───────────┘          │
                                                                           ┌─────▼──────┐
                                                                           │  Python     │
                                                                           │  LISTEN +   │
                                                                           │  debounce   │
                                                                           └─────┬──────┘
                                                                                 │ dbt run
                                                                           ┌─────▼──────┐
                                                                           │ output_*   │
                                                                           │ tables     │
                                                                           └────────────┘
```

## How the Event Trigger Works

1. Each `stg_*` table has an `AFTER INSERT` trigger that calls `pg_notify('stg_data_arrived', table_name)`
2. A Python service (`event-trigger-dbt.py`) connects to PostgreSQL and issues `LISTEN stg_data_arrived`
3. When Flink checkpoints and flushes data to staging tables, PostgreSQL sends notifications
4. The listener **debounces** for 3 seconds (configurable) to batch notifications from multiple staging tables written in quick succession
5. After the debounce window, `dbt run` executes, merging the latest staging data into output tables
6. The listener then resumes waiting for the next notification

No wasted runs -- dbt only executes when new data actually arrives.

## Key Differences from `dbt_upsert`

| Aspect | dbt_upsert | dbt_event_trigger |
|--------|-----------|-------------------|
| dbt trigger | `sleep 30` loop | PostgreSQL LISTEN/NOTIFY |
| Idle overhead | dbt runs every 30s even with no data | Zero runs when no data arrives |
| Latency after data arrives | Up to 30s (wait for next loop) | ~3s debounce + ~3s dbt run |
| PostgreSQL changes | None | NOTIFY triggers on stg_* tables |

## Configuration

Environment variables for the dbt event-trigger service (set in `docker-compose.yml`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DEBOUNCE_SECONDS` | `3` | Seconds to wait after first notification before running dbt |
| `INITIAL_WAIT_SECONDS` | `60` | Seconds to wait on startup for Flink to populate staging |
| `PGHOST` | `postgres` | PostgreSQL host |
| `PGPORT` | `5432` | PostgreSQL port |
| `PGUSER` | `cdc_user` | PostgreSQL user |
| `PGPASSWORD` | `cdc_pass` | PostgreSQL password |
| `PGDATABASE` | `cdc_db` | PostgreSQL database |

## Verify It Works

```bash
./verify-pipeline.sh
```

The verification script tests the full event-driven flow: inserts a record, waits for it to auto-merge via NOTIFY -> dbt (no manual trigger), and reports end-to-end timing.

## Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| PGAdmin | http://localhost:5050 | admin@admin.com / admin |
| Flink Dashboard | http://localhost:8081 | -- |
| Kafka Connect REST | http://localhost:8083 | -- |
| PostgreSQL | localhost:5432 | cdc_user / cdc_pass / cdc_db |

## Tear Down

```bash
docker compose down -v
```

## Troubleshooting

### dbt not triggering on new data
- Check event trigger logs: `docker compose logs dbt --tail 30`
- Verify NOTIFY triggers exist: `docker compose exec postgres psql -U cdc_user -d cdc_db -c "SELECT tgname FROM pg_trigger WHERE tgname LIKE 'stg_%';"`
- Test NOTIFY manually: `docker compose exec postgres psql -U cdc_user -d cdc_db -c "NOTIFY stg_data_arrived, 'test';"`

### Event trigger not listening
- The listener waits `INITIAL_WAIT_SECONDS` (60s) before starting. Check: `docker compose logs dbt | grep "Listening"`

### Output tables empty but staging has data
- The initial dbt run happens after the 60s wait. Check: `docker compose logs dbt | grep "initial dbt run"`

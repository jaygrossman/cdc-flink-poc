# CDC Flink PoC -- PG Transaction Variant

PostgreSQL (JSONB) -> Debezium CDC -> Kafka -> Flink SQL -> Staging Tables -> PG NOTIFY -> merge_cdc_batch() -> Output Tables

This variant replaces dbt entirely with a single PL/pgSQL function (`merge_cdc_batch()`) that merges all 5 output tables inside one Postgres transaction. It also splits the single Postgres instance into two separate databases: a source database and a CDC database.

## Prerequisites

- Docker Desktop for Mac (or Docker Engine + Docker Compose on Linux)
- No other local installs required

## Quick Start

```bash
cd pg_transaction
docker compose up --build
```

## Architecture

```
┌──────────────┐       ┌──────────┐       ┌───────┐       ┌───────────┐
│ postgres-     │──CDC──>│ Debezium │──────>│ Kafka │──────>│ Flink SQL │
│ source        │       │          │       │       │       │ (flatten) │
│ source_db     │       └──────────┘       └───────┘       └─────┬─────┘
│ port 5433     │                                                │ JDBC append
└──────────────┘                                           ┌─────▼──────────┐
                                                           │ postgres-cdc    │
                                                           │ cdc_db          │
                                                           │ port 5432       │
                                                           │                 │
                                                           │ stg_* tables ───── NOTIFY ──┐
                                                           │ output_* tables │            │
                                                           │ merge_watermark │      ┌─────▼──────┐
                                                           └─────────────────┘      │  Python    │
                                                                    ▲               │  LISTEN +  │
                                                                    │               │  debounce  │
                                                                    └───────────────┤            │
                                                              merge_cdc_batch()     └────────────┘
```

## How It Works

1. **Source DB** (`postgres-source`): Holds the `policy` table with JSONB data. Debezium captures WAL changes.
2. **Flink SQL**: Reads CDC events from Kafka, flattens nested JSONB into 5 entity types, writes to append-only `stg_*` tables on the CDC DB.
3. **NOTIFY triggers**: Each staging table has an `AFTER INSERT` trigger that fires `pg_notify('stg_data_arrived', table_name)`.
4. **Merge listener**: A Python service LISTENs for notifications, debounces for 2s, then calls `SELECT merge_cdc_batch()`.
5. **`merge_cdc_batch()`**: A PL/pgSQL function that runs as a single transaction:
   - For each entity type, finds new staging rows since the last watermark
   - Deduplicates by primary key (latest event wins via `ROW_NUMBER()`)
   - DELETEs matching keys from the output table
   - INSERTs the latest non-delete rows
   - Advances the watermark
   - Returns a JSON summary with row counts and timing

All 5 output tables are updated atomically in one transaction. If any entity fails, the entire batch rolls back.

## Key Differences

| Aspect | append_new_records | dbt_upsert | dbt_event_trigger | pg_transaction |
|--------|-------------------|------------|-------------------|----------------|
| Flink writes to | output_* (upsert) | stg_* (append) | stg_* (append) | stg_* (append) |
| Merge strategy | Flink JDBC upsert | dbt incremental | dbt incremental | PL/pgSQL DELETE+INSERT |
| Delete handling | Ignored | dbt post-hook | dbt post-hook | In-transaction DELETE |
| Atomicity | Per-table | Per-table | Per-table | All 5 tables in 1 txn |
| Databases | 1 (shared) | 1 (shared) | 1 (shared) | 2 (source + cdc) |
| dbt required | No | Yes | Yes | No |
| Trigger | N/A | 30s polling | PG NOTIFY | PG NOTIFY |
| End-to-end latency | ~5-15s | ~30-60s | ~6-10s | ~5-10s |

## What Happens on Startup

1. **postgres-source** starts with logical replication, creates `policy` table, seeds 4 records
2. **postgres-cdc** starts, creates staging + output tables, merge function, NOTIFY triggers
3. **Zookeeper + Kafka** start
4. **Kafka Connect (Debezium)** connects to postgres-source, captures WAL changes
5. **Setup container** registers the Debezium connector
6. **Flink SQL job** reads CDC events from Kafka, flattens JSONB, writes to staging tables on postgres-cdc
7. **Merge listener** waits 60s, runs initial merge, then listens for NOTIFY events

## Service Endpoints

| Service | URL/Port | Credentials |
|---------|----------|-------------|
| PGAdmin | http://localhost:5050 | admin@admin.com / admin |
| Flink Dashboard | http://localhost:8081 | -- |
| Kafka Connect REST | http://localhost:8083 | -- |
| Source PostgreSQL | localhost:5433 | cdc_user / cdc_pass / source_db |
| CDC PostgreSQL | localhost:5432 | cdc_user / cdc_pass / cdc_db |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DEBOUNCE_SECONDS` | `2` | Seconds to wait after first notification before merging |
| `INITIAL_WAIT_SECONDS` | `60` | Seconds to wait on startup for Flink to populate staging |

## Verify

```bash
./verify-pipeline.sh
```

The script tests insert, update, and delete propagation end-to-end with timing.

## Tear Down

```bash
docker compose down -v
```

## Troubleshooting

### Merge not triggering
- Check listener logs: `docker compose logs merge-listener --tail 30`
- Verify NOTIFY triggers: `docker compose exec postgres-cdc psql -U cdc_user -d cdc_db -c "SELECT tgname FROM pg_trigger WHERE tgname LIKE 'stg_%';"`
- Run merge manually: `docker compose exec postgres-cdc psql -U cdc_user -d cdc_db -c "SELECT merge_cdc_batch();"`

### Output tables empty but staging has data
- Check watermarks: `docker compose exec postgres-cdc psql -U cdc_user -d cdc_db -c "SELECT * FROM merge_watermark;"`
- The initial merge runs after 60s. Check: `docker compose logs merge-listener | grep "initial merge"`

### Debezium not connecting
- Source DB is on port 5433 externally but 5432 internally (Docker network)
- Check connector: `curl -s http://localhost:8083/connectors/policy-connector/status | python3 -m json.tool`

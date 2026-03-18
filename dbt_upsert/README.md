# CDC Flink PoC -- dbt Upsert Variant

PostgreSQL (JSONB) -> Debezium CDC -> Kafka -> Flink SQL -> Staging Tables -> dbt -> Output Tables

This variant adds a **dbt incremental merge layer** between Flink and the final output tables. Flink writes every CDC event (including deletes) to append-only staging tables, and dbt periodically merges the latest state into normalized output tables.

## Prerequisites

- Docker Desktop for Mac (or Docker Engine + Docker Compose on Linux)
- No other local installs required

## Quick Start

```bash
cd dbt_upsert
docker compose up --build
```

## Architecture

```
┌──────────┐    CDC     ┌───────┐  Debezium  ┌───────────────┐
│PostgreSQL├───────────>│ Kafka ├───────────>│  Flink SQL    │
│ (JSONB)  │  WAL logs  │       │  JSON msgs │  (flatten)    │
└──────────┘            └───────┘            └───────┬───────┘
                                                     │ JDBC append
                                             ┌───────▼───────┐
                                             │  stg_* tables  │
                                             │ (append-only,  │
                                             │  all CDC ops)  │
                                             └───────┬───────┘
                                                     │ dbt run (every 30s)
                                             ┌───────▼───────┐
                                             │  output_*      │
                                             │  tables         │
                                             │ (deduplicated,  │
                                             │  deletes applied)│
                                             └────────────────┘
```

## What Happens on Startup

1. **PostgreSQL** starts with logical replication, creates source + staging + output tables, seeds 4 policies
2. **Zookeeper + Kafka** start
3. **Kafka Connect (Debezium)** captures changes from the `policy` table
4. **Setup container** registers the Debezium connector
5. **Flink SQL job** reads CDC events from Kafka, flattens JSONB, and writes to 5 `stg_*` staging tables in append-only mode -- including the `op` field (c/r/u/d) and `event_time`
6. **dbt container** waits 60s for staging data, then runs `dbt run` every 30s
7. **dbt incremental models** merge from staging into output tables:
   - Deduplicates by primary key (keeps latest event per key)
   - Inserts/updates non-delete records via `delete+insert` strategy
   - Removes deleted records via `post_hook` DELETE statements

## Key Differences from `append_new_records`

| Aspect | append_new_records | dbt_upsert |
|--------|-------------------|------------|
| Flink target | `output_*` tables (upsert) | `stg_*` tables (append-only) |
| Delete handling | Ignored | Captured in staging, applied by dbt |
| CDC ops captured | c, r, u only | c, r, u, d (all) |
| Merge strategy | Flink JDBC upsert | dbt incremental `delete+insert` |
| Latency | ~15s (real-time) | ~30-90s (dbt loop interval) |
| Change history | No (overwritten) | Yes (staging tables preserve all events) |

## Verify It Works

### Check Staging Tables

After ~30s, staging tables should have data:

```sql
SELECT count(*) FROM stg_policy;
SELECT count(*) FROM stg_coverage;
SELECT * FROM stg_policy ORDER BY stg_id;  -- shows op column
```

### Check Output Tables

After ~90s (first dbt run), output tables should be populated:

```sql
SELECT * FROM output_policy;
SELECT * FROM output_coverage;
SELECT * FROM output_vehicle;
SELECT * FROM output_driver;
SELECT * FROM output_claim;
```

### Check dbt Logs

```bash
docker compose logs dbt
```

Look for "Completed successfully" messages.

### Test Real-Time CDC

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

Within ~15s the record appears in `stg_policy`. After the next dbt run (~30s), it appears in `output_policy`.

### Run the Verification Script

```bash
./verify-pipeline.sh
```

## Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| PGAdmin | http://localhost:5050 | admin@admin.com / admin |
| Flink Dashboard | http://localhost:8081 | -- |
| Kafka Connect REST | http://localhost:8083 | -- |
| PostgreSQL | localhost:5432 | cdc_user / cdc_pass / cdc_db |

## Trigger dbt Manually

```bash
docker compose exec dbt dbt run --profiles-dir /usr/app/dbt_project
```

## Tear Down

```bash
docker compose down -v
```

## Troubleshooting

### dbt errors
- Check logs: `docker compose logs dbt`
- Check compiled SQL: `docker compose exec dbt cat /usr/app/dbt_project/target/run/cdc_dbt_upsert/models/output_policy.sql`

### Output tables empty but staging tables have data
- dbt may not have run yet (waits 60s on startup). Check: `docker compose logs dbt`
- Run dbt manually: `docker compose exec dbt dbt run --profiles-dir /usr/app/dbt_project`

### Staging tables empty
- Check Flink job: http://localhost:8081
- Check Flink logs: `docker compose logs flink-jobmanager`

### Flink job not starting
- Check TaskManager: `docker compose logs flink-taskmanager`
- Verify connector: `curl http://localhost:8083/connectors/policy-connector/status`

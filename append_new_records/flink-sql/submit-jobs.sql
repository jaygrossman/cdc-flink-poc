-- Flink SQL: CDC Policy Pipeline
-- Reads from Debezium CDC Kafka topic, flattens JSONB, writes to PostgreSQL output tables

SET 'execution.checkpointing.interval' = '10s';
SET 'parallelism.default' = '1';

-- =============================================================================
-- SOURCE: Kafka topic fed by Debezium CDC from PostgreSQL policy table
-- =============================================================================
CREATE TABLE kafka_policy_source (
    `before` ROW<id BIGINT, data STRING, created_at STRING, updated_at STRING>,
    `after` ROW<id BIGINT, data STRING, created_at STRING, updated_at STRING>,
    op STRING,
    ts_ms BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'cdc.public.policy',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cdc-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- =============================================================================
-- SINKS: JDBC tables writing to PostgreSQL output tables
-- =============================================================================

CREATE TABLE sink_policy (
    policy_id BIGINT,
    policy_number STRING,
    status STRING,
    effective_date DATE,
    expiration_date DATE,
    holder_first_name STRING,
    holder_last_name STRING,
    holder_dob DATE,
    holder_email STRING,
    holder_phone STRING,
    holder_street STRING,
    holder_city STRING,
    holder_state STRING,
    holder_zip STRING,
    source_event_time TIMESTAMP(3),
    PRIMARY KEY (policy_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/cdc_db',
    'table-name' = 'output_policy',
    'username' = 'cdc_user',
    'password' = 'cdc_pass',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE sink_coverage (
    policy_id BIGINT,
    coverage_type STRING,
    coverage_limit DECIMAL(12,2),
    deductible DECIMAL(12,2),
    premium DECIMAL(12,2),
    PRIMARY KEY (policy_id, coverage_type) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/cdc_db',
    'table-name' = 'output_coverage',
    'username' = 'cdc_user',
    'password' = 'cdc_pass',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE sink_vehicle (
    policy_id BIGINT,
    vin STRING,
    year_made INT,
    make STRING,
    model STRING,
    PRIMARY KEY (policy_id, vin) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/cdc_db',
    'table-name' = 'output_vehicle',
    'username' = 'cdc_user',
    'password' = 'cdc_pass',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE sink_driver (
    policy_id BIGINT,
    vehicle_vin STRING,
    driver_name STRING,
    license_number STRING,
    is_primary BOOLEAN,
    PRIMARY KEY (policy_id, vehicle_vin, license_number) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/cdc_db',
    'table-name' = 'output_driver',
    'username' = 'cdc_user',
    'password' = 'cdc_pass',
    'driver' = 'org.postgresql.Driver'
);

CREATE TABLE sink_claim (
    policy_id BIGINT,
    claim_id STRING,
    claim_date DATE,
    amount DECIMAL(12,2),
    status STRING,
    description STRING,
    PRIMARY KEY (policy_id, claim_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/cdc_db',
    'table-name' = 'output_claim',
    'username' = 'cdc_user',
    'password' = 'cdc_pass',
    'driver' = 'org.postgresql.Driver'
);

-- =============================================================================
-- TRANSFORMATIONS: Flatten JSONB into normalized tables
-- =============================================================================
-- Using STATEMENT SET to run all INSERT jobs as a single Flink job.
-- Array fields use UNION ALL with indexed access (max 5 elements per array,
-- max 3 vehicles x 3 drivers for the drivers table).
-- WHERE filters out NULL entries from non-existent array indices.

EXECUTE STATEMENT SET
BEGIN

-- ----- output_policy: scalar + nested-scalar fields -----
INSERT INTO sink_policy
SELECT
    `after`.id,
    JSON_VALUE(`after`.data, '$.policy_number'),
    JSON_VALUE(`after`.data, '$.status'),
    CAST(JSON_VALUE(`after`.data, '$.effective_date') AS DATE),
    CAST(JSON_VALUE(`after`.data, '$.expiration_date') AS DATE),
    JSON_VALUE(`after`.data, '$.policyholder.first_name'),
    JSON_VALUE(`after`.data, '$.policyholder.last_name'),
    CAST(JSON_VALUE(`after`.data, '$.policyholder.date_of_birth') AS DATE),
    JSON_VALUE(`after`.data, '$.policyholder.contact.email'),
    JSON_VALUE(`after`.data, '$.policyholder.contact.phone'),
    JSON_VALUE(`after`.data, '$.policyholder.contact.address.street'),
    JSON_VALUE(`after`.data, '$.policyholder.contact.address.city'),
    JSON_VALUE(`after`.data, '$.policyholder.contact.address.state'),
    JSON_VALUE(`after`.data, '$.policyholder.contact.address.zip'),
    CAST(TO_TIMESTAMP_LTZ(ts_ms, 3) AS TIMESTAMP(3))
FROM kafka_policy_source
WHERE op IN ('c', 'r', 'u');

-- ----- output_coverage: array with UNION ALL index access (max 5 coverages) -----
INSERT INTO sink_coverage
SELECT policy_id, coverage_type, coverage_limit, deductible, premium
FROM (
    SELECT `after`.id AS policy_id,
           JSON_VALUE(`after`.data, '$.coverages[0].type') AS coverage_type,
           CAST(JSON_VALUE(`after`.data, '$.coverages[0].limit') AS DECIMAL(12,2)) AS coverage_limit,
           CAST(JSON_VALUE(`after`.data, '$.coverages[0].deductible') AS DECIMAL(12,2)) AS deductible,
           CAST(JSON_VALUE(`after`.data, '$.coverages[0].premium') AS DECIMAL(12,2)) AS premium
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.coverages[1].type'),
           CAST(JSON_VALUE(`after`.data, '$.coverages[1].limit') AS DECIMAL(12,2)),
           CAST(JSON_VALUE(`after`.data, '$.coverages[1].deductible') AS DECIMAL(12,2)),
           CAST(JSON_VALUE(`after`.data, '$.coverages[1].premium') AS DECIMAL(12,2))
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.coverages[2].type'),
           CAST(JSON_VALUE(`after`.data, '$.coverages[2].limit') AS DECIMAL(12,2)),
           CAST(JSON_VALUE(`after`.data, '$.coverages[2].deductible') AS DECIMAL(12,2)),
           CAST(JSON_VALUE(`after`.data, '$.coverages[2].premium') AS DECIMAL(12,2))
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.coverages[3].type'),
           CAST(JSON_VALUE(`after`.data, '$.coverages[3].limit') AS DECIMAL(12,2)),
           CAST(JSON_VALUE(`after`.data, '$.coverages[3].deductible') AS DECIMAL(12,2)),
           CAST(JSON_VALUE(`after`.data, '$.coverages[3].premium') AS DECIMAL(12,2))
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.coverages[4].type'),
           CAST(JSON_VALUE(`after`.data, '$.coverages[4].limit') AS DECIMAL(12,2)),
           CAST(JSON_VALUE(`after`.data, '$.coverages[4].deductible') AS DECIMAL(12,2)),
           CAST(JSON_VALUE(`after`.data, '$.coverages[4].premium') AS DECIMAL(12,2))
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
) AS t
WHERE coverage_type IS NOT NULL;

-- ----- output_vehicle: array with UNION ALL index access (max 3 vehicles) -----
INSERT INTO sink_vehicle
SELECT policy_id, vin, year_made, make, model
FROM (
    SELECT `after`.id AS policy_id,
           JSON_VALUE(`after`.data, '$.vehicles[0].vin') AS vin,
           CAST(JSON_VALUE(`after`.data, '$.vehicles[0].year') AS INT) AS year_made,
           JSON_VALUE(`after`.data, '$.vehicles[0].make') AS make,
           JSON_VALUE(`after`.data, '$.vehicles[0].model') AS model
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[1].vin'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[1].year') AS INT),
           JSON_VALUE(`after`.data, '$.vehicles[1].make'),
           JSON_VALUE(`after`.data, '$.vehicles[1].model')
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[2].vin'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[2].year') AS INT),
           JSON_VALUE(`after`.data, '$.vehicles[2].make'),
           JSON_VALUE(`after`.data, '$.vehicles[2].model')
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
) AS t
WHERE vin IS NOT NULL;

-- ----- output_driver: double-indexed array (vehicles[i].drivers[j]) -----
-- Covers up to 3 vehicles x 3 drivers = 9 combinations
INSERT INTO sink_driver
SELECT policy_id, vehicle_vin, driver_name, license_number, is_primary
FROM (
    -- vehicle[0].driver[0]
    SELECT `after`.id AS policy_id,
           JSON_VALUE(`after`.data, '$.vehicles[0].vin') AS vehicle_vin,
           JSON_VALUE(`after`.data, '$.vehicles[0].drivers[0].name') AS driver_name,
           JSON_VALUE(`after`.data, '$.vehicles[0].drivers[0].license_number') AS license_number,
           CAST(JSON_VALUE(`after`.data, '$.vehicles[0].drivers[0].is_primary') AS BOOLEAN) AS is_primary
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    -- vehicle[0].driver[1]
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[0].vin'),
           JSON_VALUE(`after`.data, '$.vehicles[0].drivers[1].name'),
           JSON_VALUE(`after`.data, '$.vehicles[0].drivers[1].license_number'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[0].drivers[1].is_primary') AS BOOLEAN)
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    -- vehicle[0].driver[2]
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[0].vin'),
           JSON_VALUE(`after`.data, '$.vehicles[0].drivers[2].name'),
           JSON_VALUE(`after`.data, '$.vehicles[0].drivers[2].license_number'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[0].drivers[2].is_primary') AS BOOLEAN)
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    -- vehicle[1].driver[0]
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[1].vin'),
           JSON_VALUE(`after`.data, '$.vehicles[1].drivers[0].name'),
           JSON_VALUE(`after`.data, '$.vehicles[1].drivers[0].license_number'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[1].drivers[0].is_primary') AS BOOLEAN)
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    -- vehicle[1].driver[1]
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[1].vin'),
           JSON_VALUE(`after`.data, '$.vehicles[1].drivers[1].name'),
           JSON_VALUE(`after`.data, '$.vehicles[1].drivers[1].license_number'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[1].drivers[1].is_primary') AS BOOLEAN)
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    -- vehicle[1].driver[2]
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[1].vin'),
           JSON_VALUE(`after`.data, '$.vehicles[1].drivers[2].name'),
           JSON_VALUE(`after`.data, '$.vehicles[1].drivers[2].license_number'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[1].drivers[2].is_primary') AS BOOLEAN)
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    -- vehicle[2].driver[0]
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[2].vin'),
           JSON_VALUE(`after`.data, '$.vehicles[2].drivers[0].name'),
           JSON_VALUE(`after`.data, '$.vehicles[2].drivers[0].license_number'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[2].drivers[0].is_primary') AS BOOLEAN)
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    -- vehicle[2].driver[1]
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[2].vin'),
           JSON_VALUE(`after`.data, '$.vehicles[2].drivers[1].name'),
           JSON_VALUE(`after`.data, '$.vehicles[2].drivers[1].license_number'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[2].drivers[1].is_primary') AS BOOLEAN)
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    -- vehicle[2].driver[2]
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.vehicles[2].vin'),
           JSON_VALUE(`after`.data, '$.vehicles[2].drivers[2].name'),
           JSON_VALUE(`after`.data, '$.vehicles[2].drivers[2].license_number'),
           CAST(JSON_VALUE(`after`.data, '$.vehicles[2].drivers[2].is_primary') AS BOOLEAN)
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
) AS t
WHERE license_number IS NOT NULL;

-- ----- output_claim: array with UNION ALL index access (max 5 claims) -----
INSERT INTO sink_claim
SELECT policy_id, claim_id, claim_date, amount, status, description
FROM (
    SELECT `after`.id AS policy_id,
           JSON_VALUE(`after`.data, '$.claims_history[0].claim_id') AS claim_id,
           CAST(JSON_VALUE(`after`.data, '$.claims_history[0].date') AS DATE) AS claim_date,
           CAST(JSON_VALUE(`after`.data, '$.claims_history[0].amount') AS DECIMAL(12,2)) AS amount,
           JSON_VALUE(`after`.data, '$.claims_history[0].status') AS status,
           JSON_VALUE(`after`.data, '$.claims_history[0].description') AS description
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.claims_history[1].claim_id'),
           CAST(JSON_VALUE(`after`.data, '$.claims_history[1].date') AS DATE),
           CAST(JSON_VALUE(`after`.data, '$.claims_history[1].amount') AS DECIMAL(12,2)),
           JSON_VALUE(`after`.data, '$.claims_history[1].status'),
           JSON_VALUE(`after`.data, '$.claims_history[1].description')
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.claims_history[2].claim_id'),
           CAST(JSON_VALUE(`after`.data, '$.claims_history[2].date') AS DATE),
           CAST(JSON_VALUE(`after`.data, '$.claims_history[2].amount') AS DECIMAL(12,2)),
           JSON_VALUE(`after`.data, '$.claims_history[2].status'),
           JSON_VALUE(`after`.data, '$.claims_history[2].description')
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.claims_history[3].claim_id'),
           CAST(JSON_VALUE(`after`.data, '$.claims_history[3].date') AS DATE),
           CAST(JSON_VALUE(`after`.data, '$.claims_history[3].amount') AS DECIMAL(12,2)),
           JSON_VALUE(`after`.data, '$.claims_history[3].status'),
           JSON_VALUE(`after`.data, '$.claims_history[3].description')
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
    UNION ALL
    SELECT `after`.id,
           JSON_VALUE(`after`.data, '$.claims_history[4].claim_id'),
           CAST(JSON_VALUE(`after`.data, '$.claims_history[4].date') AS DATE),
           CAST(JSON_VALUE(`after`.data, '$.claims_history[4].amount') AS DECIMAL(12,2)),
           JSON_VALUE(`after`.data, '$.claims_history[4].status'),
           JSON_VALUE(`after`.data, '$.claims_history[4].description')
    FROM kafka_policy_source WHERE op IN ('c', 'r', 'u')
) AS t
WHERE claim_id IS NOT NULL;

END;

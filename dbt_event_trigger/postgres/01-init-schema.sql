-- Source table
CREATE TABLE policy (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- Output tables (dbt will write to these via incremental merge)
-- =============================================================================

CREATE TABLE output_policy (
    policy_id BIGINT,
    policy_number TEXT,
    status TEXT,
    effective_date DATE,
    expiration_date DATE,
    holder_first_name TEXT,
    holder_last_name TEXT,
    holder_dob DATE,
    holder_email TEXT,
    holder_phone TEXT,
    holder_street TEXT,
    holder_city TEXT,
    holder_state TEXT,
    holder_zip TEXT,
    source_event_time TIMESTAMP,
    PRIMARY KEY (policy_id)
);

CREATE TABLE output_coverage (
    policy_id BIGINT,
    coverage_type TEXT,
    coverage_limit NUMERIC,
    deductible NUMERIC,
    premium NUMERIC,
    PRIMARY KEY (policy_id, coverage_type)
);

CREATE TABLE output_vehicle (
    policy_id BIGINT,
    vin TEXT,
    year_made INT,
    make TEXT,
    model TEXT,
    PRIMARY KEY (policy_id, vin)
);

CREATE TABLE output_driver (
    policy_id BIGINT,
    vehicle_vin TEXT,
    driver_name TEXT,
    license_number TEXT,
    is_primary BOOLEAN,
    PRIMARY KEY (policy_id, vehicle_vin, license_number)
);

CREATE TABLE output_claim (
    policy_id BIGINT,
    claim_id TEXT,
    claim_date DATE,
    amount NUMERIC,
    status TEXT,
    description TEXT,
    PRIMARY KEY (policy_id, claim_id)
);

-- =============================================================================
-- Staging tables (Flink writes here in append-only mode)
-- Each mirrors its output table + op (CDC operation) + event_time
-- =============================================================================

CREATE TABLE stg_policy (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    policy_number TEXT,
    status TEXT,
    effective_date DATE,
    expiration_date DATE,
    holder_first_name TEXT,
    holder_last_name TEXT,
    holder_dob DATE,
    holder_email TEXT,
    holder_phone TEXT,
    holder_street TEXT,
    holder_city TEXT,
    holder_state TEXT,
    holder_zip TEXT,
    source_event_time TIMESTAMP,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE stg_coverage (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    coverage_type TEXT,
    coverage_limit NUMERIC,
    deductible NUMERIC,
    premium NUMERIC,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE stg_vehicle (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    vin TEXT,
    year_made INT,
    make TEXT,
    model TEXT,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE stg_driver (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    vehicle_vin TEXT,
    driver_name TEXT,
    license_number TEXT,
    is_primary BOOLEAN,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE stg_claim (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    claim_id TEXT,
    claim_date DATE,
    amount NUMERIC,
    status TEXT,
    description TEXT,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

-- Enable full replica identity for CDC
ALTER TABLE policy REPLICA IDENTITY FULL;

-- =============================================================================
-- NOTIFY triggers: fire on INSERT to any stg_* table
-- Used by the event-driven dbt trigger service
-- =============================================================================

CREATE OR REPLACE FUNCTION notify_staging_insert()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('stg_data_arrived', TG_TABLE_NAME);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER stg_policy_notify
    AFTER INSERT ON stg_policy
    FOR EACH STATEMENT EXECUTE FUNCTION notify_staging_insert();

CREATE TRIGGER stg_coverage_notify
    AFTER INSERT ON stg_coverage
    FOR EACH STATEMENT EXECUTE FUNCTION notify_staging_insert();

CREATE TRIGGER stg_vehicle_notify
    AFTER INSERT ON stg_vehicle
    FOR EACH STATEMENT EXECUTE FUNCTION notify_staging_insert();

CREATE TRIGGER stg_driver_notify
    AFTER INSERT ON stg_driver
    FOR EACH STATEMENT EXECUTE FUNCTION notify_staging_insert();

CREATE TRIGGER stg_claim_notify
    AFTER INSERT ON stg_claim
    FOR EACH STATEMENT EXECUTE FUNCTION notify_staging_insert();

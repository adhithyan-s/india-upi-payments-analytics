-- ============================================================
-- UPI Payments Analytics — PostgreSQL Schema Initialisation
-- ============================================================
-- Runs automatically on first docker-compose up via docker-entrypoint-initdb.d
--
-- schema separation mirrors medallion layers
-- raw     = Silver layer  (cleaned records, loaded from MinIO)
-- staging = dbt staging   (type-cast, standardised)
-- intermediate = dbt intermediate (enriched, joined)
-- marts   = Gold layer    (star schema — what Grafana queries)

-- Raw schema — Silver layer
-- Individual cleaned transaction records
-- Loaded by loader/load_to_postgres.py from MinIO Parquet files
CREATE SCHEMA IF NOT EXISTS raw;

-- dbt transformation schemas — Gold layer
-- Created and owned by dbt — do not manually insert here
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- ============================================================
-- Raw transactions table — Silver layer
-- This is schema-on-write for the warehouse
-- Every column has an explicit type — PostgreSQL enforces it
-- Contrast with Bronze (MinIO Parquet) which is schema-on-read
-- ============================================================
CREATE TABLE IF NOT EXISTS raw.transactions (
    txn_id              VARCHAR(50)     NOT NULL,
    amount_inr          DECIMAL(12, 2)  NOT NULL,
    city                VARCHAR(100)    NOT NULL,
    state               VARCHAR(100)    NOT NULL,
    merchant_category   VARCHAR(100)    NOT NULL,
    upi_app             VARCHAR(50)     NOT NULL,
    payment_type        VARCHAR(50)     NOT NULL,
    status              VARCHAR(20)     NOT NULL,
    failure_reason      VARCHAR(200),               -- nullable: null for SUCCESS
    txn_date            DATE            NOT NULL,
    txn_hour            SMALLINT        NOT NULL,
    is_weekend          BOOLEAN         NOT NULL,
    sender_upi          VARCHAR(100)    NOT NULL,
    receiver_upi        VARCHAR(100)    NOT NULL,
    device_type         VARCHAR(50)     NOT NULL,
    created_at          TIMESTAMPTZ     NOT NULL,
    loaded_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Index on txn_id for fast deduplication queries
-- dbt's stg_transactions.sql does ROW_NUMBER() OVER (PARTITION BY txn_id)
-- This index makes that partition scan significantly faster
CREATE INDEX IF NOT EXISTS idx_raw_txns_txn_id
    ON raw.transactions (txn_id);

-- Index on txn_date for partition-style filtering in dbt models
CREATE INDEX IF NOT EXISTS idx_raw_txns_txn_date
    ON raw.transactions (txn_date);

-- Index on loaded_at for incremental dbt model lookback window
-- dbt incremental model filters WHERE loaded_at > max(loaded_at)
-- This index makes that filter O(log n) instead of O(n)
CREATE INDEX IF NOT EXISTS idx_raw_txns_loaded_at
    ON raw.transactions (loaded_at);

-- ============================================================
-- Grant permissions to dbt (runs as the same user here,
-- but in production dbt would have a separate read/write role)
-- ============================================================
GRANT ALL PRIVILEGES ON SCHEMA raw TO upiuser;
GRANT ALL PRIVILEGES ON SCHEMA staging TO upiuser;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO upiuser;
GRANT ALL PRIVILEGES ON SCHEMA marts TO upiuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO upiuser;
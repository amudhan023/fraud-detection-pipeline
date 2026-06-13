-- Schema and seed data for the fraud detection pipeline.
-- Run automatically by the Postgres container on first start.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ─── Core Tables ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS accounts (
    account_id      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    owner_name      VARCHAR(255)      NOT NULL,
    tier            VARCHAR(20)       NOT NULL DEFAULT 'STANDARD',
    country         CHAR(2)           NOT NULL,
    credit_limit    NUMERIC(12,2)     NOT NULL DEFAULT 10000.00,
    risk_score      DOUBLE PRECISION  NOT NULL DEFAULT 0.0,
    created_at      TIMESTAMPTZ       NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id     UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            VARCHAR(255)  NOT NULL,
    category        VARCHAR(100)  NOT NULL,
    risk_category   VARCHAR(10)   NOT NULL DEFAULT 'LOW',
    country         CHAR(2)       NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id      UUID          NOT NULL REFERENCES accounts(account_id),
    merchant_id     UUID          NOT NULL REFERENCES merchants(merchant_id),
    amount          NUMERIC(12,2) NOT NULL,
    currency        CHAR(3)       NOT NULL DEFAULT 'USD',
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    status          VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
    event_time      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- ─── Analytics Sink Tables ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS scored_transactions (
    transaction_id  UUID PRIMARY KEY,
    account_id      UUID              NOT NULL,
    merchant_id     UUID              NOT NULL,
    amount          NUMERIC(12,2)     NOT NULL,
    risk_score      DOUBLE PRECISION  NOT NULL,
    score_breakdown JSONB,
    event_time      TIMESTAMPTZ       NOT NULL,
    processed_at    TIMESTAMPTZ       NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS anomaly_events (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id  UUID        NOT NULL,
    account_id      UUID        NOT NULL,
    reason_code     VARCHAR(50) NOT NULL,
    evidence        JSONB,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS spend_aggregates (
    id              BIGSERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ   NOT NULL,
    window_end      TIMESTAMPTZ   NOT NULL,
    dimension       VARCHAR(20)   NOT NULL,
    dimension_value VARCHAR(255)  NOT NULL,
    total_amount    NUMERIC(14,2) NOT NULL DEFAULT 0,
    txn_count       BIGINT        NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (window_start, window_end, dimension, dimension_value)
);

-- ─── Indexes ──────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_transactions_account_id  ON transactions (account_id);
CREATE INDEX IF NOT EXISTS idx_transactions_event_time  ON transactions (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly_account_id       ON anomaly_events (account_id);
CREATE INDEX IF NOT EXISTS idx_anomaly_detected_at      ON anomaly_events (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_spend_dimension          ON spend_aggregates (dimension, dimension_value, window_start DESC);

-- ─── Logical Replication Setup ────────────────────────────────────────────────

-- Idempotent replication slot creation
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_fraud_slot'
    ) THEN
        PERFORM pg_create_logical_replication_slot('debezium_fraud_slot', 'pgoutput');
    END IF;
END;
$$;

-- Idempotent publication creation
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication'
    ) THEN
        CREATE PUBLICATION dbz_publication FOR TABLE transactions, accounts, merchants;
    END IF;
END;
$$;

-- ─── Seed Data ────────────────────────────────────────────────────────────────

INSERT INTO merchants (merchant_id, name, category, risk_category, country) VALUES
    ('a1b2c3d4-0001-0001-0001-000000000001', 'Amazon',            'RETAIL',  'LOW',    'US'),
    ('a1b2c3d4-0001-0001-0001-000000000002', 'Delta Airlines',    'TRAVEL',  'MEDIUM', 'US'),
    ('a1b2c3d4-0001-0001-0001-000000000003', 'Starbucks',         'FOOD',    'LOW',    'US'),
    ('a1b2c3d4-0001-0001-0001-000000000004', 'CryptoExchange X',  'CRYPTO',  'HIGH',   'KY'),
    ('a1b2c3d4-0001-0001-0001-000000000005', 'Shell Gas',         'FUEL',    'LOW',    'US'),
    ('a1b2c3d4-0001-0001-0001-000000000006', 'Best Buy',          'RETAIL',  'LOW',    'US'),
    ('a1b2c3d4-0001-0001-0001-000000000007', 'AirBnb',            'TRAVEL',  'MEDIUM', 'US'),
    ('a1b2c3d4-0001-0001-0001-000000000008', 'Dark Market XYZ',   'UNKNOWN', 'HIGH',   'RU')
ON CONFLICT (merchant_id) DO NOTHING;

INSERT INTO accounts (account_id, owner_name, tier, country, credit_limit, risk_score) VALUES
    ('b2c3d4e5-0001-0001-0001-000000000001', 'Alice Chen',   'PREMIUM',  'US', 25000.00,  0.10),
    ('b2c3d4e5-0001-0001-0001-000000000002', 'Bob Smith',    'STANDARD', 'US',  5000.00,  0.20),
    ('b2c3d4e5-0001-0001-0001-000000000003', 'Carol White',  'VIP',      'GB', 100000.00, 0.05),
    ('b2c3d4e5-0001-0001-0001-000000000004', 'Dave Jones',   'STANDARD', 'US',  3000.00,  0.50),
    ('b2c3d4e5-0001-0001-0001-000000000005', 'Eve Kumar',    'PREMIUM',  'IN', 15000.00,  0.15)
ON CONFLICT (account_id) DO NOTHING;

-- Raw tables: provider-shaped, wide, replaceable. All have provider, asof_loaded_at, source_hash.

-- SimFin quarterly fundamentals
CREATE TABLE IF NOT EXISTS raw.raw_simfin_income_q (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'simfin',
    asof_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash TEXT,
    payload JSONB,
    ticker TEXT,
    simfin_id INTEGER,
    period DATE,
    report_date DATE,
    revenue BIGINT,
    cost_of_revenue BIGINT,
    gross_profit BIGINT,
    operating_expenses BIGINT,
    operating_income BIGINT,
    net_income BIGINT,
    UNIQUE (ticker, period, source_hash)
);

CREATE TABLE IF NOT EXISTS raw.raw_simfin_balance_q (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'simfin',
    asof_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash TEXT,
    payload JSONB,
    ticker TEXT,
    simfin_id INTEGER,
    period DATE,
    report_date DATE,
    total_assets BIGINT,
    total_liabilities BIGINT,
    total_equity BIGINT,
    cash_and_equivalents BIGINT,
    total_debt BIGINT,
    UNIQUE (ticker, period, source_hash)
);

CREATE TABLE IF NOT EXISTS raw.raw_simfin_cashflow_q (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'simfin',
    asof_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash TEXT,
    payload JSONB,
    ticker TEXT,
    simfin_id INTEGER,
    period DATE,
    report_date DATE,
    operating_cashflow BIGINT,
    investing_cashflow BIGINT,
    financing_cashflow BIGINT,
    free_cashflow BIGINT,
    UNIQUE (ticker, period, source_hash)
);

CREATE TABLE IF NOT EXISTS raw.raw_simfin_shares_q (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'simfin',
    asof_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash TEXT,
    payload JSONB,
    ticker TEXT,
    simfin_id INTEGER,
    period DATE,
    report_date DATE,
    shares_basic BIGINT,
    shares_diluted BIGINT,
    UNIQUE (ticker, period, source_hash)
);

CREATE TABLE IF NOT EXISTS raw.raw_simfin_derived_metrics_q (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'simfin',
    asof_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash TEXT,
    payload JSONB,
    ticker TEXT,
    simfin_id INTEGER,
    period DATE,
    report_date DATE,
    UNIQUE (ticker, period, source_hash)
);

-- Daily prices (SimFin for MVP)
CREATE TABLE IF NOT EXISTS raw.raw_prices_daily (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'simfin',
    asof_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash TEXT,
    payload JSONB,
    ticker TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    UNIQUE (ticker, trade_date, source_hash)
);

-- SEC company facts (JSON per CIK)
CREATE TABLE IF NOT EXISTS raw.raw_sec_companyfacts (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'sec',
    asof_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash TEXT,
    payload JSONB NOT NULL,
    cik TEXT NOT NULL,
    entity_name TEXT,
    UNIQUE (cik, source_hash)
);

-- SEC submissions (filing history per CIK)
CREATE TABLE IF NOT EXISTS raw.raw_sec_submissions (
    id SERIAL PRIMARY KEY,
    provider TEXT NOT NULL DEFAULT 'sec',
    asof_loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source_hash TEXT,
    payload JSONB NOT NULL,
    cik TEXT NOT NULL,
    UNIQUE (cik, source_hash)
);

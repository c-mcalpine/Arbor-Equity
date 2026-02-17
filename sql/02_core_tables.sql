-- Core tables: normalized, stable contract to the app.

CREATE TABLE IF NOT EXISTS core.core_security_master (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    cik TEXT,
    name TEXT,
    exchange TEXT,
    currency TEXT DEFAULT 'USD',
    sector TEXT,
    industry TEXT,
    simfin_id INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.core_prices_daily (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL REFERENCES core.core_security_master(id),
    trade_date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC NOT NULL,
    volume BIGINT,
    UNIQUE (security_id, trade_date)
);

CREATE TABLE IF NOT EXISTS core.core_fundamentals_quarterly (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL REFERENCES core.core_security_master(id),
    period_end DATE NOT NULL,
    report_date DATE,
    revenue BIGINT,
    gross_profit BIGINT,
    operating_income BIGINT,
    net_income BIGINT,
    total_assets BIGINT,
    total_liabilities BIGINT,
    total_equity BIGINT,
    cash_and_equivalents BIGINT,
    total_debt BIGINT,
    operating_cashflow BIGINT,
    free_cashflow BIGINT,
    shares_diluted BIGINT,
    UNIQUE (security_id, period_end)
);

CREATE TABLE IF NOT EXISTS core.core_events_earnings (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL REFERENCES core.core_security_master(id),
    event_date DATE,
    event_time TIME,
    fiscal_period TEXT,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.core_estimates (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL REFERENCES core.core_security_master(id),
    as_of_date DATE NOT NULL,
    period_label TEXT,
    revenue_est BIGINT,
    eps_est NUMERIC,
    source TEXT DEFAULT 'manual',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (security_id, as_of_date, period_label)
);

CREATE TABLE IF NOT EXISTS core.core_peer_sets (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    security_ids INTEGER[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.core_thesis (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL REFERENCES core.core_security_master(id),
    thesis_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.core_thesis_log (
    id SERIAL PRIMARY KEY,
    thesis_id INTEGER NOT NULL REFERENCES core.core_thesis(id),
    log_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.core_positions (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL REFERENCES core.core_security_master(id),
    weight NUMERIC NOT NULL,
    target_weight NUMERIC,
    constraints JSONB,
    as_of_date DATE NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE (security_id, as_of_date)
);

CREATE TABLE IF NOT EXISTS core.core_scenarios (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL REFERENCES core.core_security_master(id),
    scenario TEXT NOT NULL,
    price_target NUMERIC,
    probability NUMERIC,
    as_of_date DATE NOT NULL,
    source TEXT DEFAULT 'analyst',
    UNIQUE (security_id, scenario, as_of_date)
);

-- Benchmarks stored as synthetic securities (ticker like SPYTR, QQQ, TB3M) or we use a small benchmark table
CREATE TABLE IF NOT EXISTS core.core_benchmarks (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT
);

CREATE TABLE IF NOT EXISTS core.core_benchmark_prices_daily (
    id SERIAL PRIMARY KEY,
    benchmark_id INTEGER NOT NULL REFERENCES core.core_benchmarks(id),
    trade_date DATE NOT NULL,
    close NUMERIC NOT NULL,
    total_return_index NUMERIC,
    UNIQUE (benchmark_id, trade_date)
);

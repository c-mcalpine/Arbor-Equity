-- Feat tables: computed features for fast UI.

CREATE TABLE IF NOT EXISTS feat.feat_returns (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL,
    as_of_date DATE NOT NULL,
    return_24h NUMERIC,
    return_7d NUMERIC,
    return_mtd NUMERIC,
    return_qtd NUMERIC,
    return_ytd NUMERIC,
    rolling_vol_20d NUMERIC,
    drawdown NUMERIC,
    UNIQUE (security_id, as_of_date)
);

CREATE TABLE IF NOT EXISTS feat.feat_portfolio (
    id SERIAL PRIMARY KEY,
    as_of_date DATE NOT NULL UNIQUE,
    return_24h NUMERIC,
    return_7d NUMERIC,
    return_mtd NUMERIC,
    return_qtd NUMERIC,
    return_ytd NUMERIC,
    alpha_vs_sp500_24h NUMERIC,
    alpha_vs_sp500_7d NUMERIC,
    alpha_vs_sp500_mtd NUMERIC,
    alpha_vs_sp500_qtd NUMERIC,
    alpha_vs_sp500_ytd NUMERIC,
    alpha_vs_nasdaq_24h NUMERIC,
    alpha_vs_nasdaq_7d NUMERIC,
    alpha_vs_nasdaq_mtd NUMERIC,
    alpha_vs_nasdaq_qtd NUMERIC,
    alpha_vs_nasdaq_ytd NUMERIC,
    alpha_vs_tbill_24h NUMERIC,
    alpha_vs_tbill_7d NUMERIC,
    alpha_vs_tbill_mtd NUMERIC,
    alpha_vs_tbill_qtd NUMERIC,
    alpha_vs_tbill_ytd NUMERIC
);

CREATE TABLE IF NOT EXISTS feat.feat_valuation (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL,
    as_of_date DATE NOT NULL,
    ev_sales_fy1 NUMERIC,
    ev_fcf NUMERIC,
    pct_historical NUMERIC,
    UNIQUE (security_id, as_of_date)
);

CREATE TABLE IF NOT EXISTS feat.feat_revisions (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL,
    as_of_date DATE NOT NULL,
    estimate_change NUMERIC,
    revision_velocity NUMERIC,
    UNIQUE (security_id, as_of_date)
);

CREATE TABLE IF NOT EXISTS feat.feat_rpo (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL,
    period_end DATE NOT NULL,
    rpo BIGINT,
    crpo BIGINT,
    coverage_flag BOOLEAN DEFAULT FALSE,
    UNIQUE (security_id, period_end)
);

CREATE TABLE IF NOT EXISTS feat.feat_risk (
    id SERIAL PRIMARY KEY,
    security_id INTEGER NOT NULL,
    as_of_date DATE NOT NULL,
    beta_sp500 NUMERIC,
    beta_nasdaq NUMERIC,
    correlation_cluster_id INTEGER,
    UNIQUE (security_id, as_of_date)
);

CREATE TABLE IF NOT EXISTS feat.feat_benchmark_returns (
    id SERIAL PRIMARY KEY,
    benchmark_id INTEGER NOT NULL,
    as_of_date DATE NOT NULL,
    return_24h NUMERIC,
    return_7d NUMERIC,
    return_mtd NUMERIC,
    return_qtd NUMERIC,
    return_ytd NUMERIC,
    total_return_index NUMERIC,
    UNIQUE (benchmark_id, as_of_date)
);

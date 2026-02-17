"""
Ingest SimFin data: load into raw_* tables then upsert into core_fundamentals_quarterly
and core_prices_daily. Uses SimFin Python API (free tier) or bulk CSV.
"""
import os
import sys
import hashlib
import json
from datetime import datetime

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from models.db import get_connection

def _source_hash(rows: list, keys: list) -> str:
    raw = json.dumps(rows, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:32]

def _ensure_simfin_config():
    try:
        import simfin
        api_key = os.getenv("SIMFIN_API_KEY")
        if api_key:
            simfin.set_api_key(api_key)
        simfin.set_data_dir(os.path.join(ROOT, "data", "simfin"))
        os.makedirs(simfin.get_data_dir(), exist_ok=True)
    except Exception:
        pass

def load_simfin_fundamentals(tickers: list) -> tuple:
    """Load income, balance, cashflow, shares from SimFin. Returns (income_df, balance_df, cashflow_df, shares_df) or None for missing."""
    _ensure_simfin_config()
    income_df = balance_df = cashflow_df = shares_df = None
    try:
        import simfin
        from simfin.names import TICKER, REPORT_DATE, PERIOD_END_DATE
        # Load datasets; filter to our tickers
        for name, loader in [
            ("income", getattr(simfin, "load_income", None) or getattr(simfin.load, "load_income", None)),
            ("balance", getattr(simfin, "load_balance", None) or getattr(simfin.load, "load_balance", None)),
            ("cashflow", getattr(simfin, "load_cashflow", None) or getattr(simfin.load, "load_cashflow", None)),
            ("shares", getattr(simfin, "load_shareprices", None) or getattr(simfin.load, "load_shareprices", None)),
        ]:
            if loader is None:
                continue
            try:
                df = loader(variant="quarterly") if name != "shares" else loader()
                if df is not None and not df.empty and TICKER in df.columns:
                    df = df[df[TICKER].isin(tickers)].copy()
                    if name == "income":
                        income_df = df
                    elif name == "balance":
                        balance_df = df
                    elif name == "cashflow":
                        cashflow_df = df
                    elif name == "shares":
                        shares_df = df
            except Exception:
                pass
    except ImportError:
        pass
    return income_df, balance_df, cashflow_df, shares_df

def load_simfin_prices(tickers: list) -> pd.DataFrame:
    _ensure_simfin_config()
    try:
        import simfin
        loader = getattr(simfin, "load_shareprices", None) or getattr(simfin.load, "load_shareprices", None)
        if loader is None:
            return pd.DataFrame()
        df = loader()
        if df is not None and not df.empty and "Ticker" in df.columns:
            return df[df["Ticker"].isin(tickers)].copy()
        if df is not None and not df.empty and "ticker" in df.columns:
            return df[df["ticker"].isin(tickers)].copy()
        return df.copy() if df is not None else pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def job_ingest_simfin(tickers: list = None):
    if tickers is None:
        from config.tickers import DEFAULT_TICKERS
        tickers = DEFAULT_TICKERS

    conn = get_connection()
    asof = datetime.utcnow()
    provider = "simfin"

    # --- Prices (most important for MVP) ---
    prices_df = load_simfin_prices(tickers)
    if not prices_df.empty:
        # Normalize column names (SimFin uses Title Case sometimes)
        col_map = {c: c.lower() for c in prices_df.columns}
        prices_df = prices_df.rename(columns=col_map)
        date_col = "date" if "date" in prices_df.columns else "trade date"
        ticker_col = "ticker" if "ticker" in prices_df.columns else "Ticker"
        if date_col not in prices_df.columns and "trade date" in prices_df.columns:
            date_col = "trade date"
        if ticker_col not in prices_df.columns:
            ticker_col = [c for c in prices_df.columns if "tick" in c.lower()][0] if any("tick" in c.lower() for c in prices_df.columns) else prices_df.columns[0]
        for _, row in prices_df.iterrows():
            ticker = str(row.get(ticker_col, "")).strip()
            dt = row.get(date_col)
            if pd.isna(dt):
                continue
            d = pd.to_datetime(dt).date() if hasattr(dt, "date") else dt
            source_hash = hashlib.sha256(f"{ticker}|{d}|{row.get('close', row.get('Close', ''))}".encode()).hexdigest()[:32]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.raw_prices_daily (provider, asof_loaded_at, source_hash, ticker, trade_date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, trade_date, source_hash) DO NOTHING
                    """,
                    (
                        provider,
                        asof,
                        source_hash,
                        ticker,
                        d,
                        row.get("open", row.get("Open")),
                        row.get("high", row.get("High")),
                        row.get("low", row.get("Low")),
                        row.get("close", row.get("Close")),
                        row.get("volume", row.get("Volume")),
                    ),
                )
        conn.commit()

        # Core prices: upsert from raw by security_id
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO core.core_prices_daily (security_id, trade_date, open, high, low, close, volume)
                SELECT m.id, r.trade_date, r.open, r.high, r.low, r.close, r.volume
                FROM raw.raw_prices_daily r
                JOIN core.core_security_master m ON m.ticker = r.ticker
                WHERE r.provider = %s
                ON CONFLICT (security_id, trade_date) DO UPDATE SET
                    open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                    close = EXCLUDED.close, volume = EXCLUDED.volume
            """, (provider,))
        conn.commit()

    # --- Fundamentals (income, balance, cashflow, shares) ---
    income_df, balance_df, cashflow_df, shares_df = load_simfin_fundamentals(tickers)

    def _quarterly_to_raw(df, table: str, row_to_vals):
        if df is None or df.empty:
            return
        for _, row in df.iterrows():
            vals = row_to_vals(row)
            if vals is None:
                continue
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO raw.{table} (provider, asof_loaded_at, source_hash, payload, {", ".join(v[0] for v in vals)})
                    VALUES (%s, %s, %s, %s, {", ".join("%s" for _ in vals)})
                    ON CONFLICT DO NOTHING
                    """,
                    (provider, asof, _source_hash([row.to_dict()], ["ticker", "period"]), json.dumps(row.to_dict(), default=str), *[v[1] for v in vals]),
                )
        conn.commit()

    # Map SimFin column names (they use mixed case)
    def _num(x):
        if pd.isna(x):
            return None
        try:
            return int(float(x))
        except (ValueError, TypeError):
            return None

    if income_df is not None and not income_df.empty:
        ticker_col = "Ticker" if "Ticker" in income_df.columns else "ticker"
        period_col = "Report Date" if "Report Date" in income_df.columns else "Period End Date" if "Period End Date" in income_df.columns else "period"
        for _, row in income_df.iterrows():
            ticker = str(row.get(ticker_col, "")).strip()
            period = row.get(period_col)
            if pd.isna(period):
                continue
            period = pd.to_datetime(period).date() if hasattr(period, "date") else period
            sh = _source_hash([{"t": ticker, "p": str(period)}], ["t", "p"])
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.raw_simfin_income_q (provider, asof_loaded_at, source_hash, ticker, period, revenue, net_income)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, period, source_hash) DO NOTHING
                    """,
                    (provider, asof, sh, ticker, period, _num(row.get("Revenue", row.get("revenue"))), _num(row.get("Net Income", row.get("net_income")))),
                )
        conn.commit()

    if balance_df is not None and not balance_df.empty:
        ticker_col = "Ticker" if "Ticker" in balance_df.columns else "ticker"
        period_col = "Report Date" if "Report Date" in balance_df.columns else "Period End Date" if "Period End Date" in balance_df.columns else "period"
        for _, row in balance_df.iterrows():
            ticker = str(row.get(ticker_col, "")).strip()
            period = row.get(period_col)
            if pd.isna(period):
                continue
            period = pd.to_datetime(period).date() if hasattr(period, "date") else period
            sh = _source_hash([{"t": ticker, "p": str(period)}], ["t", "p"])
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.raw_simfin_balance_q (provider, asof_loaded_at, source_hash, ticker, period, total_assets, total_liabilities, total_equity, cash_and_equivalents, total_debt)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, period, source_hash) DO NOTHING
                    """,
                    (
                        provider, asof, sh, ticker, period,
                        _num(row.get("Total Assets", row.get("total_assets"))),
                        _num(row.get("Total Liabilities", row.get("total_liabilities"))),
                        _num(row.get("Total Equity", row.get("total_equity"))),
                        _num(row.get("Cash and Equivalents", row.get("cash_and_equivalents"))),
                        _num(row.get("Total Debt", row.get("total_debt"))),
                    ),
                )
        conn.commit()

    if cashflow_df is not None and not cashflow_df.empty:
        ticker_col = "Ticker" if "Ticker" in cashflow_df.columns else "ticker"
        period_col = "Report Date" if "Report Date" in cashflow_df.columns else "Period End Date" if "Period End Date" in cashflow_df.columns else "period"
        for _, row in cashflow_df.iterrows():
            ticker = str(row.get(ticker_col, "")).strip()
            period = row.get(period_col)
            if pd.isna(period):
                continue
            period = pd.to_datetime(period).date() if hasattr(period, "date") else period
            sh = _source_hash([{"t": ticker, "p": str(period)}], ["t", "p"])
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.raw_simfin_cashflow_q (provider, asof_loaded_at, source_hash, ticker, period, operating_cashflow, free_cashflow)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, period, source_hash) DO NOTHING
                    """,
                    (provider, asof, sh, ticker, period,
                     _num(row.get("Operating Cash Flow", row.get("operating_cashflow"))),
                     _num(row.get("Free Cash Flow", row.get("free_cashflow")))),
                )
        conn.commit()

    # Upsert core_fundamentals_quarterly from raw (income as driver; attach balance/cashflow/shares by ticker+period)
    with conn.cursor() as cur:
        cur.execute("""
            WITH inc AS (
                SELECT DISTINCT ON (ticker, period) ticker, period, report_date, revenue, net_income
                FROM raw.raw_simfin_income_q WHERE provider = %s ORDER BY ticker, period, asof_loaded_at DESC
            ),
            bal AS (
                SELECT DISTINCT ON (ticker, period) ticker, period, total_assets, total_liabilities, total_equity, cash_and_equivalents, total_debt
                FROM raw.raw_simfin_balance_q WHERE provider = %s ORDER BY ticker, period, asof_loaded_at DESC
            ),
            cf AS (
                SELECT DISTINCT ON (ticker, period) ticker, period, operating_cashflow, free_cashflow
                FROM raw.raw_simfin_cashflow_q WHERE provider = %s ORDER BY ticker, period, asof_loaded_at DESC
            ),
            sh AS (
                SELECT DISTINCT ON (ticker, period) ticker, period, shares_diluted
                FROM raw.raw_simfin_shares_q WHERE provider = %s ORDER BY ticker, period, asof_loaded_at DESC
            )
            INSERT INTO core.core_fundamentals_quarterly (security_id, period_end, report_date, revenue, net_income, total_assets, total_liabilities, total_equity, cash_and_equivalents, total_debt, operating_cashflow, free_cashflow, shares_diluted)
            SELECT m.id, i.period, i.report_date, i.revenue, i.net_income, b.total_assets, b.total_liabilities, b.total_equity, b.cash_and_equivalents, b.total_debt, c.operating_cashflow, c.free_cashflow, s.shares_diluted
            FROM inc i
            JOIN core.core_security_master m ON m.ticker = i.ticker
            LEFT JOIN bal b ON b.ticker = i.ticker AND b.period = i.period
            LEFT JOIN cf c ON c.ticker = i.ticker AND c.period = i.period
            LEFT JOIN sh s ON s.ticker = i.ticker AND s.period = i.period
            ON CONFLICT (security_id, period_end) DO UPDATE SET
                revenue = EXCLUDED.revenue, net_income = EXCLUDED.net_income, total_assets = EXCLUDED.total_assets,
                total_liabilities = EXCLUDED.total_liabilities, total_equity = EXCLUDED.total_equity,
                cash_and_equivalents = EXCLUDED.cash_and_equivalents, total_debt = EXCLUDED.total_debt,
                operating_cashflow = EXCLUDED.operating_cashflow, free_cashflow = EXCLUDED.free_cashflow,
                shares_diluted = EXCLUDED.shares_diluted
        """, (provider, provider, provider, provider))
    conn.commit()
    conn.close()
    return True

if __name__ == "__main__":
    job_ingest_simfin()

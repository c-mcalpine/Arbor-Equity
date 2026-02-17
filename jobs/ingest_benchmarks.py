"""
Load SPY and QQQ prices (e.g. from SimFin) into core.core_benchmark_prices_daily.
TB3M has no free price series; leave null or add a constant later.
"""
import os
import sys

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from models.db import get_connection

def _load_prices_simfin(tickers: list) -> list:
    try:
        import simfin
        api_key = os.getenv("SIMFIN_API_KEY")
        if api_key:
            simfin.set_api_key(api_key)
        simfin.set_data_dir(os.path.join(ROOT, "data", "simfin"))
        loader = getattr(simfin, "load_shareprices", None) or getattr(simfin.load, "load_shareprices", None)
        if loader is None:
            return []
        df = loader()
        if df is None or df.empty:
            return []
        tc = "Ticker" if "Ticker" in df.columns else "ticker"
        df = df[df[tc].isin(tickers)].copy()
        return df.to_dict("records") if not df.empty else []
    except Exception:
        return []

def job_ingest_benchmark_prices():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, ticker FROM core.core_benchmarks WHERE ticker IN ('SPY', 'QQQ')")
    benchmarks = cur.fetchall()
    tickers = [t for _, t in benchmarks]
    rows = _load_prices_simfin(tickers)
    if not rows:
        conn.close()
        return
    for r in rows:
        ticker = str(r.get("Ticker", r.get("ticker", ""))).strip()
        dt = r.get("Date", r.get("date"))
        if not dt:
            continue
        d = pd.to_datetime(dt).date() if hasattr(dt, "date") else dt
        close = r.get("Close", r.get("close"))
        if close is None:
            continue
        bid = next((b[0] for b in benchmarks if b[1] == ticker), None)
        if not bid:
            continue
        cur.execute(
            """
            INSERT INTO core.core_benchmark_prices_daily (benchmark_id, trade_date, close)
            VALUES (%s, %s, %s)
            ON CONFLICT (benchmark_id, trade_date) DO UPDATE SET close = EXCLUDED.close
            """,
            (bid, d, close),
        )
    conn.commit()
    conn.close()

if __name__ == "__main__":
    job_ingest_benchmark_prices()

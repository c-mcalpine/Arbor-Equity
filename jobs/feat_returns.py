"""
Feature job: compute 24h/7d/MTD/QTD/YTD returns per security and for benchmarks,
then portfolio roll-up and alpha vs S&P 500, Nasdaq, 3M T-bill.
"""
import os
import sys
from datetime import date, timedelta
from decimal import Decimal

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from models.db import get_connection

def _decimal(n):
    if n is None or (isinstance(n, float) and pd.isna(n)):
        return None
    return Decimal(str(round(n, 6)))

def _price_series(cur, security_id: int, end_date: date, lookback_days: int) -> pd.Series:
    cur.execute(
        """
        SELECT trade_date, close FROM core.core_prices_daily
        WHERE security_id = %s AND trade_date <= %s
        ORDER BY trade_date DESC
        LIMIT %s
        """,
        (security_id, end_date, lookback_days + 1),
    )
    rows = cur.fetchall()
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows, columns=["trade_date", "close"])
    df = df.set_index("trade_date").sort_index()
    return df["close"].astype(float)

def _benchmark_series(cur, benchmark_id: int, end_date: date, lookback_days: int) -> pd.Series:
    cur.execute(
        """
        SELECT trade_date, close FROM core.core_benchmark_prices_daily
        WHERE benchmark_id = %s AND trade_date <= %s
        ORDER BY trade_date DESC
        LIMIT %s
        """,
        (benchmark_id, end_date, lookback_days + 1),
    )
    rows = cur.fetchall()
    if not rows:
        return pd.Series(dtype=float)
    df = pd.DataFrame(rows, columns=["trade_date", "close"])
    df = df.set_index("trade_date").sort_index()
    return df["close"].astype(float)

def vol_and_drawdown(series: pd.Series) -> dict:
    """Compute vol_7d, vol_60d, vol_spike_ratio, drawdown_52w from close price series (newest first)."""
    out = {"vol_7d": None, "vol_60d": None, "vol_spike_ratio": None, "drawdown_52w": None}
    if series is None or len(series) < 2:
        return out
    series = series.sort_index(ascending=False)
    rets = series.pct_change().dropna()
    if len(rets) >= 7:
        out["vol_7d"] = float(rets.iloc[:7].std())
    if len(rets) >= 60:
        out["vol_60d"] = float(rets.iloc[:60].std())
    if out["vol_60d"] and out["vol_60d"] != 0 and out["vol_7d"] is not None:
        out["vol_spike_ratio"] = out["vol_7d"] / out["vol_60d"]
    # 52w high (approx 252 trading days)
    look = series.iloc[: min(260, len(series))]
    if not look.empty:
        high52w = float(look.max())
        p0 = float(series.iloc[0])
        if high52w and high52w > 0:
            out["drawdown_52w"] = (p0 - high52w) / high52w
    return out

def returns_for_series(series: pd.Series, as_of: date) -> dict:
    """Compute 24h, 7d, MTD, QTD, YTD from a close price series (index = trade_date)."""
    if series is None or series.empty:
        return {"return_24h": None, "return_7d": None, "return_mtd": None, "return_qtd": None, "return_ytd": None}
    series = series.sort_index(ascending=False)
    latest = series.index[0]
    p0 = float(series.iloc[0])
    if p0 == 0:
        return {"return_24h": None, "return_7d": None, "return_mtd": None, "return_qtd": None, "return_ytd": None}
    out = {}
    # 24h: previous close
    if len(series) >= 2:
        out["return_24h"] = (p0 / float(series.iloc[1]) - 1) if series.iloc[1] else None
    else:
        out["return_24h"] = None
    # 7d
    week_ago = as_of - timedelta(days=7)
    px = series[series.index <= week_ago]
    if not px.empty:
        out["return_7d"] = (p0 / float(px.iloc[-1]) - 1) if px.iloc[-1] else None
    else:
        out["return_7d"] = None
    # MTD
    month_start = date(as_of.year, as_of.month, 1)
    px = series[series.index < month_start]
    if not px.empty:
        out["return_mtd"] = (p0 / float(px.iloc[-1]) - 1) if px.iloc[-1] else None
    else:
        out["return_mtd"] = None
    # QTD
    q = (as_of.month - 1) // 3 + 1
    quarter_start = date(as_of.year, (q - 1) * 3 + 1, 1)
    px = series[series.index < quarter_start]
    if not px.empty:
        out["return_qtd"] = (p0 / float(px.iloc[-1]) - 1) if px.iloc[-1] else None
    else:
        out["return_qtd"] = None
    # YTD
    year_start = date(as_of.year, 1, 1)
    px = series[series.index < year_start]
    if not px.empty:
        out["return_ytd"] = (p0 / float(px.iloc[-1]) - 1) if px.iloc[-1] else None
    else:
        out["return_ytd"] = None
    return out

def job_feat_returns(as_of_date: date = None):
    if as_of_date is None:
        as_of_date = date.today()
    conn = get_connection()
    cur = conn.cursor()
    # Latest trade date we have
    cur.execute("SELECT MAX(trade_date) FROM core.core_prices_daily WHERE trade_date <= %s", (as_of_date,))
    row = cur.fetchone()
    latest = row[0] if row and row[0] else as_of_date
    lookback = 400

    # Security returns -> feat_returns (with vol spike, drawdown_52w, what_changed_score)
    cur.execute("SELECT id FROM core.core_security_master")
    for (security_id,) in cur.fetchall():
        series = _price_series(cur, security_id, latest, lookback)
        ret = returns_for_series(series, latest)
        vd = vol_and_drawdown(series)
        # What-changed score: |return_7d|*10 + vol_spike_ratio + |drawdown_52w|*10 (higher = more change)
        score = None
        if ret.get("return_7d") is not None or vd.get("vol_spike_ratio") is not None or vd.get("drawdown_52w") is not None:
            score = 0.0
            if ret.get("return_7d") is not None:
                score += abs(float(ret["return_7d"])) * 10
            if vd.get("vol_spike_ratio") is not None:
                score += float(vd["vol_spike_ratio"])
            if vd.get("drawdown_52w") is not None:
                score += abs(float(vd["drawdown_52w"])) * 10
        cur.execute(
            """
            INSERT INTO feat.feat_returns (security_id, as_of_date, return_24h, return_7d, return_mtd, return_qtd, return_ytd,
                vol_7d, vol_60d, vol_spike_ratio, drawdown_52w, what_changed_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (security_id, as_of_date) DO UPDATE SET
                return_24h = EXCLUDED.return_24h, return_7d = EXCLUDED.return_7d,
                return_mtd = EXCLUDED.return_mtd, return_qtd = EXCLUDED.return_qtd, return_ytd = EXCLUDED.return_ytd,
                vol_7d = EXCLUDED.vol_7d, vol_60d = EXCLUDED.vol_60d, vol_spike_ratio = EXCLUDED.vol_spike_ratio,
                drawdown_52w = EXCLUDED.drawdown_52w, what_changed_score = EXCLUDED.what_changed_score
            """,
            (
                security_id, latest,
                _decimal(ret["return_24h"]), _decimal(ret["return_7d"]), _decimal(ret["return_mtd"]), _decimal(ret["return_qtd"]), _decimal(ret["return_ytd"]),
                _decimal(vd.get("vol_7d")), _decimal(vd.get("vol_60d")), _decimal(vd.get("vol_spike_ratio")), _decimal(vd.get("drawdown_52w")), _decimal(score),
            ),
        )
    conn.commit()

    # Benchmark returns -> feat_benchmark_returns
    cur.execute("SELECT id, ticker FROM core.core_benchmarks")
    benchmarks = cur.fetchall()
    for benchmark_id, ticker in benchmarks:
        series = _benchmark_series(cur, benchmark_id, latest, lookback)
        if series.empty and ticker == "TB3M":
            # Placeholder: no series for T-bills; use null or tiny constant
            ret = {"return_24h": None, "return_7d": None, "return_mtd": None, "return_qtd": None, "return_ytd": None}
        else:
            ret = returns_for_series(series, latest)
        cur.execute(
            """
            INSERT INTO feat.feat_benchmark_returns (benchmark_id, as_of_date, return_24h, return_7d, return_mtd, return_qtd, return_ytd)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (benchmark_id, as_of_date) DO UPDATE SET
                return_24h = EXCLUDED.return_24h, return_7d = EXCLUDED.return_7d,
                return_mtd = EXCLUDED.return_mtd, return_qtd = EXCLUDED.return_qtd, return_ytd = EXCLUDED.return_ytd
            """,
            (benchmark_id, latest, _decimal(ret["return_24h"]), _decimal(ret["return_7d"]), _decimal(ret["return_mtd"]), _decimal(ret["return_qtd"]), _decimal(ret["return_ytd"])),
        )
    conn.commit()

    # Portfolio: weighted sum of security returns; alpha = portfolio - benchmark
    cur.execute("""
        SELECT p.security_id, p.weight, r.return_24h, r.return_7d, r.return_mtd, r.return_qtd, r.return_ytd
        FROM core.core_positions p
        JOIN feat.feat_returns r ON r.security_id = p.security_id AND r.as_of_date = %s
        WHERE p.as_of_date = (SELECT MAX(as_of_date) FROM core.core_positions)
    """, (latest,))
    pos = cur.fetchall()
    if not pos:
        conn.close()
        return
    portfolio_24h = sum(r[2] * float(r[1]) for r in pos if r[2] is not None)
    portfolio_7d = sum(r[3] * float(r[1]) for r in pos if r[3] is not None)
    portfolio_mtd = sum(r[4] * float(r[1]) for r in pos if r[4] is not None)
    portfolio_qtd = sum(r[5] * float(r[1]) for r in pos if r[5] is not None)
    portfolio_ytd = sum(r[6] * float(r[1]) for r in pos if r[6] is not None)

    # Resolve benchmark IDs by name (SPY = S&P 500, QQQ = Nasdaq, TB3M = T-bill)
    cur.execute("SELECT id, ticker FROM core.core_benchmarks")
    bid_by_ticker = {t: i for i, t in cur.fetchall()}
    cur.execute("SELECT benchmark_id, return_24h, return_7d, return_mtd, return_qtd, return_ytd FROM feat.feat_benchmark_returns WHERE as_of_date = %s", (latest,))
    bench_rows = {r[0]: r for r in cur.fetchall()}

    def alpha(bid, period_idx):
        if bid not in bench_rows:
            return None
        b = bench_rows[bid]
        bench_val = b[period_idx]
        if bench_val is None:
            return None
        port_val = [portfolio_24h, portfolio_7d, portfolio_mtd, portfolio_qtd, portfolio_ytd][period_idx - 1]
        if port_val is None:
            return None
        return port_val - float(bench_val)

    sp500_id = bid_by_ticker.get("SPY")
    nasdaq_id = bid_by_ticker.get("QQQ")
    tbill_id = bid_by_ticker.get("TB3M")

    cur.execute(
        """
        INSERT INTO feat.feat_portfolio (as_of_date, return_24h, return_7d, return_mtd, return_qtd, return_ytd,
            alpha_vs_sp500_24h, alpha_vs_sp500_7d, alpha_vs_sp500_mtd, alpha_vs_sp500_qtd, alpha_vs_sp500_ytd,
            alpha_vs_nasdaq_24h, alpha_vs_nasdaq_7d, alpha_vs_nasdaq_mtd, alpha_vs_nasdaq_qtd, alpha_vs_nasdaq_ytd,
            alpha_vs_tbill_24h, alpha_vs_tbill_7d, alpha_vs_tbill_mtd, alpha_vs_tbill_qtd, alpha_vs_tbill_ytd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (as_of_date) DO UPDATE SET
            return_24h = EXCLUDED.return_24h, return_7d = EXCLUDED.return_7d, return_mtd = EXCLUDED.return_mtd, return_qtd = EXCLUDED.return_qtd, return_ytd = EXCLUDED.return_ytd,
            alpha_vs_sp500_24h = EXCLUDED.alpha_vs_sp500_24h, alpha_vs_sp500_7d = EXCLUDED.alpha_vs_sp500_7d, alpha_vs_sp500_mtd = EXCLUDED.alpha_vs_sp500_mtd, alpha_vs_sp500_qtd = EXCLUDED.alpha_vs_sp500_qtd, alpha_vs_sp500_ytd = EXCLUDED.alpha_vs_sp500_ytd,
            alpha_vs_nasdaq_24h = EXCLUDED.alpha_vs_nasdaq_24h, alpha_vs_nasdaq_7d = EXCLUDED.alpha_vs_nasdaq_7d, alpha_vs_nasdaq_mtd = EXCLUDED.alpha_vs_nasdaq_mtd, alpha_vs_nasdaq_qtd = EXCLUDED.alpha_vs_nasdaq_qtd, alpha_vs_nasdaq_ytd = EXCLUDED.alpha_vs_nasdaq_ytd,
            alpha_vs_tbill_24h = EXCLUDED.alpha_vs_tbill_24h, alpha_vs_tbill_7d = EXCLUDED.alpha_vs_tbill_7d, alpha_vs_tbill_mtd = EXCLUDED.alpha_vs_tbill_mtd, alpha_vs_tbill_qtd = EXCLUDED.alpha_vs_tbill_qtd, alpha_vs_tbill_ytd = EXCLUDED.alpha_vs_tbill_ytd
        """,
        (
            latest,
            _decimal(portfolio_24h), _decimal(portfolio_7d), _decimal(portfolio_mtd), _decimal(portfolio_qtd), _decimal(portfolio_ytd),
            _decimal(alpha(sp500_id, 1) if sp500_id else None), _decimal(alpha(sp500_id, 2) if sp500_id else None), _decimal(alpha(sp500_id, 3) if sp500_id else None), _decimal(alpha(sp500_id, 4) if sp500_id else None), _decimal(alpha(sp500_id, 5) if sp500_id else None),
            _decimal(alpha(nasdaq_id, 1) if nasdaq_id else None), _decimal(alpha(nasdaq_id, 2) if nasdaq_id else None), _decimal(alpha(nasdaq_id, 3) if nasdaq_id else None), _decimal(alpha(nasdaq_id, 4) if nasdaq_id else None), _decimal(alpha(nasdaq_id, 5) if nasdaq_id else None),
            _decimal(alpha(tbill_id, 1) if tbill_id else None), _decimal(alpha(tbill_id, 2) if tbill_id else None), _decimal(alpha(tbill_id, 3) if tbill_id else None), _decimal(alpha(tbill_id, 4) if tbill_id else None), _decimal(alpha(tbill_id, 5) if tbill_id else None),
        ),
    )
    conn.commit()
    conn.close()

if __name__ == "__main__":
    job_feat_returns()

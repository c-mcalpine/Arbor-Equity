"""
Bootstrap DB: create schemas, load core_security_master from ticker list + CIK map,
create core_positions (optional default weights), insert benchmarks.
"""
import os
import sys

# Project root
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from models.db import get_connection, run_sql_file

def main():
    conn = get_connection()
    sql_dir = os.path.join(ROOT, "sql")
    for name in ["00_schemas.sql", "01_raw_tables.sql", "02_core_tables.sql", "03_feat_tables.sql", "04_phase2.sql"]:
        path = os.path.join(sql_dir, name)
        if os.path.isfile(path):
            run_sql_file(conn, path)
            print(f"Ran {name}")

    from config.tickers import DEFAULT_TICKERS
    from config.cik_map import TICKER_TO_CIK
    from config.benchmarks import BENCHMARKS

    with conn.cursor() as cur:
        for ticker in DEFAULT_TICKERS:
            cik = TICKER_TO_CIK.get(ticker)
            cur.execute(
                """
                INSERT INTO core.core_security_master (ticker, cik, name, currency)
                VALUES (%s, %s, %s, 'USD')
                ON CONFLICT (ticker) DO UPDATE SET cik = EXCLUDED.cik, updated_at = NOW()
                """,
                (ticker, cik, ticker),
            )
        for b in BENCHMARKS:
            cur.execute(
                """
                INSERT INTO core.core_benchmarks (ticker, name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (ticker) DO UPDATE SET name = EXCLUDED.name, description = EXCLUDED.description
                """,
                (b["ticker"], b["name"], b.get("description", "")),
            )
        # Default positions: equal weight 1/N for each security (as_of_date = today)
        cur.execute("SELECT id FROM core.core_security_master WHERE ticker = ANY(%s)", (DEFAULT_TICKERS,))
        ids = [r[0] for r in cur.fetchall()]
        n = len(ids) if ids else 1
        w = 1.0 / n
        cur.execute("SELECT as_of_date FROM core.core_positions ORDER BY as_of_date DESC LIMIT 1")
        row = cur.fetchone()
        if not row:
            for sid in ids:
                cur.execute(
                    "INSERT INTO core.core_positions (security_id, weight, as_of_date) VALUES (%s, %s, CURRENT_DATE) ON CONFLICT (security_id, as_of_date) DO NOTHING",
                    (sid, w),
                )
    conn.commit()
    conn.close()
    print("Bootstrap done: security_master, benchmarks, default positions.")

if __name__ == "__main__":
    main()

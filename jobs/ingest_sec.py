"""
Ingest SEC data: companyfacts + submissions (JSON) for each CIK in our universe.
Uses data.sec.gov JSON endpoints. No manual filing download.
"""
import os
import sys
import hashlib
import json
import time
from datetime import datetime

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from models.db import get_connection

SEC_USER_AGENT = "EquityInfraMVP contact@example.com"
SEC_BASE = "https://data.sec.gov"

def _source_hash(data: dict) -> str:
    return hashlib.sha256(json.dumps(data, sort_keys=True, default=str).encode()).hexdigest()[:32]

def fetch_companyfacts(cik: str) -> dict | None:
    """Fetch companyfacts JSON for a CIK (10-digit padded)."""
    cik_pad = cik.zfill(10)
    url = f"{SEC_BASE}/api/xbrl/companyfacts/CIK{cik_pad}.json"
    try:
        r = requests.get(url, headers={"User-Agent": SEC_USER_AGENT}, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def fetch_submissions(cik: str) -> dict | None:
    """Fetch submissions (filing history) JSON for a CIK."""
    cik_pad = cik.zfill(10)
    url = f"{SEC_BASE}/api/submissions/CIK{cik_pad}.json"
    try:
        r = requests.get(url, headers={"User-Agent": SEC_USER_AGENT}, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None

def job_ingest_sec_companyfacts(tickers: list = None):
    if tickers is None:
        from config.tickers import DEFAULT_TICKERS
        tickers = DEFAULT_TICKERS
    from config.cik_map import TICKER_TO_CIK

    conn = get_connection()
    asof = datetime.utcnow()
    provider = "sec"

    for ticker in tickers:
        cik = TICKER_TO_CIK.get(ticker)
        if not cik:
            continue
        # Company facts
        data = fetch_companyfacts(cik)
        if data:
            cik_pad = cik.zfill(10)
            payload = json.dumps(data) if isinstance(data, dict) else data
            entity_name = (data.get("entityName") or data.get("entity", {}).get("name") or ticker) if isinstance(data, dict) else ticker
            sh = _source_hash(data) if isinstance(data, dict) else hashlib.sha256(str(data).encode()).hexdigest()[:32]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.raw_sec_companyfacts (provider, asof_loaded_at, source_hash, payload, cik, entity_name)
                    VALUES (%s, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (cik, source_hash) DO NOTHING
                    """,
                    (provider, asof, sh, json.dumps(data) if isinstance(data, dict) else payload, cik_pad, entity_name),
                )
            conn.commit()
        time.sleep(0.2)
        # Submissions (filing history)
        sub = fetch_submissions(cik)
        if sub:
            sh = _source_hash(sub) if isinstance(sub, dict) else hashlib.sha256(str(sub).encode()).hexdigest()[:32]
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw.raw_sec_submissions (provider, asof_loaded_at, source_hash, payload, cik)
                    VALUES (%s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (cik, source_hash) DO NOTHING
                    """,
                    (provider, asof, sh, json.dumps(sub), cik.zfill(10)),
                )
            conn.commit()
        time.sleep(0.2)

    conn.close()
    return True

if __name__ == "__main__":
    job_ingest_sec_companyfacts()

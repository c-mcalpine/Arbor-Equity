"""Sanity checks: DB connectivity, schemas, ingest/feat jobs run without hard failure."""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

def test_db_connection():
    from models.db import get_connection
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    assert cur.fetchone()[0] == 1
    conn.close()

def test_schemas_exist():
    from models.db import get_connection
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw','core','feat')")
    names = {r[0] for r in cur.fetchall()}
    assert "raw" in names and "core" in names and "feat" in names
    conn.close()

def test_security_master_has_rows():
    from models.db import get_connection
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM core.core_security_master")
    n = cur.fetchone()[0]
    assert n >= 0
    conn.close()

def test_ingest_simfin_runs():
    from jobs.ingest_simfin import job_ingest_simfin
    from config.tickers import DEFAULT_TICKERS
    job_ingest_simfin(tickers=DEFAULT_TICKERS[:2])
    # No exception = pass

def test_feat_returns_runs():
    from jobs.feat_returns import job_feat_returns
    job_feat_returns()
    # No exception = pass

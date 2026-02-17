"""DB connection and run DDL helpers."""
import os
import sys

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "equity_mvp"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )

def run_sql_file(conn, path: str) -> None:
    with open(path, "r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        if "DO $$" in sql:
            cur.execute(sql)
        else:
            statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
            for stmt in statements:
                if stmt:
                    cur.execute(stmt)
    conn.commit()

def run_sql_files_in_order(conn, base_dir: str, pattern: str = "*.sql") -> None:
    import glob
    sql_dir = os.path.join(base_dir, "sql")
    if not os.path.isdir(sql_dir):
        return
    for path in sorted(glob.glob(os.path.join(sql_dir, "*.sql"))):
        run_sql_file(conn, path)

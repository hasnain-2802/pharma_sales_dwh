import os
from pathlib import Path

import duckdb

from src.query.logger import init_log, run_logged
from src.query.queries import AGG_SQL, QUERIES

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
DB_PATH = str(DATA_DIR / "warehouse" / "sdi.duckdb")


def build_aggregates():
    init_log()
    con = duckdb.connect(DB_PATH)
    try:
        for stmt in AGG_SQL:
            con.execute(stmt)
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
                for t in ["agg_sale_month", "agg_demand_month"]}
    finally:
        con.close()


def run_queries(repeats=3):
    init_log()
    con = duckdb.connect(DB_PATH, read_only=True)
    ok = failed = 0
    try:
        for q in QUERIES:
            for variant in ("base", "opt"):
                sql = q.get(variant)
                if not sql:
                    continue
                for _ in range(repeats):
                    if run_logged(con, q["name"], variant, sql) is None:
                        failed += 1
                    else:
                        ok += 1
    finally:
        con.close()
    return {"succeeded": ok, "failed": failed}


def summary():
    from src.query.logger import engine
    from sqlalchemy import text
    with engine().connect() as c:
        rows = c.execute(text("SELECT query_name, variant, runs, avg_ms, error_rate_pct "
                              "FROM ops.v_query_summary ORDER BY 1, 2")).fetchall()
    for r in rows:
        print(r)
    return len(rows)

import os
import time

from sqlalchemy import create_engine, text

LOG_DDL = [
    "CREATE SCHEMA IF NOT EXISTS ops",
    """CREATE TABLE IF NOT EXISTS ops.query_log (
        query_id BIGSERIAL PRIMARY KEY, query_name TEXT, variant TEXT,
        run_ts TIMESTAMP DEFAULT now(), duration_ms NUMERIC(12,2),
        rows_returned INTEGER, status TEXT, error_msg TEXT)""",
    """CREATE OR REPLACE VIEW ops.v_query_summary AS
       SELECT query_name, variant, count(*) AS runs,
              round(avg(duration_ms) FILTER (WHERE status = 'SUCCESS'), 2) AS avg_ms,
              round(min(duration_ms) FILTER (WHERE status = 'SUCCESS'), 2) AS min_ms,
              round(100.0 * count(*) FILTER (WHERE status = 'FAILED') / count(*), 2) AS error_rate_pct
       FROM ops.query_log GROUP BY 1, 2""",
    """CREATE OR REPLACE VIEW ops.v_query_daily AS
       SELECT run_ts::date AS day, count(*) AS query_count,
              count(*) FILTER (WHERE status = 'FAILED') AS failed,
              round(100.0 * count(*) FILTER (WHERE status = 'FAILED') / count(*), 2) AS error_rate_pct
       FROM ops.query_log GROUP BY 1""",
]


def engine():
    return create_engine(os.environ["STAGING_DB_URL"], pool_pre_ping=True)


def init_log():
    with engine().begin() as c:
        for stmt in LOG_DDL:
            c.execute(text(stmt))


def log_query(name, variant, ms, rows, status, err=None):
    with engine().begin() as c:
        c.execute(
            text("INSERT INTO ops.query_log (query_name, variant, duration_ms, rows_returned, status, error_msg) "
                 "VALUES (:n, :v, :ms, :r, :s, :e)"),
            {"n": name, "v": variant, "ms": round(ms, 2), "r": rows, "s": status, "e": err},
        )


def run_logged(con, name, variant, sql):
    """Time a query, log success or failure, and return the rows (None on failure)."""
    t0 = time.perf_counter()
    try:
        rows = con.execute(sql).fetchall()
        log_query(name, variant, (time.perf_counter() - t0) * 1000, len(rows), "SUCCESS")
        return rows
    except Exception as e:
        log_query(name, variant, (time.perf_counter() - t0) * 1000, 0, "FAILED", str(e)[:500])
        return None

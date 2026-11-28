import os

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text

from src.transform.dwh import connect, DB_PATH

DAY_SEL = """
SELECT f.snapshot_date_key, d.year_month, w.wh_id, w.wh_name,
       w.total_capacity_packs AS capacity,
       sum(f.closing_qty) AS stock_qty, sum(f.shipped_qty) AS shipped_qty,
       count(*) AS rows_n,
       sum(CASE WHEN p.stockout_flag = 'Y' THEN 1 ELSE 0 END) AS stockout_rows
FROM fact_inventory_snapshot f
JOIN dim_warehouse w ON w.wh_key = f.wh_key
JOIN dim_date d ON d.date_key = f.snapshot_date_key
JOIN dim_inventory_profile p ON p.inv_profile_key = f.inv_profile_key
GROUP BY ALL"""

MONTH_SEL = """
SELECT year_month, wh_id, wh_name, any_value(capacity) AS capacity,
       round(avg(stock_qty)) AS avg_stock, round(avg(shipped_qty)) AS avg_shipped,
       round(100 * avg(stock_qty) / any_value(capacity), 2) AS utilisation_pct,
       round(avg(stock_qty) / nullif(avg(shipped_qty), 0), 1) AS days_of_cover,
       sum(stockout_rows) AS stockout_rows, sum(rows_n) AS rows_n,
       round(100.0 * sum(stockout_rows) / sum(rows_n), 2) AS stockout_pct
FROM agg_inv_day GROUP BY year_month, wh_id, wh_name"""

NEAR_SEL = """
WITH lastsnap AS (
    SELECT * FROM fact_inventory_snapshot
    QUALIFY row_number() OVER (PARTITION BY warehouse_lot_id ORDER BY snapshot_date_key DESC) = 1)
SELECT w.wh_name,
       CASE WHEN l.days_to_expiry < 0 THEN 'Expired' WHEN l.days_to_expiry <= 30 THEN '0-30'
            WHEN l.days_to_expiry <= 60 THEN '31-60' ELSE '61-90' END AS bucket,
       count(*) AS lots, sum(l.closing_qty) AS qty,
       round(sum(l.closing_qty * coalesce(m.wholesale_price_usd, 0)), 2) AS at_risk_usd
FROM lastsnap l
JOIN dim_warehouse w ON w.wh_key = l.wh_key
LEFT JOIN dim_medicine m ON m.med_key = l.med_key
WHERE l.closing_qty > 0 AND l.days_to_expiry <= 90
GROUP BY ALL"""

FEFO_SEL = """
SELECT d.year_month, w.wh_name, count(*) AS pick_rows,
       sum(CASE WHEN p.fefo_flag = 'Y' THEN 1 ELSE 0 END) AS compliant_rows,
       round(100.0 * sum(CASE WHEN p.fefo_flag = 'Y' THEN 1 ELSE 0 END) / count(*), 2) AS fefo_pct
FROM fact_inventory_snapshot f
JOIN dim_warehouse w ON w.wh_key = f.wh_key
JOIN dim_date d ON d.date_key = f.snapshot_date_key
JOIN dim_inventory_profile p ON p.inv_profile_key = f.inv_profile_key
WHERE f.picked_qty > 0
GROUP BY d.year_month, w.wh_name"""

CARRIER_SEL = """
SELECT c.carrier_name, d.year_month, count(*) AS lines,
       round(100.0 * avg(f.otif), 2) AS otif_pct,
       round(sum(f.freight_cost_usd) / nullif(sum(f.shipped_qty), 0), 4) AS freight_per_pack
FROM fact_shipment f
JOIN dim_carrier c ON c.carrier_key = f.carrier_key
JOIN dim_date d ON d.date_key = f.dispatch_date_key
GROUP BY c.carrier_name, d.year_month"""

AGGS = [
    ("agg_inv_day", DAY_SEL),
    ("agg_inv_wh_month", MONTH_SEL),
    ("agg_near_expiry", NEAR_SEL),
    ("agg_fefo", FEFO_SEL),
    ("agg_carrier", CARRIER_SEL),
]
MART_TABLES = {"inv_by_warehouse": "agg_inv_wh_month", "near_expiry": "agg_near_expiry",
               "fefo": "agg_fefo", "carrier_otif": "agg_carrier"}

# (name, base SQL on facts, opt SQL on aggregates)
INV_QUERIES = [
    ("I1_stockout_by_warehouse",
     """SELECT w.wh_name, round(100.0 * sum(CASE WHEN p.stockout_flag = 'Y' THEN 1 ELSE 0 END) / count(*), 2)
        FROM fact_inventory_snapshot f JOIN dim_warehouse w ON w.wh_key = f.wh_key
        JOIN dim_inventory_profile p ON p.inv_profile_key = f.inv_profile_key GROUP BY 1 ORDER BY 2 DESC""",
     """SELECT wh_name, round(100.0 * sum(stockout_rows) / sum(rows_n), 2)
        FROM agg_inv_wh_month GROUP BY 1 ORDER BY 2 DESC"""),
    ("I2_near_expiry_matrix",
     NEAR_SEL + " ORDER BY 1, 2",
     "SELECT wh_name, bucket, sum(lots), sum(qty), sum(at_risk_usd) FROM agg_near_expiry GROUP BY 1, 2 ORDER BY 1, 2"),
    ("I3_fefo_by_warehouse",
     """SELECT w.wh_name, round(100.0 * avg(CASE WHEN p.fefo_flag = 'Y' THEN 1 ELSE 0 END), 2)
        FROM fact_inventory_snapshot f JOIN dim_warehouse w ON w.wh_key = f.wh_key
        JOIN dim_inventory_profile p ON p.inv_profile_key = f.inv_profile_key
        WHERE f.picked_qty > 0 GROUP BY 1 ORDER BY 2""",
     "SELECT wh_name, round(100.0 * sum(compliant_rows) / sum(pick_rows), 2) FROM agg_fefo GROUP BY 1 ORDER BY 2"),
    ("I4_days_of_cover",
     "SELECT wh_name, round(avg(stock_qty) / nullif(avg(shipped_qty), 0), 1) FROM (" + DAY_SEL + ") GROUP BY 1 ORDER BY 1",
     "SELECT wh_name, round(avg(stock_qty) / nullif(avg(shipped_qty), 0), 1) FROM agg_inv_day GROUP BY 1 ORDER BY 1"),
    ("I5_carrier_otif_freight",
     CARRIER_SEL + " ORDER BY 1, 2",
     "SELECT carrier_name, year_month, lines, otif_pct, freight_per_pack FROM agg_carrier ORDER BY 1, 2"),
]


def build_inv_aggregates():
    with connect() as con:
        for name, sel in AGGS:
            con.execute(f"CREATE OR REPLACE TABLE {name} AS {sel}")
        return {n: con.execute(f"SELECT count(*) FROM {n}").fetchone()[0] for n, _ in AGGS}


def publish_mart():
    eng = create_engine(os.environ["STAGING_DB_URL"])
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
    out = {}
    with connect() as con:
        for pg, dd in MART_TABLES.items():
            df = con.execute(f"SELECT * FROM {dd}").df()
            df.to_sql(pg, eng, schema="mart", if_exists="replace", index=False)
            out[pg] = len(df)
        st = con.execute("""
            SELECT 'fact_inventory_snapshot' AS table_name, count(*) AS row_count,
                   max(loaded_at) AS last_loaded, max(snapshot_date_key) AS max_date_key
            FROM fact_inventory_snapshot
            UNION ALL
            SELECT 'fact_shipment', count(*), max(loaded_at), max(dispatch_date_key) FROM fact_shipment""").df()
    st["published_at"] = pd.Timestamp.now()
    st.to_sql("refresh_status", eng, schema="mart", if_exists="append", index=False)
    return out


def run_inv_queries(repeats=3):
    from src.query.logger import init_log, run_logged
    init_log()
    con = duckdb.connect(str(DB_PATH), read_only=True)
    ok = failed = 0
    try:
        for name, base, opt in INV_QUERIES:
            for variant, sql in (("base", base), ("opt", opt)):
                for _ in range(repeats):
                    if run_logged(con, name, variant, sql) is None:
                        failed += 1
                    else:
                        ok += 1
    finally:
        con.close()
    return {"succeeded": ok, "failed": failed}

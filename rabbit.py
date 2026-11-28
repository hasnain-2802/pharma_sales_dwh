This is the whole remaining Phase 2 job as one runbook, in text only. Every stage has the files, the commands and the output to expect. I haven't seen the result of your dag_05 run yet, so Stage 0 comes first. Everything runs in your PowerShell window in the pharma_sales_dwh folder. Create files with code <path> in VS Code, and press Ctrl+S before you run anything. A 0-byte file was what broke the last attempt.

I haven't seen your Phase 1 generator or bi_sync.py, so the Phase 2 generator below is self-contained.

FILE MAP (what you will have at the end)
src/transform/inventory.py        (done) star schema + loaders
src/transform/inv_mart.py         (Stage 1) aggregates, queries, publish to Postgres mart.*
src/generator/inv_gen.py          (Stage 2) generator for files 03 and 04, plus ingest
dags/dag_05_inventory_transform.py (done) first load
dags/dag_06_phase2_refresh.py     (Stage 3) master refresh DAG used in the demo
STAGE 0: Confirm dag_05 worked (5 min)
powershell
docker compose exec airflow-scheduler airflow dags list-runs -d dag_05_inventory_transform

If there is no run, trigger it:

powershell
docker compose exec airflow-scheduler airflow dags trigger dag_05_inventory_transform
Start-Sleep 60
docker compose exec airflow-scheduler airflow dags list-runs -d dag_05_inventory_transform
docker compose exec airflow-scheduler python -c "import duckdb; c=duckdb.connect('/opt/airflow/data/warehouse/sdi.duckdb', read_only=True); print(c.execute('select wh_id, wh_name, total_capacity_packs, city, state from dim_warehouse order by 1').fetchall()); print(c.execute('select count(*) from fact_inventory_snapshot').fetchone(), c.execute('select count(*) from fact_shipment').fetchone())"
docker compose exec airflow-scheduler python -c "from src.query.logger import run_logged, init_log; print('logger OK')"

Expected:

The run state is success.
dim_warehouse shows 7 rows (the 6 canonical names plus UNKNOWN), with cities and states filled in.
The fact counts print as (122184,) (1159,).
logger OK prints.

If a task fails, run docker compose logs airflow-scheduler --tail 300 > err.txt; notepad err.txt and send me the error. Don't move on until the counts match.

STAGE 1: Metrics, aggregates, queries, publish (about 60 min)

Create src/transform/inv_mart.py:

python
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

Test it directly, without Airflow:

powershell
docker compose exec airflow-scheduler python -c "from src.transform import inv_mart as m; print(m.build_inv_aggregates()); print(m.publish_mart()); print(m.run_inv_queries(1))"
docker compose exec postgres psql -U airflow -d staging -c "SELECT * FROM mart.refresh_status;"
docker compose exec postgres psql -U airflow -d staging -c "SELECT * FROM ops.v_query_summary WHERE query_name LIKE 'I%' ORDER BY 1,2;"

Expected:

The aggregate row counts print: agg_inv_day is about 7,500 (6 warehouses × about 1,250 days), agg_inv_wh_month is about 250, agg_near_expiry is a few dozen, agg_fefo is about 250, and agg_carrier is a few hundred.
publish_mart prints the row counts of the four mart tables.
run_inv_queries(1) prints {'succeeded': 10, 'failed': 0}.
mart.refresh_status has 2 rows.
v_query_summary has 9 rows (5 base, 5 opt, minus none missing) with error_rate_pct 0.
STAGE 2: Generator for files 03 and 04 (about 60 min, Person B's task)

This fixes the bugs we found:

New IDs continue from the staging maximum.
It uses only the canonical warehouse names, GLNs and capacities, copied from the original files.
Each refresh starts after the latest date already loaded.

It copies real rows as templates and changes the IDs, dates and quantities, so every column stays valid. About 15% of new lots are short-dated, which exercises the near-expiry logic.

Create src/generator/inv_gen.py:

python
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
OUT = DATA_DIR / "oversampled" / "p2_pending"
N_SHIP = 150            # new shipment lines per refresh
WINDOW = 45             # days of snapshots per refresh
SHORT_DATED_SHARE = 0.15
FEFO_P = {"WH001": .95, "WH002": .93, "WH003": .94, "WH004": .90, "WH005": .92, "WH006": .82}


def _tmpl(name):
    for d in ("archive", "source"):
        p = DATA_DIR / d / name
        if p.exists():
            return pd.read_csv(p, dtype=str, keep_default_na=False)
    raise FileNotFoundError(name)


def _max_ids():
    eng = create_engine(os.environ["STAGING_DB_URL"])
    with eng.connect() as c:
        s = lambda q: c.execute(text(q)).scalar()
        return {
            "ship": s("SELECT coalesce(max(substring(shipmentid from '[0-9]+$')::bigint),0) FROM stg.manufacturer_delivery_data WHERE shipmentid LIKE 'MFG-SHP-%'"),
            "lot": s("SELECT coalesce(max(substring(warehouselotid from '[0-9]+$')::bigint),0) FROM stg.warehouse_data"),
            "inv": s("SELECT coalesce(max(substring(inventorysnapshotid from '[0-9]+$')::bigint),0) FROM stg.warehouse_data"),
            "last_snap": s("SELECT max(snapshotdate::date) FROM stg.warehouse_data"),
            "last_disp": s("SELECT max(dispatchdate::date) FROM stg.manufacturer_delivery_data"),
        }


def generate_refresh(seed=None):
    ids = _max_ids()
    rng = random.Random(seed or int(datetime.now().timestamp()))
    t03, t04 = _tmpl("03_manufacturer_delivery_data.csv"), _tmpl("04_warehouse_data.csv")
    wh_ids = sorted(t04["WarehouseID"].unique())
    start = max(ids["last_snap"], ids["last_disp"]) + timedelta(days=1)
    end = start + timedelta(days=WINDOW - 1)
    ship_n, lot_n, inv_n = ids["ship"], ids["lot"], ids["inv"]
    ship_rows, snap_rows = [], []
    iso = lambda d: d.strftime("%Y-%m-%d")

    for _ in range(N_SHIP):
        wh = rng.choice(wh_ids)
        s = t03[t03["DestinationWarehouseID"] == wh].sample(1, random_state=rng.randint(0, 10**9)).iloc[0].to_dict()
        w = t04[t04["WarehouseID"] == wh].sample(1, random_state=rng.randint(0, 10**9)).iloc[0].to_dict()
        ship_n += 1
        lot_n += 1
        disp = start + timedelta(days=rng.randint(0, 29))
        promised = disp + timedelta(days=rng.randint(2, 5))
        actual = promised + timedelta(days=rng.choice([0, 0, 0, 1, 2]))
        qty = rng.randint(2000, 15000)
        damaged = rng.choice([0, 0, 0, rng.randint(1, 60)])
        short = rng.choice([0, 0, 0, rng.randint(1, 200)])
        received = qty - damaged - short
        on_time = "Y" if actual <= promised else "N"
        in_full = "Y" if (damaged == 0 and short == 0) else "N"
        freight = round(qty * rng.uniform(0.12, 0.30), 2)
        s.update({
            "ShipmentID": f"MFG-SHP-{ship_n:07d}", "ShipmentLineNumber": "1",
            "SSCC18": f"95060010{ship_n:09d}", "SealNumber": f"SEAL-{ship_n:08d}",
            "DispatchDate": iso(disp), "PromisedDeliveryDate": iso(promised),
            "ActualDeliveryDate": iso(actual), "ShipmentStatus": "Delivered",
            "ShippedQuantityPacks": str(qty), "ReceivedQuantityPacks": str(received),
            "DamagedQuantityPacks": str(damaged), "ShortQuantityPacks": str(short),
            "TransitHours": str((actual - disp).days * 24),
            "OnTimeDeliveryFlag": on_time, "InFullDeliveryFlag": in_full,
            "OTIFFlag": "Y" if on_time == "Y" and in_full == "Y" else "N",
            "FreightCostUSD": str(freight), "FreightCostPerPackUSD": str(round(freight / qty, 4)),
        })
        ship_rows.append(s)

        short_dated = rng.random() < SHORT_DATED_SHARE
        expiry = actual + timedelta(days=rng.randint(20, 100) if short_dated else rng.randint(540, 720))
        lot = f"WHLOT-{lot_n:07d}"
        day, stock, first = actual, 0, True
        while day <= end:
            inv_n += 1
            days = (expiry - day).days
            opening = 0 if first else stock
            rec = received if first else 0
            if first:
                picked = exp_q = 0
            elif days < 0:
                picked, exp_q = 0, stock
            else:
                picked, exp_q = min(stock, int(stock * rng.uniform(0.01, 0.06))), 0
            closing = opening + rec - picked - exp_q
            if first:
                status = "Quarantined"
            elif days < 0:
                status = "Expired"
            elif closing == 0:
                status = "Depleted"
            elif days <= 90:
                status = "Near expiry"
            else:
                status = "Available"
            row = dict(w)
            row.update({
                "InventorySnapshotID": f"INV-{inv_n:09d}", "SnapshotDate": iso(day),
                "WarehouseLotID": lot, "InboundShipmentID": s["ShipmentID"],
                "InboundShipmentLineNumber": "1", "ProductionBatchID": s["ProductionBatchID"],
                "MedicineID": s["MedicineID"], "GTIN14": s["GTIN14"],
                "BatchExpiryDate": iso(expiry), "DaysToExpiry": str(days), "InventoryStatus": status,
                "OpeningQuantityPacks": str(opening), "ReceivedQuantityPacks": str(rec),
                "PutawayQuantityPacks": str(rec), "QuarantineQuantityPacks": str(rec),
                "AvailableQuantityPacks": "0" if (first or days < 0) else str(closing),
                "ReservedQuantityPacks": "0", "PickedQuantityPacks": str(picked),
                "ShippedQuantityPacks": str(picked), "TransferInQuantityPacks": "0",
                "TransferOutQuantityPacks": "0", "DamagedQuantityPacks": "0",
                "ExpiredQuantityPacks": str(exp_q), "AdjustmentQuantityPacks": "0",
                "ClosingQuantityPacks": str(closing),
                "StockoutFlag": "Y" if (closing == 0 and not first) else "N",
                "NearExpiryFlag": "Y" if (0 <= days <= 90 and closing > 0) else "N",
                "FEFOComplianceFlag": "Y" if rng.random() < FEFO_P.get(wh, .92) else "N",
            })
            snap_rows.append(row)
            stock, day, first = closing, day + timedelta(days=1), False

    OUT.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    f3, f4 = OUT / f"03_manufacturer_delivery_data_p2_{ts}.csv", OUT / f"04_warehouse_data_p2_{ts}.csv"
    pd.DataFrame(ship_rows)[list(t03.columns)].to_csv(f3, index=False)
    pd.DataFrame(snap_rows)[list(t04.columns)].to_csv(f4, index=False)
    return {"ship_rows": len(ship_rows), "snapshot_rows": len(snap_rows),
            "window": f"{start} to {end}", "files": [f3.name, f4.name]}


def ingest_pending():
    from src.ingestion.loader import ingest_file
    res = [ingest_file(p) for p in sorted(OUT.glob("*.csv"))]
    bad = [r for r in res if r["status"] == "FAILED"]
    if bad:
        raise RuntimeError(str(bad))
    return res

Test it:

powershell
docker compose exec airflow-scheduler python -c "from src.generator import inv_gen as g; print(g.generate_refresh(seed=42))"
Get-ChildItem data\oversampled\p2_pending | Select-Object Name, Length

Expected:

The summary shows ship_rows 150, snapshot_rows roughly 3,500 to 4,500, and a window starting 2026-12-31.
Two CSVs appear in data\oversampled\p2_pending.

Don't ingest yet. The master DAG does that. If you want a dry run, delete the two files afterwards, because pending files would be ingested twice.

If you see FileNotFoundError, the original CSVs aren't in data\archive or data\source. Copy 04_warehouse_data.csv and 03_manufacturer_delivery_data.csv into data\source and retry.

STAGE 3: Master refresh DAG (20 min)

Create dags/dag_06_phase2_refresh.py:

python
import importlib
from datetime import datetime

from airflow.decorators import dag, task


def step(task_id, module, func):
    @task(task_id=task_id)
    def _run():
        return getattr(importlib.import_module(module), func)()
    return _run()


@dag(dag_id="dag_06_phase2_refresh", start_date=datetime(2026, 11, 1), schedule=None,
     catchup=False, max_active_runs=1, tags=["phase2", "refresh", "master"])
def phase2_refresh():
    steps = [
        step("generate_files", "src.generator.inv_gen", "generate_refresh"),
        step("ingest_to_staging", "src.generator.inv_gen", "ingest_pending"),
        step("init_schema", "src.transform.inventory", "init_inventory_schema"),
        step("extract_staging", "src.transform.inventory", "extract_inventory"),
        step("load_dimensions", "src.transform.inventory", "load_inventory_dims"),
        step("load_fact_inventory", "src.transform.inventory", "load_fact_inventory"),
        step("load_fact_shipment", "src.transform.inventory", "load_fact_shipment"),
        step("commit_batches", "src.transform.inventory", "commit_inventory_batches"),
        step("parquet_partitions", "src.transform.inventory", "export_parquet"),
        step("validate", "src.transform.inventory", "validate_inventory"),
        step("build_aggregates", "src.transform.inv_mart", "build_inv_aggregates"),
        step("publish_mart", "src.transform.inv_mart", "publish_mart"),
        step("run_queries", "src.transform.inv_mart", "run_inv_queries"),
    ]
    for a, b in zip(steps, steps[1:]):
        a >> b


phase2_refresh()

Register it:

powershell
docker compose exec airflow-scheduler airflow dags reserialize
docker compose exec airflow-scheduler airflow dags list-import-errors
docker compose exec airflow-scheduler airflow dags unpause dag_06_phase2_refresh

Expected: the import-errors output says No data found.

STAGE 4: Grafana dashboards (about 90 min, Person B's task)

Open http://localhost:3000. The login is probably admin/admin, or it's in your docker-compose.yml. Check whether a Postgres datasource already exists under Connections → Data sources. If not, add one:

Host: postgres:5432
Database: staging
User: airflow
Password: airflow
TLS mode: disable

Dashboard 1, "Stockout vs Overstock":

Bar gauge of utilisation, thresholds amber 60 and red 90:
SELECT wh_name, utilisation_pct FROM mart.inv_by_warehouse WHERE year_month = (SELECT max(year_month) FROM mart.inv_by_warehouse)
Bar gauge of days of cover, thresholds red below 7 and amber above 90 (assumed):
SELECT wh_name, days_of_cover FROM mart.inv_by_warehouse WHERE year_month = (SELECT max(year_month) FROM mart.inv_by_warehouse)
Time series of stockout %, with format set to Time series:
SELECT (year_month || '-01')::timestamp AS time, wh_name AS metric, stockout_pct FROM mart.inv_by_warehouse ORDER BY 1

Dashboard 2, "Near-Expiry Risk Matrix":

Table: SELECT wh_name, bucket, lots, qty, at_risk_usd FROM mart.near_expiry ORDER BY 1, 2
Turn it into a matrix with the "Grouping to matrix" transformation (Column = bucket, Row = wh_name, Cell = at_risk_usd), then colour the cells.
Stat panel: SELECT sum(at_risk_usd) FROM mart.near_expiry

Dashboard 3, "FEFO Compliance":

Bar gauge: SELECT wh_name, round(100.0*sum(compliant_rows)/sum(pick_rows),2) AS fefo_pct FROM mart.fefo GROUP BY 1
Time series: SELECT (year_month || '-01')::timestamp AS time, wh_name AS metric, fefo_pct FROM mart.fefo ORDER BY 1

Panel for the refresh demo (put it on every dashboard):

SELECT published_at AS time, table_name AS metric, row_count FROM mart.refresh_status ORDER BY 1
Set the dashboard auto-refresh to 10s.

Optional: carrier OTIF from mart.carrier_otif, and query timing from SELECT query_name, variant, avg_ms FROM ops.v_query_summary WHERE query_name LIKE 'I%'.

Build and screenshot all of these after Stage 3's first run, so the panels show real data.

STAGE 5: Run the two refreshes (30 min)

Take baseline screenshots of all three dashboards first, and save them as shots\0_baseline_*.png.

Refresh 1:

powershell
docker compose exec airflow-scheduler airflow dags trigger dag_06_phase2_refresh
Start-Sleep 90
docker compose exec airflow-scheduler airflow dags list-runs -d dag_06_phase2_refresh

Check:

powershell
docker compose exec postgres psql -U airflow -d staging -c "SELECT table_name, row_count, last_loaded FROM mart.refresh_status ORDER BY published_at;"
docker compose exec postgres psql -U airflow -d staging -c "SELECT file_name, status, rows_read, rows_loaded, rows_rejected FROM ops.batch_ingestion_log ORDER BY log_id DESC LIMIT 2;"

Expected:

The run state is success, with all 13 tasks green.
fact_inventory_snapshot rises from 122,184 to roughly 126,000 to 127,000, and fact_shipment from 1,159 to 1,309.
The log shows two SUCCESS rows with 0 rejected.

Reload Grafana and screenshot. The numbers should differ from the baseline. Then run refresh 2 with the same trigger command. The generator picks the next window by itself, because it reads the new maximum dates and IDs from staging.

Idempotence check: trigger the DAG a third time and confirm the counts grow only by the new generated rows, never by repeats. Verify with:

powershell
docker compose exec postgres psql -U airflow -d staging -c "SELECT count(*), count(DISTINCT inventorysnapshotid) FROM stg.warehouse_data;"

The two numbers must be equal.

STAGE 6: Evidence and report (about 90 min)

Screenshots to take, saved in shots\:

Airflow graph view of dag_06_phase2_refresh, all green (run 1 and run 2).
The Airflow DAG list.
The dashboards at baseline, after refresh 1 and after refresh 2.
The ops.batch_ingestion_log query output.
ops.v_query_summary (base vs opt timings).
The mart.refresh_status query output.

Report (PDF) sections:

X-ray (the table from earlier).
Star schema diagram (the 2 facts and 5 dimensions), with the SCD2 dim_medicine lookup, the junk dimensions and the role-playing date views.
Pipeline architecture (generator → ingest → staging → DuckDB → mart.* → Grafana).
Assumptions:
Dedup on the business key.
Canonical warehouse names.
Utilisation and days-of-cover thresholds.
Refreshes continue the simulated timeline after the latest date.
15% of new lots are short-dated.
The semi-additive closing quantity is averaged per day.
FEFO is computed only on rows with picks.
One paragraph per dashboard with the story it tells.
Query layer: before and after timings, and why aggregates help.
Generator report (1-2 pages): 150 shipment lines and about 4k snapshot rows per refresh, which is about 3% of history, so the dashboards move without a long run. Include the real counts from generate_refresh.
Data problems found and fixed: the duplicate IDs, the 224k duplicate sales rows, and the warehouse name drift. This is a strong point to include.

Git:

powershell
Add-Content .gitignore "data/`nstaging_backup.dump`nerr.txt`ncheck*.txt`nprofile.txt"
git add .
git commit -m "Phase 2: inventory mart, generator, refresh DAG, dashboards"
git push
WORK SPLIT AND TIMELINE
You (Person A): Stages 0, 1, 3, 5 and the query section of the report.
Person B: Stage 2, then Stage 4 and the screenshots, using mart.* once Stage 1 has published it.

Schedule:

Sat 28 Nov, until about 17:00: Stages 0 and 1.
Sat 17:00 to 20:00: Stages 2 and 3, and the first test of dag_06.
Sun 29 Nov morning: Stage 4 on real data, then Stage 5's two refreshes.
Sun afternoon: Stage 6.
Mon 30 Nov to Fri 5 Dec: buffer. The deadline is Fri 5 Dec.
IF SOMETHING FAILS
Import check prints OK but the DAG is missing: the file is 0 bytes. Run Get-ChildItem dags, src\transform, src\generator | Select Name, Length.
A task is red: run docker compose logs airflow-scheduler --tail 300 > err.txt; notepad err.txt and paste the traceback here.
database is locked or a DuckDB IO error: two DAGs are running at once. Check airflow dags list-runs for all DAGs, and wait for the first run to finish.
Postgres host not found: run docker compose up -d. Never use docker compose down -v.
The generator can't find templates: copy 03_manufacturer_delivery_data.csv and 04_warehouse_data.csv into data\source.
Grafana shows "no data": check the datasource (host postgres:5432, not localhost) and that mart.* tables exist. Run \dt mart.* in psql.
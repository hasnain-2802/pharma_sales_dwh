"""
BI Sync Layer
=============
Exports key star-schema aggregations from DuckDB back into Postgres
so that Grafana dashboards can query them directly.

Tables created in schema ``bi``:
  - bi.kpi_summary          : headline KPIs (net sales, margin, service level)
  - bi.monthly_sales_trend  : monthly net sales & margin trend
  - bi.channel_performance  : revenue, margin %, return rates by channel
  - bi.disease_sales        : sales by disease category
  - bi.hospital_demand      : forecast vs actual per hospital & region
  - bi.unfulfilled_by_class : unfulfilled demand & stockout by therapeutic class
  - bi.medicine_360         : cross-fact medicine view (revenue + demand + service level)
  - bi.regional_intelligence: regional sales vs hospital demand over time
  - bi.emergency_ratio      : emergency demand as % of total demand by hospital
  - bi.top_movers           : fast and slow movers per therapeutic class
  - bi.service_level_detail : service level per hospital & medicine for heatmap
"""

import os
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
DB_PATH = str(DATA_DIR / "warehouse" / "sdi.duckdb")


def pg_engine():
    return create_engine(os.environ["STAGING_DB_URL"], pool_pre_ping=True)


def _run(con, sql):
    """Execute a DuckDB query and return a pandas DataFrame."""
    return con.execute(sql).fetchdf()


def sync_bi_views():
    """
    Read aggregated data from DuckDB star schema and write summary
    tables into Postgres schema ``bi`` for Grafana consumption.
    """
    engine = pg_engine()
    con = duckdb.connect(DB_PATH, read_only=True)

    try:
        with engine.begin() as pg:
            pg.execute(text("CREATE SCHEMA IF NOT EXISTS bi"))

        # ── 1. KPI Summary (single row for stat panels) ──────────
        kpi = _run(con, """
            SELECT
                (SELECT round(sum(net_sales_amount_usd), 2) FROM fact_pharmacy_sale) AS total_net_sales,
                (SELECT round(sum(gross_margin_usd), 2)     FROM fact_pharmacy_sale) AS total_gross_margin,
                (SELECT round(100.0 * sum(gross_margin_usd) / nullif(sum(net_sales_amount_usd), 0), 2)
                   FROM fact_pharmacy_sale) AS overall_margin_pct,
                (SELECT count(*) FROM fact_pharmacy_sale) AS total_transactions,
                (SELECT round(avg(service_level_pct), 2) FROM fact_hospital_demand) AS avg_service_level,
                (SELECT sum(unfulfilled_demand_packs)     FROM fact_hospital_demand) AS total_unfulfilled_packs,
                (SELECT sum(stockout_days)                FROM fact_hospital_demand) AS total_stockout_days,
                (SELECT round(100.0 * sum(emergency_request_packs) / nullif(sum(requested_quantity_packs), 0), 2)
                   FROM fact_hospital_demand) AS emergency_demand_pct,
                (SELECT sum(returned_quantity_packs)       FROM fact_pharmacy_sale) AS total_returned_packs,
                (SELECT round(100.0 * sum(returned_quantity_packs) / nullif(sum(quantity_sold_packs), 0), 2)
                   FROM fact_pharmacy_sale) AS overall_return_rate_pct,
                (SELECT count(DISTINCT med_key) FROM dim_medicine WHERE is_current) AS active_medicines,
                (SELECT count(DISTINCT hosp_key) FROM fact_hospital_demand WHERE hosp_key > 0) AS active_hospitals,
                now() AS refreshed_at
        """)
        kpi.to_sql("kpi_summary", engine, schema="bi", if_exists="replace", index=False)

        # ── 2. Monthly Sales Trend ────────────────────────────────
        monthly = _run(con, """
            SELECT d.year_month,
                   round(sum(f.net_sales_amount_usd), 2) AS net_sales,
                   round(sum(f.gross_margin_usd), 2) AS gross_margin,
                   round(100.0 * sum(f.gross_margin_usd) / nullif(sum(f.net_sales_amount_usd), 0), 2) AS margin_pct,
                   sum(f.quantity_sold_packs) AS qty_sold,
                   count(*) AS txn_count,
                   sum(f.returned_quantity_packs) AS returned_qty
            FROM fact_pharmacy_sale f
            JOIN dim_date d ON d.date_key = f.sales_date_key
            GROUP BY 1 ORDER BY 1
        """)
        monthly.to_sql("monthly_sales_trend", engine, schema="bi", if_exists="replace", index=False)

        # ── 3. Channel Performance ─────────────────────────────────
        channel = _run(con, """
            SELECT
                CASE WHEN ch.channel_group ILIKE '%e-commerce%' THEN 'E-commerce'
                     WHEN ph.pharm_type = 'Hospital' THEN 'Hospital'
                     ELSE 'Retail' END AS channel,
                count(*) AS txns,
                round(sum(f.net_sales_amount_usd), 2) AS net_sales,
                round(sum(f.gross_margin_usd), 2) AS gross_margin,
                round(100.0 * sum(f.gross_margin_usd) / nullif(sum(f.net_sales_amount_usd), 0), 2) AS margin_pct,
                sum(f.quantity_sold_packs) AS qty_sold,
                sum(f.returned_quantity_packs) AS returned_qty,
                round(100.0 * sum(f.returned_quantity_packs) / nullif(sum(f.quantity_sold_packs), 0), 2) AS return_rate_pct,
                sum(CASE WHEN sp.return_flag = 'Y' THEN 1 ELSE 0 END) AS flagged_returns,
                round(100.0 * sum(CASE WHEN sp.return_flag = 'Y' THEN 1 ELSE 0 END) / nullif(count(*), 0), 2)
                    AS return_flag_pct,
                sum(CASE WHEN sp.ecommerce_delivery_status ILIKE 'Returned%' THEN 1 ELSE 0 END)
                    AS returned_to_sender,
                round(100.0 * sum(CASE WHEN sp.ecommerce_delivery_status ILIKE 'Returned%' THEN 1 ELSE 0 END)
                      / nullif(count(*), 0), 2) AS rts_pct
            FROM fact_pharmacy_sale f
            LEFT JOIN dim_pharmacy ph ON ph.pharm_key = f.pharm_key
            LEFT JOIN dim_sales_channel ch ON ch.channel_key = f.channel_key
            LEFT JOIN dim_sale_profile sp ON sp.sale_profile_key = f.sale_profile_key
            GROUP BY 1 ORDER BY 3 DESC
        """)
        channel.to_sql("channel_performance", engine, schema="bi", if_exists="replace", index=False)

        # ── 4. Disease Category Sales ──────────────────────────────
        disease = _run(con, """
            SELECT m.disease_category,
                   count(*) AS txns,
                   round(sum(f.net_sales_amount_usd), 2) AS net_sales,
                   round(sum(f.gross_margin_usd), 2) AS gross_margin,
                   sum(f.quantity_sold_packs) AS qty_sold
            FROM fact_pharmacy_sale f
            JOIN dim_medicine m ON m.med_key = f.med_key
            GROUP BY 1 ORDER BY 3 DESC
        """)
        disease.to_sql("disease_sales", engine, schema="bi", if_exists="replace", index=False)

        # ── 5. Hospital Demand Overview ────────────────────────────
        hosp = _run(con, """
            SELECT h.hosp_name, h.region, h.size_band, h.hosp_type,
                   sum(f.forecast_demand_packs) AS total_forecast,
                   sum(f.consumed_quantity_packs) AS total_consumed,
                   sum(f.unfulfilled_demand_packs) AS total_unfulfilled,
                   sum(f.stockout_days) AS total_stockout_days,
                   round(avg(f.service_level_pct), 2) AS avg_service_level,
                   sum(f.emergency_request_packs) AS total_emergency_packs,
                   round(100.0 * sum(f.emergency_request_packs)
                         / nullif(sum(f.requested_quantity_packs), 0), 2) AS emergency_pct,
                   sum(f.wastage_quantity_packs) AS total_wastage
            FROM fact_hospital_demand f
            JOIN dim_hospital h ON h.hosp_key = f.hosp_key
            WHERE h.hosp_key > 0
            GROUP BY 1, 2, 3, 4
            ORDER BY 7 DESC
        """)
        hosp.to_sql("hospital_demand", engine, schema="bi", if_exists="replace", index=False)

        # ── 6. Unfulfilled Demand by Therapeutic Class ──────────────
        unful = _run(con, """
            SELECT m.therapeutic_class, m.disease_category,
                   sum(f.forecast_demand_packs) AS forecast,
                   sum(f.consumed_quantity_packs) AS consumed,
                   sum(f.unfulfilled_demand_packs) AS unfulfilled,
                   sum(f.stockout_days) AS stockout_days,
                   round(100.0 * sum(f.unfulfilled_demand_packs)
                         / nullif(sum(f.requested_quantity_packs), 0), 2) AS unfulfilled_pct,
                   round(avg(f.service_level_pct), 2) AS avg_service_level
            FROM fact_hospital_demand f
            JOIN dim_medicine m ON m.med_key = f.med_key
            GROUP BY 1, 2
            ORDER BY 5 DESC
        """)
        unful.to_sql("unfulfilled_by_class", engine, schema="bi", if_exists="replace", index=False)

        # ── 7. Medicine 360° View (Constellation) ──────────────────
        med360 = _run(con, """
            SELECT m.med_id, m.med_name, m.disease_category,
                   m.therapeutic_class, m.cold_chain_flag,
                   m.wholesale_price_usd, m.retail_price_usd,
                   COALESCE(ss.total_txns, 0) AS total_txns,
                   round(COALESCE(ss.total_revenue, 0), 2) AS total_revenue,
                   ss.avg_margin_pct,
                   COALESCE(ds.total_forecast, 0) AS total_forecast,
                   COALESCE(ds.total_unfulfilled, 0) AS total_unfulfilled,
                   round(COALESCE(ds.avg_service_level, 0), 2) AS avg_service_level,
                   COALESCE(ds.total_stockout_days, 0) AS total_stockout_days,
                   round(COALESCE(ss.total_revenue, 0)
                         / nullif(COALESCE(ds.total_forecast, 0) * m.wholesale_price_usd, 0) * 100, 2)
                       AS revenue_capture_pct
            FROM dim_medicine m
            LEFT JOIN (
                SELECT med_key, count(*) AS total_txns,
                       sum(net_sales_amount_usd) AS total_revenue,
                       round(100.0 * sum(gross_margin_usd)
                             / nullif(sum(net_sales_amount_usd), 0), 2) AS avg_margin_pct
                FROM fact_pharmacy_sale GROUP BY 1
            ) ss ON ss.med_key = m.med_key
            LEFT JOIN (
                SELECT med_key,
                       sum(forecast_demand_packs) AS total_forecast,
                       sum(unfulfilled_demand_packs) AS total_unfulfilled,
                       avg(service_level_pct) AS avg_service_level,
                       sum(stockout_days) AS total_stockout_days
                FROM fact_hospital_demand GROUP BY 1
            ) ds ON ds.med_key = m.med_key
            WHERE m.is_current
            ORDER BY ss.total_revenue DESC NULLS LAST
        """)
        med360.to_sql("medicine_360", engine, schema="bi", if_exists="replace", index=False)

        # ── 8. Regional Intelligence (cross-fact) ──────────────────
        regional = _run(con, """
            WITH pharmacy_sales AS (
                SELECT ph.region, d.year_month,
                       round(sum(fs.net_sales_amount_usd), 2) AS region_sales,
                       sum(fs.quantity_sold_packs) AS region_qty_sold
                FROM fact_pharmacy_sale fs
                JOIN dim_pharmacy ph ON ph.pharm_key = fs.pharm_key
                JOIN dim_date d ON d.date_key = fs.sales_date_key
                GROUP BY 1, 2
            ),
            hospital_demand AS (
                SELECT h.region, d.year_month,
                       sum(fd.forecast_demand_packs) AS region_forecast,
                       sum(fd.unfulfilled_demand_packs) AS region_unfulfilled,
                       sum(fd.stockout_days) AS region_stockout_days
                FROM fact_hospital_demand fd
                JOIN dim_hospital h ON h.hosp_key = fd.hosp_key
                JOIN dim_date d ON d.date_key = fd.demand_month_key
                GROUP BY 1, 2
            )
            SELECT COALESCE(ps.region, hd.region) AS region,
                   COALESCE(ps.year_month, hd.year_month) AS year_month,
                   ps.region_sales, ps.region_qty_sold,
                   hd.region_forecast, hd.region_unfulfilled,
                   hd.region_stockout_days,
                   round(ps.region_qty_sold::double / nullif(hd.region_forecast, 0), 3)
                       AS fulfillment_ratio
            FROM pharmacy_sales ps
            FULL OUTER JOIN hospital_demand hd
                 ON ps.region = hd.region AND ps.year_month = hd.year_month
            WHERE COALESCE(ps.region, hd.region) IS NOT NULL
            ORDER BY 1, 2
        """)
        regional.to_sql("regional_intelligence", engine, schema="bi", if_exists="replace", index=False)

        # ── 9. Emergency Demand Ratio by Hospital ──────────────────
        emerg = _run(con, """
            SELECT h.hosp_name, h.region,
                   sum(f.emergency_request_packs) AS emergency_packs,
                   sum(f.requested_quantity_packs) AS total_requested,
                   round(100.0 * sum(f.emergency_request_packs)
                         / nullif(sum(f.requested_quantity_packs), 0), 2) AS emergency_pct,
                   sum(f.stockout_days) AS stockout_days
            FROM fact_hospital_demand f
            JOIN dim_hospital h ON h.hosp_key = f.hosp_key
            WHERE h.hosp_key > 0
            GROUP BY 1, 2
            ORDER BY 5 DESC
        """)
        emerg.to_sql("emergency_ratio", engine, schema="bi", if_exists="replace", index=False)

        # ── 10. Fast / Slow Movers ─────────────────────────────────
        movers = _run(con, """
            WITH t AS (
                SELECT m.therapeutic_class, m.med_name,
                       sum(f.quantity_sold_packs) AS qty,
                       sum(f.net_sales_amount_usd) AS revenue
                FROM fact_pharmacy_sale f
                JOIN dim_medicine m ON m.med_key = f.med_key
                GROUP BY 1, 2),
            r AS (
                SELECT *,
                       rank() OVER (PARTITION BY therapeutic_class ORDER BY qty DESC) AS fast_rank,
                       rank() OVER (PARTITION BY therapeutic_class ORDER BY qty ASC) AS slow_rank
                FROM t)
            SELECT therapeutic_class, med_name, qty, round(revenue, 2) AS revenue,
                   fast_rank, slow_rank,
                   CASE WHEN fast_rank <= 3 THEN 'Fast Mover'
                        WHEN slow_rank <= 3 THEN 'Slow Mover'
                        ELSE 'Middle' END AS mover_category
            FROM r WHERE fast_rank <= 3 OR slow_rank <= 3
            ORDER BY therapeutic_class, fast_rank
        """)
        movers.to_sql("top_movers", engine, schema="bi", if_exists="replace", index=False)

        # ── 11. Service Level Heatmap Detail ───────────────────────
        svc = _run(con, """
            SELECT h.hosp_name, m.therapeutic_class,
                   round(avg(f.service_level_pct), 2) AS avg_service_level,
                   sum(f.stockout_days) AS stockout_days,
                   sum(f.unfulfilled_demand_packs) AS unfulfilled,
                   CASE WHEN avg(f.service_level_pct) >= 95 THEN 'Green'
                        WHEN avg(f.service_level_pct) >= 90 THEN 'Amber'
                        ELSE 'Red' END AS rag_status
            FROM fact_hospital_demand f
            JOIN dim_hospital h ON h.hosp_key = f.hosp_key
            JOIN dim_medicine m ON m.med_key = f.med_key
            WHERE h.hosp_key > 0
            GROUP BY 1, 2
            ORDER BY 3 ASC
        """)
        svc.to_sql("service_level_detail", engine, schema="bi", if_exists="replace", index=False)

        counts = {}
        with engine.connect() as pg:
            for t in ["kpi_summary", "monthly_sales_trend", "channel_performance",
                       "disease_sales", "hospital_demand", "unfulfilled_by_class",
                       "medicine_360", "regional_intelligence", "emergency_ratio",
                       "top_movers", "service_level_detail"]:
                r = pg.execute(text(f"SELECT count(*) FROM bi.{t}")).scalar()
                counts[t] = r

        print(f"BI sync complete: {counts}")
        return counts

    finally:
        con.close()

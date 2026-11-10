CH = ("CASE WHEN ch.channel_group = 'E-commerce' THEN 'E-commerce' "
      "WHEN ph.pharm_type = 'Hospital' THEN 'Hospital' ELSE 'Retail' END")
CH_AGG = ("CASE WHEN channel_group = 'E-commerce' THEN 'E-commerce' "
          "WHEN pharm_type = 'Hospital' THEN 'Hospital' ELSE 'Retail' END")

AGG_SQL = [
    """
    CREATE OR REPLACE TABLE agg_sale_month AS
    SELECT d.year_month, d.cal_year, f.med_key,
           COALESCE(ph.pharm_type, 'Unknown') AS pharm_type,
           COALESCE(ch.channel_group, 'Unknown') AS channel_group,
           sum(f.quantity_sold_packs) AS qty,
           sum(f.gross_sales_amount_usd) AS gross_sales,
           sum(f.net_sales_amount_usd) AS net_sales,
           sum(f.cogs_usd) AS cogs,
           sum(f.gross_margin_usd) AS gross_margin,
           count(*) AS txns,
           sum(CASE WHEN sp.return_flag = 'Y' THEN 1 ELSE 0 END) AS flagged_returns,
           sum(CASE WHEN sp.ecommerce_delivery_status ILIKE 'Returned%' THEN 1 ELSE 0 END) AS rts_returns,
           sum(f.returned_quantity_packs) AS returned_qty
    FROM fact_pharmacy_sale f
    LEFT JOIN dim_date d ON d.date_key = f.sales_date_key
    LEFT JOIN dim_pharmacy ph ON ph.pharm_key = f.pharm_key
    LEFT JOIN dim_sales_channel ch ON ch.channel_key = f.channel_key
    LEFT JOIN dim_sale_profile sp ON sp.sale_profile_key = f.sale_profile_key
    GROUP BY ALL
    """,
    """
    CREATE OR REPLACE TABLE agg_demand_month AS
    SELECT d.year_month, d.cal_year, f.hosp_key, m.disease_category,
           sum(f.forecast_demand_packs) AS forecast_packs,
           sum(f.consumed_quantity_packs) AS consumed_packs,
           sum(f.requested_quantity_packs) AS requested_packs,
           sum(f.unfulfilled_demand_packs) AS unfulfilled_packs,
           sum(f.stockout_days) AS stockout_days,
           count(*) AS records
    FROM fact_hospital_demand f
    LEFT JOIN dim_date d ON d.date_key = f.demand_month_key
    LEFT JOIN dim_medicine m ON m.med_key = f.med_key
    GROUP BY ALL
    """,
    "CREATE INDEX IF NOT EXISTS idx_fact_sale_txn ON fact_pharmacy_sale (sales_transaction_id)",
    """CREATE OR REPLACE VIEW v_constellation_medicine AS
       SELECT m.med_key, m.med_id, m.med_name, m.disease_category,
              m.therapeutic_class, m.wholesale_price_usd,
              COALESCE(s.total_sold, 0) AS total_sold,
              COALESCE(s.total_revenue, 0) AS total_revenue,
              COALESCE(d.total_forecast, 0) AS total_forecast,
              COALESCE(d.total_unfulfilled, 0) AS total_unfulfilled
       FROM dim_medicine m
       LEFT JOIN (
           SELECT med_key, sum(quantity_sold_packs) AS total_sold,
                  sum(net_sales_amount_usd) AS total_revenue
           FROM fact_pharmacy_sale GROUP BY 1
       ) s ON s.med_key = m.med_key
       LEFT JOIN (
           SELECT med_key, sum(forecast_demand_packs) AS total_forecast,
                  sum(unfulfilled_demand_packs) AS total_unfulfilled
           FROM fact_hospital_demand GROUP BY 1
       ) d ON d.med_key = m.med_key
       WHERE m.is_current""",
]

QUERIES = [
    {
        "name": "Q1_monthly_sales_trend",
        "base": """
            SELECT d.year_month, round(sum(f.net_sales_amount_usd), 2) AS net_sales,
                   round(sum(f.gross_margin_usd), 2) AS gross_margin, count(*) AS txns
            FROM fact_pharmacy_sale f JOIN dim_date d ON d.date_key = f.sales_date_key
            GROUP BY 1 ORDER BY 1""",
        "opt": """
            SELECT year_month, round(sum(net_sales), 2), round(sum(gross_margin), 2), sum(txns)
            FROM agg_sale_month GROUP BY 1 ORDER BY 1""",
    },
    {
        "name": "Q2_channel_revenue_margin",
        "base": f"""
            SELECT {CH} AS channel, round(sum(f.net_sales_amount_usd), 2) AS net_sales,
                   round(sum(f.gross_margin_usd), 2) AS gross_margin,
                   round(100 * sum(f.gross_margin_usd) / sum(f.net_sales_amount_usd), 2) AS margin_pct
            FROM fact_pharmacy_sale f
            LEFT JOIN dim_pharmacy ph ON ph.pharm_key = f.pharm_key
            LEFT JOIN dim_sales_channel ch ON ch.channel_key = f.channel_key
            GROUP BY 1 ORDER BY 2 DESC""",
        "opt": f"""
            SELECT {CH_AGG} AS channel, round(sum(net_sales), 2), round(sum(gross_margin), 2),
                   round(100 * sum(gross_margin) / sum(net_sales), 2)
            FROM agg_sale_month GROUP BY 1 ORDER BY 2 DESC""",
    },
    {
        "name": "Q3_fast_slow_movers_by_class",
        "base": """
            WITH t AS (
                SELECT m.therapeutic_class, m.med_name, sum(f.quantity_sold_packs) AS qty
                FROM fact_pharmacy_sale f JOIN dim_medicine m ON m.med_key = f.med_key
                GROUP BY 1, 2),
            r AS (
                SELECT *, rank() OVER (PARTITION BY therapeutic_class ORDER BY qty DESC) AS fast_rank,
                          rank() OVER (PARTITION BY therapeutic_class ORDER BY qty ASC) AS slow_rank
                FROM t)
            SELECT therapeutic_class, med_name, qty, fast_rank, slow_rank
            FROM r WHERE fast_rank <= 3 OR slow_rank <= 3 ORDER BY 1, fast_rank""",
        "opt": """
            WITH t AS (
                SELECT m.therapeutic_class, m.med_name, sum(a.qty) AS qty
                FROM agg_sale_month a JOIN dim_medicine m ON m.med_key = a.med_key
                GROUP BY 1, 2),
            r AS (
                SELECT *, rank() OVER (PARTITION BY therapeutic_class ORDER BY qty DESC) AS fast_rank,
                          rank() OVER (PARTITION BY therapeutic_class ORDER BY qty ASC) AS slow_rank
                FROM t)
            SELECT therapeutic_class, med_name, qty, fast_rank, slow_rank
            FROM r WHERE fast_rank <= 3 OR slow_rank <= 3 ORDER BY 1, fast_rank""",
    },
    {
        "name": "Q4_hospital_forecast_vs_actual",
        "base": """
            SELECT h.hosp_name, h.region, sum(f.forecast_demand_packs) AS forecast,
                   sum(f.consumed_quantity_packs) AS actual,
                   round(sum(f.consumed_quantity_packs) / nullif(sum(f.forecast_demand_packs), 0), 3) AS actual_to_forecast,
                   round(100 * avg(abs(f.consumed_quantity_packs - f.forecast_demand_packs)
                                   / nullif(f.forecast_demand_packs, 0)), 2) AS mape_pct
            FROM fact_hospital_demand f JOIN dim_hospital h ON h.hosp_key = f.hosp_key
            GROUP BY 1, 2 ORDER BY 3 DESC""",
        "opt": None,
    },
    {
        "name": "Q5_unfulfilled_demand",
        "base": """
            SELECT m.disease_category, h.region, sum(f.unfulfilled_demand_packs) AS unfulfilled,
                   sum(f.requested_quantity_packs) AS requested,
                   round(100 * sum(f.unfulfilled_demand_packs) / nullif(sum(f.requested_quantity_packs), 0), 2) AS unfulfilled_pct,
                   sum(f.stockout_days) AS stockout_days
            FROM fact_hospital_demand f
            JOIN dim_medicine m ON m.med_key = f.med_key
            JOIN dim_hospital h ON h.hosp_key = f.hosp_key
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20""",
        "opt": """
            SELECT a.disease_category, h.region, sum(a.unfulfilled_packs), sum(a.requested_packs),
                   round(100 * sum(a.unfulfilled_packs) / nullif(sum(a.requested_packs), 0), 2),
                   sum(a.stockout_days)
            FROM agg_demand_month a JOIN dim_hospital h ON h.hosp_key = a.hosp_key
            GROUP BY 1, 2 ORDER BY 3 DESC LIMIT 20""",
    },
    {
        "name": "Q6_return_rate_by_channel",
        "base": f"""
            SELECT {CH} AS channel, count(*) AS txns,
                   round(100.0 * sum(CASE WHEN sp.return_flag = 'Y' THEN 1 ELSE 0 END) / count(*), 2) AS return_flag_pct,
                   round(100.0 * sum(CASE WHEN sp.ecommerce_delivery_status ILIKE 'Returned%' THEN 1 ELSE 0 END) / count(*), 2) AS returned_to_sender_pct
            FROM fact_pharmacy_sale f
            LEFT JOIN dim_pharmacy ph ON ph.pharm_key = f.pharm_key
            LEFT JOIN dim_sales_channel ch ON ch.channel_key = f.channel_key
            LEFT JOIN dim_sale_profile sp ON sp.sale_profile_key = f.sale_profile_key
            GROUP BY 1 ORDER BY 2 DESC""",
        "opt": f"""
            SELECT {CH_AGG} AS channel, sum(txns),
                   round(100.0 * sum(flagged_returns) / sum(txns), 2),
                   round(100.0 * sum(rts_returns) / sum(txns), 2)
            FROM agg_sale_month GROUP BY 1 ORDER BY 2 DESC""",
    },
    {
        "name": "Q7_margin_by_class_year",
        "base": """
            SELECT m.therapeutic_class, d.cal_year, round(sum(f.net_sales_amount_usd), 2) AS net_sales,
                   round(100 * sum(f.gross_margin_usd) / sum(f.net_sales_amount_usd), 2) AS margin_pct
            FROM fact_pharmacy_sale f
            JOIN dim_medicine m ON m.med_key = f.med_key
            JOIN dim_date d ON d.date_key = f.sales_date_key
            GROUP BY 1, 2 ORDER BY 1, 2""",
        "opt": """
            SELECT m.therapeutic_class, a.cal_year, round(sum(a.net_sales), 2),
                   round(100 * sum(a.gross_margin) / sum(a.net_sales), 2)
            FROM agg_sale_month a JOIN dim_medicine m ON m.med_key = a.med_key
            GROUP BY 1, 2 ORDER BY 1, 2""",
    },
    {
        "name": "Q8_transaction_lookup",
        "base": None,
        "opt": """
            SELECT f.sales_transaction_id, d.full_date, m.med_name, ph.pharm_name,
                   f.production_batch_id, f.warehouse_lot_id, f.net_sales_amount_usd
            FROM fact_pharmacy_sale f
            JOIN dim_date d ON d.date_key = f.sales_date_key
            JOIN dim_medicine m ON m.med_key = f.med_key
            JOIN dim_pharmacy ph ON ph.pharm_key = f.pharm_key
            WHERE f.sales_transaction_id = 'SALE-000250000'""",
    },
]

# ── Fact Constellation Queries ────────────────────────────────────────
# Cross-fact queries joining multiple star schemas through conformed
# dimensions (dim_medicine, dim_date) — the "360° Inventory Intelligence"
CONSTELLATION_QUERIES = [
    {
        "name": "C1_supply_demand_gap",
        "base": """
            WITH sales AS (
                SELECT m.med_id, m.med_name, m.disease_category,
                       m.therapeutic_class,
                       sum(fs.quantity_sold_packs)    AS total_sold,
                       sum(fs.net_sales_amount_usd)   AS total_revenue,
                       round(100.0 * sum(fs.gross_margin_usd)
                             / nullif(sum(fs.net_sales_amount_usd), 0), 2) AS margin_pct
                FROM fact_pharmacy_sale fs
                JOIN dim_medicine m ON m.med_key = fs.med_key
                GROUP BY 1,2,3,4
            ),
            demand AS (
                SELECT m.med_id,
                       sum(fd.forecast_demand_packs)     AS total_forecast,
                       sum(fd.unfulfilled_demand_packs)  AS total_unfulfilled,
                       sum(fd.stockout_days)             AS total_stockout_days,
                       round(avg(fd.service_level_pct), 2) AS avg_service_level
                FROM fact_hospital_demand fd
                JOIN dim_medicine m ON m.med_key = fd.med_key
                GROUP BY 1
            )
            SELECT s.med_name, s.disease_category, s.therapeutic_class,
                   s.total_sold, round(s.total_revenue, 2) AS total_revenue,
                   s.margin_pct,
                   d.total_forecast, d.total_unfulfilled,
                   d.total_stockout_days, d.avg_service_level,
                   round(s.total_sold::double
                         / nullif(d.total_forecast, 0), 3) AS sell_through_ratio
            FROM sales s
            JOIN demand d ON s.med_id = d.med_id
            ORDER BY d.total_unfulfilled DESC
            LIMIT 20""",
        "opt": None,
    },
    {
        "name": "C2_regional_inventory_intelligence",
        "base": """
            WITH pharmacy_sales AS (
                SELECT ph.region, d.year_month,
                       sum(fs.net_sales_amount_usd) AS region_sales,
                       sum(fs.quantity_sold_packs)   AS region_qty_sold
                FROM fact_pharmacy_sale fs
                JOIN dim_pharmacy ph ON ph.pharm_key = fs.pharm_key
                JOIN dim_date d      ON d.date_key   = fs.sales_date_key
                GROUP BY 1, 2
            ),
            hospital_demand AS (
                SELECT h.region, d.year_month,
                       sum(fd.forecast_demand_packs)    AS region_forecast,
                       sum(fd.unfulfilled_demand_packs) AS region_unfulfilled,
                       sum(fd.stockout_days)            AS region_stockout_days
                FROM fact_hospital_demand fd
                JOIN dim_hospital h ON h.hosp_key = fd.hosp_key
                JOIN dim_date d     ON d.date_key = fd.demand_month_key
                GROUP BY 1, 2
            )
            SELECT COALESCE(ps.region, hd.region)       AS region,
                   COALESCE(ps.year_month, hd.year_month) AS year_month,
                   round(ps.region_sales, 2) AS region_sales,
                   ps.region_qty_sold,
                   hd.region_forecast, hd.region_unfulfilled,
                   hd.region_stockout_days,
                   round(ps.region_qty_sold::double
                         / nullif(hd.region_forecast, 0), 3) AS fulfillment_ratio
            FROM pharmacy_sales ps
            FULL OUTER JOIN hospital_demand hd
                 ON ps.region = hd.region
                AND ps.year_month = hd.year_month
            WHERE COALESCE(ps.region, hd.region) IS NOT NULL
            ORDER BY 1, 2""",
        "opt": None,
    },
    {
        "name": "C3_medicine_360_view",
        "base": """
            SELECT m.med_name, m.disease_category,
                   m.wholesale_price_usd, m.retail_price_usd,
                   ss.total_txns, round(ss.total_revenue, 2) AS total_revenue,
                   ss.avg_margin_pct,
                   ds.total_forecast,
                   round(ds.avg_service_level, 2) AS avg_service_level,
                   ds.total_stockout_days,
                   round(ss.total_revenue
                         / nullif(ds.total_forecast * m.wholesale_price_usd, 0)
                         * 100, 2) AS revenue_capture_pct
            FROM dim_medicine m
            LEFT JOIN (
                SELECT med_key, count(*) AS total_txns,
                       sum(net_sales_amount_usd) AS total_revenue,
                       round(100.0 * sum(gross_margin_usd)
                             / nullif(sum(net_sales_amount_usd), 0), 2)
                           AS avg_margin_pct
                FROM fact_pharmacy_sale GROUP BY 1
            ) ss ON ss.med_key = m.med_key
            LEFT JOIN (
                SELECT med_key,
                       sum(forecast_demand_packs) AS total_forecast,
                       avg(service_level_pct)     AS avg_service_level,
                       sum(stockout_days)         AS total_stockout_days
                FROM fact_hospital_demand GROUP BY 1
            ) ds ON ds.med_key = m.med_key
            WHERE m.is_current
            ORDER BY ss.total_revenue DESC NULLS LAST""",
        "opt": None,
    },
]

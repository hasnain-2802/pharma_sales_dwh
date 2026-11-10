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

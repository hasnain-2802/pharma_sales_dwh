import os

import pandas as pd
from sqlalchemy import create_engine, text

from src.query.queries import CH_AGG
from src.transform.dwh import connect

MART = {
    "p1_monthly": """
        SELECT year_month, round(sum(net_sales), 2) AS net_sales,
               round(sum(gross_margin), 2) AS gross_margin, sum(txns) AS txns
        FROM agg_sale_month GROUP BY 1 ORDER BY 1""",
    "p1_channel": f"""
        SELECT year_month, {CH_AGG} AS channel, round(sum(net_sales), 2) AS net_sales,
               round(sum(gross_margin), 2) AS gross_margin,
               round(100 * sum(gross_margin) / nullif(sum(net_sales), 0), 2) AS margin_pct,
               sum(txns) AS txns
        FROM agg_sale_month GROUP BY 1, 2 ORDER BY 1, 2""",
    "p1_movers": """
        WITH t AS (
            SELECT m.therapeutic_class, m.med_name, m.cold_chain_flag, sum(a.qty) AS qty
            FROM agg_sale_month a JOIN dim_medicine m ON m.med_key = a.med_key
            GROUP BY 1, 2, 3)
        SELECT *, rank() OVER (PARTITION BY therapeutic_class ORDER BY qty DESC) AS fast_rank_in_class,
                  rank() OVER (PARTITION BY therapeutic_class ORDER BY qty ASC) AS slow_rank_in_class,
                  rank() OVER (ORDER BY qty DESC) AS fast_rank_overall,
                  rank() OVER (ORDER BY qty ASC) AS slow_rank_overall
        FROM t""",
    "p1_hospital": """
        SELECT a.year_month, h.hosp_name, h.region,
               sum(a.forecast_packs) AS forecast_packs, sum(a.consumed_packs) AS consumed_packs,
               round(sum(a.consumed_packs) / nullif(sum(a.forecast_packs), 0), 3) AS actual_to_forecast,
               sum(a.requested_packs) AS requested_packs, sum(a.unfulfilled_packs) AS unfulfilled_packs,
               round(100.0 * sum(a.unfulfilled_packs) / nullif(sum(a.requested_packs), 0), 2) AS unfulfilled_pct,
               sum(a.stockout_days) AS stockout_days
        FROM agg_demand_month a JOIN dim_hospital h ON h.hosp_key = a.hosp_key
        GROUP BY 1, 2, 3 ORDER BY 1, 2""",
    "p1_unfulfilled_disease": """
        SELECT disease_category, sum(unfulfilled_packs) AS unfulfilled_packs,
               sum(requested_packs) AS requested_packs,
               round(100.0 * sum(unfulfilled_packs) / nullif(sum(requested_packs), 0), 2) AS unfulfilled_pct
        FROM agg_demand_month GROUP BY 1 ORDER BY 2 DESC""",
}


def publish_p1():
    eng = create_engine(os.environ["STAGING_DB_URL"])
    with eng.begin() as c:
        c.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
    out = {}
    with connect() as con:
        for name, sql in MART.items():
            df = con.execute(sql).df()
            df.to_sql(name, eng, schema="mart", if_exists="replace", index=False)
            out[name] = len(df)
        st = con.execute("""
            SELECT 'fact_pharmacy_sale' AS table_name, count(*) AS row_count, max(loaded_at) AS last_loaded
            FROM fact_pharmacy_sale
            UNION ALL
            SELECT 'fact_hospital_demand', count(*), max(loaded_at) FROM fact_hospital_demand""").df()
    st["published_at"] = pd.Timestamp.now()
    st.to_sql("p1_refresh_status", eng, schema="mart", if_exists="append", index=False)
    return out

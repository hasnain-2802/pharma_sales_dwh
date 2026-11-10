import os
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pandas as pd
from sqlalchemy import create_engine, text

from src.transform.ddl import DDL, SALE_MEASURES, DEMAND_MEASURES, conv

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
DB_PATH = DATA_DIR / "warehouse" / "sdi.duckdb"

REGIONS = {
    "Northeast": "CT ME MA NH RI VT NJ NY PA",
    "Midwest": "IL IN MI OH WI IA KS MN MO NE ND SD",
    "South": "DE FL GA MD NC SC VA DC WV AL KY MS TN AR LA OK TX",
    "West": "AZ CO ID MT NV NM UT WY AK CA HI OR WA",
}

SALES_COLS = [
    "salestransactionid", "salesdate", "saleschannel", "pharmacyid", "pharmacygln",
    "pharmacyname", "pharmacytype", "hospitalid", "pharmacycity", "pharmacystate",
    "pharmacyorderid", "pharmacydeliveryid", "pharmacydeliverydate", "warehouselotid",
    "productionbatchid", "medicineid", "batchexpirydate", "daystoexpiryatsale",
    "quantitysoldpacks", "unitretailpriceusd", "grosssalesamountusd", "discountamountusd",
    "netsalesamountusd", "salestaxusd", "costofgoodssoldusd", "grossmarginusd",
    "promotioncode", "prescriptionrequiredflag", "prescriptionid", "customersegment",
    "payertype", "paymentmethod", "ecommercedeliverystatus", "returnflag",
    "returnedquantitypacks", "salestatus",
]
DEMAND_COLS = [
    "hospitaldemandrecordid", "demandmonth", "hospitalid", "hospitalgln", "hospitalname",
    "hospitaltype", "hospitalpharmacyid", "city", "state", "licensedbeds", "icubeds",
    "averageoccupancyratepct", "traumalevel", "catchmentpopulation", "medicineid",
    "medicationrequeststatus", "formularystatus",
] + [v[0] for v in DEMAND_MEASURES.values()]
MASTER_COLS = [
    "medicineid", "gtin14", "genericname", "fictionalbrandname", "diseasecategory",
    "icd10category", "atccode", "therapeuticclass", "dosageform", "strength",
    "routeofadministration", "prescriptionclass", "controlledmedicineflag", "coldchainflag",
    "packsize", "shelflifemonths", "wholesalepriceusd", "recommendedretailpriceusd",
    "unitmanufacturingcostusd", "productstatus", "effectivefromdate",
]


def n(c):
    return f"NULLIF(TRIM({c}),'')"


def u(c, d="Unknown"):
    return f"COALESCE({n(c)},'{d}')"


def dk(x):
    return f"COALESCE(CAST(strftime({x}, '%Y%m%d') AS INTEGER), -1)"


@contextmanager
def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    try:
        yield con
    finally:
        con.close()


def pg_engine():
    return create_engine(os.environ["STAGING_DB_URL"], pool_pre_ping=True)


# ---------------------------------------------------------------- schema
def init_schema():
    with connect() as con:
        for stmt in DDL.split(";"):
            if stmt.strip():
                con.execute(stmt)
        con.execute("CREATE OR REPLACE TABLE ref_state_region (state VARCHAR, region VARCHAR)")
        con.executemany(
            "INSERT INTO ref_state_region VALUES (?, ?)",
            [(s, r) for r, sts in REGIONS.items() for s in sts.split()],
        )
        if con.execute("SELECT count(*) FROM dim_date WHERE date_key > 0").fetchone()[0] == 0:
            con.execute("""
                INSERT INTO dim_date
                SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER), d, year(d), quarter(d), month(d),
                       monthname(d), strftime(d, '%Y-%m'), day(d), isodow(d), dayname(d),
                       isodow(d) >= 6, weekofyear(d)
                FROM (SELECT CAST(x AS DATE) AS d
                      FROM generate_series(DATE '2022-01-01', DATE '2030-12-31', INTERVAL 1 DAY) t(x))
            """)
        # role-playing date dimensions: one physical table, several named roles
        for role in ["sales", "delivery", "expiry", "demand_month"]:
            con.execute(f"""
                CREATE OR REPLACE VIEW dim_{role}_date AS
                SELECT date_key AS {role}_date_key, full_date AS {role}_date,
                       cal_year AS {role}_year, cal_quarter AS {role}_quarter,
                       cal_month AS {role}_month, month_name AS {role}_month_name,
                       year_month AS {role}_year_month
                FROM dim_date
            """)
    return "schema ready"


# ---------------------------------------------------------------- extract
def _extract(con, name, table, cols):
    done = [r[0] for r in con.execute(
        "SELECT batch_id FROM etl_loaded_batches WHERE source = ?", [name]).fetchall()]
    con.execute(f"DROP TABLE IF EXISTS raw_{name}")
    ddl = ", ".join(f"{c} VARCHAR" for c in cols)
    con.execute(f"CREATE TABLE raw_{name} ({ddl}, _batch_id VARCHAR)")
    q = text(f"SELECT {', '.join(cols)}, _batch_id FROM {table} "
             f"WHERE _batch_id <> ALL(CAST(:ids AS text[]))")
    total = 0
    with pg_engine().connect().execution_options(stream_results=True) as c:
        for df in pd.read_sql(q, c, params={"ids": done}, chunksize=100_000):
            con.execute(f"INSERT INTO raw_{name} SELECT * FROM df")
            total += len(df)
    return total


def extract_staging():
    with connect() as con:
        return {
            "master": _extract(con, "master", "stg.master_data", MASTER_COLS),
            "sales": _extract(con, "sales", "stg.pharmacy_sales_data", SALES_COLS),
            "demand": _extract(con, "demand", "stg.hospital_demand_data", DEMAND_COLS),
        }


# ---------------------------------------------------------------- views
def _sale_view(con):
    meas = ",\n".join(f"{conv(s, t)} AS {k}" for k, (s, t) in SALE_MEASURES.items())
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW v_sale AS
    SELECT salestransactionid AS sales_transaction_id,
           TRY_CAST(salesdate AS DATE) AS sales_date,
           TRY_CAST(pharmacydeliverydate AS DATE) AS delivery_date,
           TRY_CAST(batchexpirydate AS DATE) AS expiry_date,
           {u('saleschannel')} AS sales_channel,
           {u('pharmacyid', 'UNKNOWN')} AS pharm_id,
           medicineid AS med_id,
           {u('customersegment')} AS customer_segment,
           {u('payertype')} AS payer_type,
           {u('paymentmethod')} AS payment_method,
           {u('returnflag')} AS return_flag,
           {u('salestatus')} AS sale_status,
           {u('ecommercedeliverystatus')} AS ecommerce_delivery_status,
           {u('prescriptionrequiredflag')} AS prescription_required_flag,
           {u('promotioncode', 'NONE')} AS promotion_code,
           pharmacyorderid AS pharmacy_order_id, pharmacydeliveryid AS pharmacy_delivery_id,
           warehouselotid AS warehouse_lot_id, productionbatchid AS production_batch_id,
           prescriptionid AS prescription_id,
           {meas},
           _batch_id AS batch_id
    FROM raw_sales
    """)


def _demand_view(con):
    meas = ",\n".join(f"{conv(s, t)} AS {k}" for k, (s, t) in DEMAND_MEASURES.items())
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW v_demand AS
    SELECT hospitaldemandrecordid AS hospital_demand_record_id,
           TRY_CAST(demandmonth AS DATE) AS demand_month,
           hospitalid AS hosp_id, medicineid AS med_id,
           {u('medicationrequeststatus')} AS medication_request_status,
           {u('formularystatus')} AS formulary_status,
           {meas},
           _batch_id AS batch_id
    FROM raw_demand
    """)


# ---------------------------------------------------------------- dimensions
def load_dim_medicine():
    """SCD Type 2: price/cost/status change closes the old row and opens a new one."""
    with connect() as con:
        con.execute("""
        CREATE OR REPLACE TEMP VIEW stg_med AS
        SELECT medicineid AS med_id, gtin14, genericname AS med_name,
               fictionalbrandname AS brand_name, diseasecategory AS disease_category,
               icd10category AS icd10_category, atccode AS atc_code,
               therapeuticclass AS therapeutic_class, dosageform AS dosage_form, strength,
               routeofadministration AS route, prescriptionclass AS presc_class,
               controlledmedicineflag AS controlled_flag, coldchainflag AS cold_chain_flag,
               TRY_CAST(packsize AS INTEGER) AS pack_size,
               CASE WHEN TRY_CAST(packsize AS INTEGER) > 15 THEN 'Large' ELSE 'Small' END AS pack_size_band,
               TRY_CAST(shelflifemonths AS INTEGER) AS shelf_life_months,
               CASE WHEN TRY_CAST(shelflifemonths AS INTEGER) > 24 THEN 'Extended' ELSE 'Standard' END AS shelf_life_band,
               TRY_CAST(wholesalepriceusd AS DOUBLE) AS wholesale_price_usd,
               TRY_CAST(recommendedretailpriceusd AS DOUBLE) AS retail_price_usd,
               TRY_CAST(unitmanufacturingcostusd AS DOUBLE) AS unit_mfg_cost_usd,
               productstatus AS product_status,
               COALESCE(TRY_CAST(effectivefromdate AS DATE), DATE '2023-01-01') AS eff_from,
               md5(concat_ws('|', wholesalepriceusd, recommendedretailpriceusd,
                             unitmanufacturingcostusd, productstatus)) AS row_hash
        FROM raw_master
        QUALIFY row_number() OVER (PARTITION BY medicineid ORDER BY effectivefromdate DESC) = 1
        """)
        # 1) close current rows whose tracked attributes changed
        con.execute("""
        UPDATE dim_medicine d SET eff_to = s.eff_from, is_current = FALSE
        FROM stg_med s
        WHERE d.med_id = s.med_id AND d.is_current AND d.row_hash <> s.row_hash
          AND s.eff_from > d.eff_from
        """)
        # 2) open a new current row for new or just-closed medicines
        con.execute("""
        INSERT INTO dim_medicine (med_id, gtin14, med_name, brand_name, disease_category,
            icd10_category, atc_code, therapeutic_class, dosage_form, strength, route,
            presc_class, controlled_flag, cold_chain_flag, pack_size, pack_size_band,
            shelf_life_months, shelf_life_band, wholesale_price_usd, retail_price_usd,
            unit_mfg_cost_usd, product_status, eff_from, eff_to, is_current, row_hash)
        SELECT s.med_id, gtin14, med_name, brand_name, disease_category, icd10_category,
               atc_code, therapeutic_class, dosage_form, strength, route, presc_class,
               controlled_flag, cold_chain_flag, pack_size, pack_size_band, shelf_life_months,
               shelf_life_band, wholesale_price_usd, retail_price_usd, unit_mfg_cost_usd,
               product_status, eff_from, DATE '9999-12-31', TRUE, row_hash
        FROM stg_med s
        WHERE NOT EXISTS (SELECT 1 FROM dim_medicine d WHERE d.med_id = s.med_id AND d.is_current)
        """)
        return con.execute("SELECT count(*) FROM dim_medicine").fetchone()[0]


def load_dims():
    with connect() as con:
        _sale_view(con)
        _demand_view(con)

        # Hospital (SCD1)
        con.execute("""
        CREATE OR REPLACE TEMP VIEW stg_hosp AS
        SELECT d.hospitalid AS hosp_id, d.hospitalgln AS hospital_gln, d.hospitalname AS hosp_name,
               d.hospitaltype AS hosp_type, d.city, d.state, r.region,
               TRY_CAST(d.licensedbeds AS INTEGER) AS beds,
               CASE WHEN TRY_CAST(d.licensedbeds AS INTEGER) >= 700 THEN 'Large'
                    WHEN TRY_CAST(d.licensedbeds AS INTEGER) >= 500 THEN 'Medium'
                    ELSE 'Small' END AS size_band,
               TRY_CAST(d.icubeds AS INTEGER) AS icu_beds,
               ROUND(TRY_CAST(d.icubeds AS DOUBLE) / NULLIF(TRY_CAST(d.licensedbeds AS DOUBLE), 0), 3) AS icu_intensity,
               TRY_CAST(d.averageoccupancyratepct AS DOUBLE) AS occupancy_pct,
               COALESCE(NULLIF(TRIM(d.traumalevel), ''), 'Not designated') AS trauma_level,
               TRY_CAST(TRY_CAST(d.catchmentpopulation AS DOUBLE) AS BIGINT) AS catchment_pop,
               d.hospitalpharmacyid AS pharmacy_id
        FROM raw_demand d LEFT JOIN ref_state_region r ON d.state = r.state
        QUALIFY row_number() OVER (PARTITION BY d.hospitalid ORDER BY d.demandmonth DESC) = 1
        """)
        con.execute("""
        UPDATE dim_hospital d SET hospital_gln = s.hospital_gln, hosp_name = s.hosp_name,
            hosp_type = s.hosp_type, city = s.city, state = s.state, region = s.region,
            beds = s.beds, size_band = s.size_band, icu_beds = s.icu_beds,
            icu_intensity = s.icu_intensity, occupancy_pct = s.occupancy_pct,
            trauma_level = s.trauma_level, catchment_pop = s.catchment_pop,
            pharmacy_id = s.pharmacy_id
        FROM stg_hosp s WHERE d.hosp_id = s.hosp_id AND d.hosp_key <> -1
        """)
        con.execute("""
        INSERT INTO dim_hospital (hosp_id, hospital_gln, hosp_name, hosp_type, city, state, region,
            beds, size_band, icu_beds, icu_intensity, occupancy_pct, trauma_level, catchment_pop, pharmacy_id)
        SELECT hosp_id, hospital_gln, hosp_name, hosp_type, city, state, region, beds, size_band,
               icu_beds, icu_intensity, occupancy_pct, trauma_level, catchment_pop, pharmacy_id
        FROM stg_hosp s WHERE NOT EXISTS (SELECT 1 FROM dim_hospital d WHERE d.hosp_id = s.hosp_id)
        """)

        # Pharmacy (SCD1)
        con.execute(f"""
        CREATE OR REPLACE TEMP VIEW stg_pharm AS
        SELECT {u('s.pharmacyid', 'UNKNOWN')} AS pharm_id, s.pharmacygln AS pharmacy_gln,
               s.pharmacyname AS pharm_name,
               TRIM(REPLACE(s.pharmacytype, ' Pharmacy', '')) AS pharm_type,
               s.pharmacycity AS city, s.pharmacystate AS state, r.region,
               COALESCE(NULLIF(TRIM(s.hospitalid), ''), 'N/A') AS hosp_id,
               CASE WHEN NULLIF(TRIM(s.hospitalid), '') IS NULL THEN 'N' ELSE 'Y' END AS hosp_linked
        FROM raw_sales s LEFT JOIN ref_state_region r ON s.pharmacystate = r.state
        WHERE NULLIF(TRIM(s.pharmacyid), '') IS NOT NULL
        QUALIFY row_number() OVER (PARTITION BY s.pharmacyid ORDER BY s.salesdate DESC) = 1
        """)
        con.execute("""
        UPDATE dim_pharmacy d SET pharmacy_gln = s.pharmacy_gln, pharm_name = s.pharm_name,
            pharm_type = s.pharm_type, city = s.city, state = s.state, region = s.region,
            hosp_id = s.hosp_id, hosp_linked = s.hosp_linked
        FROM stg_pharm s WHERE d.pharm_id = s.pharm_id AND d.pharm_key <> -1
        """)
        con.execute("""
        INSERT INTO dim_pharmacy (pharm_id, pharmacy_gln, pharm_name, pharm_type, city, state,
            region, hosp_id, hosp_linked)
        SELECT pharm_id, pharmacy_gln, pharm_name, pharm_type, city, state, region, hosp_id, hosp_linked
        FROM stg_pharm s WHERE NOT EXISTS (SELECT 1 FROM dim_pharmacy d WHERE d.pharm_id = s.pharm_id)
        """)

        # Small dimensions and junk dimensions (insert unseen combinations only)
        con.execute("""
        INSERT INTO dim_sales_channel (sales_channel, channel_group)
        SELECT DISTINCT sales_channel,
               CASE WHEN sales_channel ILIKE '%hospital%' THEN 'Hospital'
                    WHEN sales_channel ILIKE '%commerce%' OR sales_channel ILIKE '%online%' THEN 'E-commerce'
                    ELSE 'Physical' END
        FROM v_sale v
        WHERE NOT EXISTS (SELECT 1 FROM dim_sales_channel d WHERE d.sales_channel = v.sales_channel)
        """)
        con.execute("""
        INSERT INTO dim_customer_segment (customer_segment)
        SELECT DISTINCT customer_segment FROM v_sale v
        WHERE NOT EXISTS (SELECT 1 FROM dim_customer_segment d WHERE d.customer_segment = v.customer_segment)
        """)
        con.execute("""
        INSERT INTO dim_payer_payment (payer_type, payment_method)
        SELECT DISTINCT payer_type, payment_method FROM v_sale v
        WHERE NOT EXISTS (SELECT 1 FROM dim_payer_payment d
                          WHERE d.payer_type = v.payer_type AND d.payment_method = v.payment_method)
        """)
        con.execute("""
        INSERT INTO dim_sale_profile (return_flag, sale_status, ecommerce_delivery_status,
                                      prescription_required_flag, promotion_code)
        SELECT DISTINCT return_flag, sale_status, ecommerce_delivery_status,
                        prescription_required_flag, promotion_code
        FROM v_sale v
        WHERE NOT EXISTS (SELECT 1 FROM dim_sale_profile d
            WHERE d.return_flag = v.return_flag AND d.sale_status = v.sale_status
              AND d.ecommerce_delivery_status = v.ecommerce_delivery_status
              AND d.prescription_required_flag = v.prescription_required_flag
              AND d.promotion_code = v.promotion_code)
        """)
        con.execute("""
        INSERT INTO dim_request_profile (medication_request_status, formulary_status)
        SELECT DISTINCT medication_request_status, formulary_status FROM v_demand v
        WHERE NOT EXISTS (SELECT 1 FROM dim_request_profile d
            WHERE d.medication_request_status = v.medication_request_status
              AND d.formulary_status = v.formulary_status)
        """)
    return "dimensions loaded"


# ---------------------------------------------------------------- facts
def load_fact_sale():
    with connect() as con:
        _sale_view(con)
        mcols = ", ".join(SALE_MEASURES)
        mvals = ", ".join(f"v.{c}" for c in SALE_MEASURES)
        con.execute(f"""
        INSERT INTO fact_pharmacy_sale
            (sales_transaction_id, sales_date_key, delivery_date_key, expiry_date_key,
             med_key, pharm_key, channel_key, segment_key, payer_key, sale_profile_key,
             pharmacy_order_id, pharmacy_delivery_id, warehouse_lot_id, production_batch_id,
             prescription_id, {mcols}, batch_id)
        SELECT v.sales_transaction_id,
               {dk('v.sales_date')}, {dk('v.delivery_date')}, {dk('v.expiry_date')},
               COALESCE(m.med_key, mc.med_key, -1), COALESCE(ph.pharm_key, -1),
               COALESCE(ch.channel_key, -1), COALESCE(cs.segment_key, -1),
               COALESCE(py.payer_key, -1), COALESCE(sp.sale_profile_key, -1),
               v.pharmacy_order_id, v.pharmacy_delivery_id, v.warehouse_lot_id,
               v.production_batch_id, v.prescription_id, {mvals}, v.batch_id
        FROM v_sale v
        ASOF LEFT JOIN dim_medicine m ON m.med_id = v.med_id AND v.sales_date >= m.eff_from
        LEFT JOIN dim_medicine mc ON mc.med_id = v.med_id AND mc.is_current
        LEFT JOIN dim_pharmacy ph ON ph.pharm_id = v.pharm_id
        LEFT JOIN dim_sales_channel ch ON ch.sales_channel = v.sales_channel
        LEFT JOIN dim_customer_segment cs ON cs.customer_segment = v.customer_segment
        LEFT JOIN dim_payer_payment py ON py.payer_type = v.payer_type AND py.payment_method = v.payment_method
        LEFT JOIN dim_sale_profile sp ON sp.return_flag = v.return_flag
             AND sp.sale_status = v.sale_status
             AND sp.ecommerce_delivery_status = v.ecommerce_delivery_status
             AND sp.prescription_required_flag = v.prescription_required_flag
             AND sp.promotion_code = v.promotion_code
        WHERE NOT EXISTS (SELECT 1 FROM fact_pharmacy_sale f
                          WHERE f.sales_transaction_id = v.sales_transaction_id)
        """)
        return con.execute("SELECT count(*) FROM fact_pharmacy_sale").fetchone()[0]


def load_fact_demand():
    with connect() as con:
        _demand_view(con)
        mcols = ", ".join(DEMAND_MEASURES)
        mvals = ", ".join(f"v.{c}" for c in DEMAND_MEASURES)
        con.execute(f"""
        INSERT INTO fact_hospital_demand
            (hospital_demand_record_id, hosp_key, med_key, demand_month_key, request_profile_key,
             {mcols}, batch_id)
        SELECT v.hospital_demand_record_id, COALESCE(h.hosp_key, -1),
               COALESCE(m.med_key, mc.med_key, -1), {dk('v.demand_month')},
               COALESCE(rp.request_profile_key, -1), {mvals}, v.batch_id
        FROM v_demand v
        ASOF LEFT JOIN dim_medicine m ON m.med_id = v.med_id AND v.demand_month >= m.eff_from
        LEFT JOIN dim_medicine mc ON mc.med_id = v.med_id AND mc.is_current
        LEFT JOIN dim_hospital h ON h.hosp_id = v.hosp_id
        LEFT JOIN dim_request_profile rp ON rp.medication_request_status = v.medication_request_status
             AND rp.formulary_status = v.formulary_status
        WHERE NOT EXISTS (SELECT 1 FROM fact_hospital_demand f
                          WHERE f.hospital_demand_record_id = v.hospital_demand_record_id)
        """)
        return con.execute("SELECT count(*) FROM fact_hospital_demand").fetchone()[0]


def commit_batches():
    with connect() as con:
        for src in ["master", "sales", "demand"]:
            con.execute(f"""
                INSERT INTO etl_loaded_batches (source, batch_id)
                SELECT DISTINCT '{src}', _batch_id FROM raw_{src}
            """)
    return "batches committed"


def validate():
    with connect() as con:
        out = {}
        for t in ["dim_date", "dim_medicine", "dim_hospital", "dim_pharmacy", "dim_sales_channel",
                  "dim_customer_segment", "dim_payer_payment", "dim_sale_profile",
                  "dim_request_profile", "fact_pharmacy_sale", "fact_hospital_demand"]:
            out[t] = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        out["sale_unknown_med"] = con.execute("SELECT count(*) FROM fact_pharmacy_sale WHERE med_key = -1").fetchone()[0]
        out["sale_unknown_pharm"] = con.execute("SELECT count(*) FROM fact_pharmacy_sale WHERE pharm_key = -1").fetchone()[0]
        out["demand_unknown_hosp"] = con.execute("SELECT count(*) FROM fact_hospital_demand WHERE hosp_key = -1").fetchone()[0]
        out["sale_dupes"] = con.execute(
            "SELECT count(*) - count(DISTINCT sales_transaction_id) FROM fact_pharmacy_sale").fetchone()[0]
        print(out)
        if out["sale_dupes"] > 0:
            raise ValueError(f"duplicate sales transactions: {out['sale_dupes']}")
        return out

# (src, type) per measure; shared by DDL and loaders
SALE_MEASURES = {
    "quantity_sold_packs": ("quantitysoldpacks", "INTEGER"),
    "unit_retail_price_usd": ("unitretailpriceusd", "DOUBLE"),
    "gross_sales_amount_usd": ("grosssalesamountusd", "DOUBLE"),
    "discount_amount_usd": ("discountamountusd", "DOUBLE"),
    "net_sales_amount_usd": ("netsalesamountusd", "DOUBLE"),
    "sales_tax_usd": ("salestaxusd", "DOUBLE"),
    "cogs_usd": ("costofgoodssoldusd", "DOUBLE"),
    "gross_margin_usd": ("grossmarginusd", "DOUBLE"),
    "returned_quantity_packs": ("returnedquantitypacks", "INTEGER"),
    "days_to_expiry_at_sale": ("daystoexpiryatsale", "INTEGER"),
}

DEMAND_MEASURES = {
    "estimated_disease_cases": ("estimateddiseasecases", "BIGINT"),
    "disease_related_admissions": ("diseaserelatedadmissions", "BIGINT"),
    "outpatient_encounters": ("outpatientencounters", "BIGINT"),
    "forecast_demand_packs": ("forecastdemandpacks", "BIGINT"),
    "requested_quantity_packs": ("requestedquantitypacks", "BIGINT"),
    "approved_quantity_packs": ("approvedquantitypacks", "BIGINT"),
    "received_quantity_packs": ("receivedquantitypacks", "BIGINT"),
    "emergency_request_packs": ("emergencyrequestquantitypacks", "BIGINT"),
    "opening_stock_packs": ("openingstockpacks", "BIGINT"),
    "dispensed_quantity_packs": ("dispensedquantitypacks", "BIGINT"),
    "consumed_quantity_packs": ("consumedquantitypacks", "BIGINT"),
    "wastage_quantity_packs": ("wastagequantitypacks", "BIGINT"),
    "closing_stock_packs": ("closingstockpacks", "BIGINT"),
    "unfulfilled_demand_packs": ("unfulfilleddemandpacks", "BIGINT"),
    "stockout_days": ("stockoutdays", "INTEGER"),
    "service_level_pct": ("servicelevelpct", "DOUBLE"),
    "avg_lead_time_days": ("averageleadtimedays", "DOUBLE"),
    "demand_trend_index": ("demandtrendindex", "DOUBLE"),
}


def conv(src, typ):
    if typ == "DOUBLE":
        return f"TRY_CAST({src} AS DOUBLE)"
    return f"TRY_CAST(TRY_CAST({src} AS DOUBLE) AS {typ})"


def _cols(m):
    return ",\n    ".join(f"{k} {v[1]}" for k, v in m.items())


DDL = f"""
CREATE SEQUENCE IF NOT EXISTS seq_med START 1;
CREATE SEQUENCE IF NOT EXISTS seq_hosp START 1;
CREATE SEQUENCE IF NOT EXISTS seq_pharm START 1;
CREATE SEQUENCE IF NOT EXISTS seq_channel START 1;
CREATE SEQUENCE IF NOT EXISTS seq_segment START 1;
CREATE SEQUENCE IF NOT EXISTS seq_payer START 1;
CREATE SEQUENCE IF NOT EXISTS seq_sale_profile START 1;
CREATE SEQUENCE IF NOT EXISTS seq_req_profile START 1;

CREATE TABLE IF NOT EXISTS etl_loaded_batches (
    source VARCHAR, batch_id VARCHAR, loaded_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY, full_date DATE,
    cal_year INTEGER, cal_quarter INTEGER, cal_month INTEGER,
    month_name VARCHAR, year_month VARCHAR,
    day_of_month INTEGER, day_of_week INTEGER, day_name VARCHAR,
    is_weekend BOOLEAN, week_of_year INTEGER
);

CREATE TABLE IF NOT EXISTS dim_medicine (
    med_key INTEGER DEFAULT nextval('seq_med'),
    med_id VARCHAR, gtin14 VARCHAR, med_name VARCHAR, brand_name VARCHAR,
    disease_category VARCHAR, icd10_category VARCHAR, atc_code VARCHAR,
    therapeutic_class VARCHAR, dosage_form VARCHAR, strength VARCHAR, route VARCHAR,
    presc_class VARCHAR, controlled_flag VARCHAR, cold_chain_flag VARCHAR,
    pack_size INTEGER, pack_size_band VARCHAR,
    shelf_life_months INTEGER, shelf_life_band VARCHAR,
    wholesale_price_usd DOUBLE, retail_price_usd DOUBLE, unit_mfg_cost_usd DOUBLE,
    product_status VARCHAR,
    eff_from DATE, eff_to DATE, is_current BOOLEAN, row_hash VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_hospital (
    hosp_key INTEGER DEFAULT nextval('seq_hosp'),
    hosp_id VARCHAR, hospital_gln VARCHAR, hosp_name VARCHAR, hosp_type VARCHAR,
    city VARCHAR, state VARCHAR, region VARCHAR,
    beds INTEGER, size_band VARCHAR, icu_beds INTEGER, icu_intensity DOUBLE,
    occupancy_pct DOUBLE, trauma_level VARCHAR, catchment_pop BIGINT, pharmacy_id VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_pharmacy (
    pharm_key INTEGER DEFAULT nextval('seq_pharm'),
    pharm_id VARCHAR, pharmacy_gln VARCHAR, pharm_name VARCHAR, pharm_type VARCHAR,
    city VARCHAR, state VARCHAR, region VARCHAR, hosp_id VARCHAR, hosp_linked VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_sales_channel (
    channel_key INTEGER DEFAULT nextval('seq_channel'),
    sales_channel VARCHAR, channel_group VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_customer_segment (
    segment_key INTEGER DEFAULT nextval('seq_segment'),
    customer_segment VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_payer_payment (
    payer_key INTEGER DEFAULT nextval('seq_payer'),
    payer_type VARCHAR, payment_method VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_sale_profile (
    sale_profile_key INTEGER DEFAULT nextval('seq_sale_profile'),
    return_flag VARCHAR, sale_status VARCHAR, ecommerce_delivery_status VARCHAR,
    prescription_required_flag VARCHAR, promotion_code VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_request_profile (
    request_profile_key INTEGER DEFAULT nextval('seq_req_profile'),
    medication_request_status VARCHAR, formulary_status VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_pharmacy_sale (
    sales_transaction_id VARCHAR,
    sales_date_key INTEGER, delivery_date_key INTEGER, expiry_date_key INTEGER,
    med_key INTEGER, pharm_key INTEGER, channel_key INTEGER, segment_key INTEGER,
    payer_key INTEGER, sale_profile_key INTEGER,
    pharmacy_order_id VARCHAR, pharmacy_delivery_id VARCHAR, warehouse_lot_id VARCHAR,
    production_batch_id VARCHAR, prescription_id VARCHAR,
    {_cols(SALE_MEASURES)},
    batch_id VARCHAR, loaded_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS fact_hospital_demand (
    hospital_demand_record_id VARCHAR,
    hosp_key INTEGER, med_key INTEGER, demand_month_key INTEGER, request_profile_key INTEGER,
    {_cols(DEMAND_MEASURES)},
    batch_id VARCHAR, loaded_at TIMESTAMP DEFAULT now()
);

INSERT INTO dim_medicine (med_key, med_id, med_name, eff_from, eff_to, is_current, row_hash)
SELECT -1, 'UNKNOWN', 'Unknown', DATE '1900-01-01', DATE '9999-12-31', TRUE, '-'
WHERE NOT EXISTS (SELECT 1 FROM dim_medicine WHERE med_key = -1);

INSERT INTO dim_hospital (hosp_key, hosp_id, hosp_name)
SELECT -1, 'UNKNOWN', 'Unknown' WHERE NOT EXISTS (SELECT 1 FROM dim_hospital WHERE hosp_key = -1);

INSERT INTO dim_pharmacy (pharm_key, pharm_id, pharm_name)
SELECT -1, 'UNKNOWN', 'Unknown' WHERE NOT EXISTS (SELECT 1 FROM dim_pharmacy WHERE pharm_key = -1);

INSERT INTO dim_date (date_key, month_name, year_month)
SELECT -1, 'Unknown', 'Unknown' WHERE NOT EXISTS (SELECT 1 FROM dim_date WHERE date_key = -1);
"""

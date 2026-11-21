================================================================================
                    OVERSAMPLED & SYNTHETIC DATA REPOSITORY
             PHARMACEUTICAL COLD CHAIN DATA WAREHOUSE & BI
================================================================================

This directory and the automated generation module (`src/generator/generate.py`)
provide the oversampled enterprise records designed to simulate continuous, live
pipeline operations across two distinct business quarters.

--------------------------------------------------------------------------------
1. GENERATION METHODOLOGY & ARCHITECTURE
--------------------------------------------------------------------------------
The synthetic data generator produces records for all 7 interconnected source files,
enforcing domain constraints, foreign-key relationships, and temporal logic:

- Random Number Generator: Seeded deterministically using NumPy PRNG (`seed = 42 + cycle`).
  Guarantees 100% reproducibility across evaluation runs.
- Temporal Progression:
  * Cycle 1: Spans July 1, 2026 to September 30, 2026 (Q3 2026).
  * Cycle 2: Spans October 1, 2026 to December 31, 2026 (Q4 2026).
- Volume Expansion:
  Each cycle adds exactly 46,635 records across the 7 files (~10% volume increase).
  Total oversampled records across both cycles = 93,270 rows.

--------------------------------------------------------------------------------
2. RECORD VOLUME BREAKDOWN PER FILE PER CYCLE
--------------------------------------------------------------------------------
Prefix / Target File               Cycle 1 Rows   Cycle 2 Rows   Total Generated
--------------------------------------------------------------------------------
01_master_data.csv                            5              5                10
02_manufacturing_data.csv                    50             50               100
03_manufacturer_delivery_data.csv            80             80               160
04_warehouse_data.csv                     5,000          5,000            10,000
05_pharmacy_sales_data.csv               25,000         25,000            50,000
06_hospital_demand_data.csv               1,500          1,500             3,000
07_iot_streaming_data.csv                15,000         15,000            30,000
--------------------------------------------------------------------------------
TOTAL PER REFRESH CYCLE                  46,635         46,635            93,270
--------------------------------------------------------------------------------

--------------------------------------------------------------------------------
3. SPECIAL DOMAIN BEHAVIORS & SCD-2 TRIGGER
--------------------------------------------------------------------------------
1. Slowly Changing Dimension (SCD Type 2) Mutation:
   In Cycle 2, `01_master_data` generates price mutations (+5% to +20%) for 5 medicines:
   - MED005, MED012, MED025, MED040, MED058.
   This tests the automated SCD-2 versioning engine in `dag_03_transform`.

2. Cold Chain Anomaly Injection:
   - 15% of IoT records in `07_iot_streaming_data` simulate temperature excursions (>8°C)
     to test operational alert triggers in the Streaming Ingestion Dashboard.

3. Customer Returns & Demand Surges:
   - 2% of sales transactions simulate returned items (`ReturnFlag = 'Y'`).
   - Emergency hospital demand orders simulate clinical inventory stockouts.

--------------------------------------------------------------------------------
4. HOW TO EXECUTE GENERATION
--------------------------------------------------------------------------------
Option A: Via Apache Airflow UI
1. Navigate to http://localhost:8080
2. Locate `dag_01_generate`
3. Click 'Trigger DAG with config'
4. Select `cycle: 1` or `cycle: 2` and click 'Trigger'

Option B: Via Command Line (Docker)
docker exec pharma_sales_dwh-airflow-scheduler-1 airflow dags trigger dag_01_generate -c '{"cycle": 1}'

Files are deposited directly into `data/landing/` for automated ingestion by `dag_02_ingestion`.

================================================================================

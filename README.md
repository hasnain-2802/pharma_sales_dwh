# Pharmaceutical Cold Supply Chain — DWH + BI Pipeline

Team Members: Hasnain Abbas (31593), Junaid Ahmed (30715)

## Architecture

```
CSV Source Files (7 files, 933K+ rows)
  │
  ▼  dag_01_generate (Data Oversampling)
  │  Generates synthetic data → data/landing/
  │
  ▼  dag_02_ingestion (File Watcher + Batch/Streaming Ingestion)
  │  Validates, loads via COPY → PostgreSQL staging (stg.*)
  │  Metrics → ops.batch_ingestion_log
  │
  ▼  dag_03_transform (Dimensional Modeling)
  │  Extract staging → DuckDB star schema
  │  SCD Type 2, Junk Dims, Role-Playing Dims
  │
  ▼  dag_04_query (Analytical + Constellation Queries)
  │  8 OLAP queries (base vs optimized) + 3 constellation queries
  │  Metrics → ops.query_log
  │
  ▼  Grafana Dashboards (4 auto-provisioned dashboards)
     Batch Ingestion │ Streaming │ Storage │ Query Performance
```

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10.5 (LocalExecutor) |
| Staging RDBMS | PostgreSQL 15 |
| OLAP Warehouse | DuckDB 1.0.0 |
| BI Dashboards | Grafana 10.4.2 (auto-provisioned) |
| Ingestion API | FastAPI + Uvicorn |
| Containerization | Docker Compose (6 services) |
| Language | Python 3.11 |

## Quick Start

```bash
# 1. Set Airflow UID
echo "AIRFLOW_UID=$(id -u)" > .env

# 2. Start all services
docker compose up -d

# 3. Access UIs
#    Airflow:  http://localhost:8080  (admin / admin)
#    Grafana:  http://localhost:3000  (admin / admin)
#    API:      http://localhost:8001/health
```

## Pipeline Flow (DAGs)

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `dag_01_generate` | Manual | Generate synthetic data for all 7 sources (cycle 1 or 2) |
| `dag_02_ingestion` | `*/5 * * * *` | File watcher → validate → bulk COPY to Postgres staging |
| `dag_03_transform` | Manual | Staging → DuckDB star schema (incremental ETL) |
| `dag_04_query` | Manual | Build aggregates → run 11 queries → log performance |

## Data Warehouse Features

### Dimensional Model (Star Schema in DuckDB)

**Dimensions (9):**
- `dim_medicine` — SCD Type 2 (eff_from, eff_to, is_current, row_hash)
- `dim_hospital` — SCD Type 1
- `dim_pharmacy` — SCD Type 1
- `dim_date` — with 4 role-playing views (sales, delivery, expiry, demand_month)
- `dim_sales_channel` — derived channel_group
- `dim_customer_segment`
- `dim_payer_payment` — **Junk dimension** (payer_type × payment_method)
- `dim_sale_profile` — **Junk dimension** (return_flag × sale_status × delivery_status × rx_flag × promo)
- `dim_request_profile` — **Junk dimension** (medication_request_status × formulary_status)

**Facts (2):**
- `fact_pharmacy_sale` — Transaction fact (500K+ rows, 10 measures)
- `fact_hospital_demand` — Periodic snapshot fact (27K+ rows, 18 measures)

### Query Optimization
- Pre-aggregated tables: `agg_sale_month`, `agg_demand_month`
- Index: `idx_fact_sale_txn` on sales_transaction_id
- Base vs Optimized query variants (8 queries, 3 repeats each)
- Constellation view: `v_constellation_medicine`

### Fact Constellation (Cross-Fact Analysis)
- **C1**: Supply-Demand Gap — joins sales + demand through dim_medicine
- **C2**: Regional Inventory Intelligence — joins sales + demand through dim_pharmacy/dim_hospital regions
- **C3**: Medicine 360° View — combines revenue, margin, forecast, service level per medicine

## Data Oversampling

| Source | Original Rows | Per Cycle | Strategy |
|---|---|---|---|
| 01_master_data | 63 | 5 | New SKUs (C1) / Price updates triggering SCD-2 (C2) |
| 02_manufacturing | 561 | 50 | New batch orders |
| 03_delivery | 999 | 80 | New shipment lines |
| 04_warehouse | 112,184 | 5,000 | Daily inventory snapshots |
| 05_sales | 500,000 | 25,000 | New transactions |
| 06_demand | 27,216 | 1,500 | Monthly hospital demand |
| 07_iot | 293,425 | 15,000 | Sensor readings |
| **Total** | **933,448** | **46,635** | **~10% increase over 2 cycles** |

## Ingestion API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness probe |
| `POST` | `/api/ingest` | Upload CSV → landing/ |
| `POST` | `/api/stream/iot` | JSON IoT readings → direct Postgres insert |
| `GET` | `/api/metrics/batch` | Batch ingestion statistics |
| `GET` | `/api/metrics/streaming` | Streaming/IoT statistics |
| `GET` | `/api/metrics/storage` | Staging table row counts |

## Grafana Dashboards

All dashboards are auto-provisioned on startup:

1. **Batch Ingestion Metrics** — load duration by source, rows processed, success rate, ingestion timeline
2. **Streaming Ingestion Metrics** — IoT records count, throughput, rejected records, duration over time
3. **Storage & Staging Metrics** — row distribution pie chart, table counts, date coverage
4. **Query Performance** — execution times (base vs optimized), error rate, daily query count, speedup ratios

## Project Structure

```
pharma_sales_dwh/
├── dags/                           # Airflow DAGs
│   ├── dag_00_hello.py             # Environment validation
│   ├── dag_01_generate.py          # Data oversampling
│   ├── dag_02_ingestion.py         # File watcher + batch ingestion
│   ├── dag_03_transform.py         # Staging → DuckDB star schema
│   └── dag_04_query.py             # Analytical + constellation queries
├── src/
│   ├── generator/generate.py       # Synthetic data generation (7 files)
│   ├── ingestion/
│   │   ├── api.py                  # FastAPI ingestion service
│   │   └── loader.py               # CSV validation + bulk COPY loading
│   ├── transform/
│   │   ├── ddl.py                  # Star schema DDL (dims, facts, sequences)
│   │   └── dwh.py                  # ETL: extract, SCD-2, dim/fact loading
│   └── query/
│       ├── queries.py              # 8 OLAP + 3 constellation queries
│       ├── runner.py               # Query execution + aggregates
│       └── logger.py               # Performance logging to Postgres
├── sql/ddl_staging.sql             # Staging schema DDL
├── docker/
│   ├── postgres-init/init.sql      # Database initialization
│   └── grafana/
│       ├── provisioning/           # Datasource + dashboard provider configs
│       └── dashboards/             # 4 JSON dashboard definitions
├── data/
│   ├── source/                     # 7 original CSV files (Git LFS)
│   ├── landing/                    # File watcher pickup directory
│   ├── archive/                    # Successfully ingested files
│   └── rejected/                   # Failed ingestion files
├── docker-compose.yml              # 6 services
├── Dockerfile                      # Airflow + Python deps
└── requirements.txt                # Python dependencies
```

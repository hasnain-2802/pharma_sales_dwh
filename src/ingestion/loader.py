import json
import os
import shutil
import time
from datetime import datetime
from io import StringIO
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
LANDING = DATA_DIR / "landing"
ARCHIVE = DATA_DIR / "archive"
REJECTED = DATA_DIR / "rejected"
CHUNK_ROWS = 50_000

SOURCES = {
    "01_master_data": {
        "table": "master_data",
        "not_null": ["MedicineID"],
        "positive": ["WholesalePriceUSD"],
        "dates": ["EffectiveFromDate"],
    },
    "02_manufacturing_data": {"table": "manufacturing_data", "not_null": ["ManufacturingOrderID"]},
    "03_manufacturer_delivery_data": {"table": "manufacturer_delivery_data", "not_null": ["ShipmentID"]},
    "04_warehouse_data": {"table": "warehouse_data", "not_null": ["InventorySnapshotID"]},
    "05_pharmacy_sales_data": {
        "table": "pharmacy_sales_data",
        "not_null": ["SalesTransactionID", "MedicineID"],
        "positive": ["UnitRetailPriceUSD", "QuantitySoldPacks"],
        "dates": ["SalesDate"],
    },
    "06_hospital_demand_data": {
        "table": "hospital_demand_data",
        "not_null": ["HospitalDemandRecordID", "HospitalID", "MedicineID"],
        "dates": ["DemandMonth"],
    },
    "07_iot_streaming_data": {"table": "iot_streaming_data", "not_null": ["SensorReadingID"]},
}


def get_engine():
    return create_engine(os.environ["STAGING_DB_URL"], pool_pre_ping=True)


def detect_source(filename):
    for prefix in SOURCES:
        if filename.startswith(prefix):
            return prefix
    raise ValueError(f"Unrecognised file name: {filename}")


def validate(df, rules):
    """Return (good_rows, bad_rows_with__reason)."""
    reason = pd.Series("", index=df.index)
    for c in rules.get("not_null", []):
        if c in df.columns:
            m = df[c].isna() | (df[c].astype(str).str.strip() == "")
            reason.loc[m] = reason.loc[m] + f"null_{c};"
    for c in rules.get("positive", []):
        if c in df.columns:
            v = pd.to_numeric(df[c], errors="coerce")
            m = v.isna() | (v <= 0)
            reason.loc[m] = reason.loc[m] + f"invalid_{c};"
    for c in rules.get("dates", []):
        if c in df.columns:
            m = pd.to_datetime(df[c], errors="coerce").isna()
            reason.loc[m] = reason.loc[m] + f"invalid_date_{c};"
    bad_mask = reason != ""
    good = df[~bad_mask]
    bad = df[bad_mask].copy()
    bad["_reason"] = reason[bad_mask]
    return good, bad


def _move(path, folder, new_name=None):
    folder.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), str(folder / (new_name or path.name)))


def ingest_file(path):
    path = Path(path)
    source = detect_source(path.name)
    cfg = SOURCES[source]
    table = cfg["table"]
    engine = get_engine()
    start_ts = datetime.now()
    batch_id = f"{path.stem}_{start_ts:%Y%m%d%H%M%S}"
    t0 = time.time()

    with engine.begin() as c:
        dup = c.execute(
            text("SELECT 1 FROM ops.batch_ingestion_log WHERE file_name=:f AND status='SUCCESS' LIMIT 1"),
            {"f": path.name},
        ).first()
    if dup:
        _move(path, ARCHIVE, f"{path.stem}_dup_{start_ts:%Y%m%d%H%M%S}{path.suffix}")
        return {"file": path.name, "status": "SKIPPED"}

    rows_read = rows_loaded = rows_rejected = 0
    status, err = "SUCCESS", None

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        header = pd.read_csv(path, nrows=0).columns.tolist()
        ddl = ", ".join(f'"{c.lower()}" TEXT' for c in header)
        cur.execute(
            f'CREATE TABLE IF NOT EXISTS stg."{table}" ({ddl}, '
            f'"_batch_id" TEXT, "_source_file" TEXT, "_ingested_at" TIMESTAMP DEFAULT now())'
        )
        collist = ", ".join(f'"{c.lower()}"' for c in header) + ', "_batch_id", "_source_file"'

        for chunk in pd.read_csv(path, dtype=str, keep_default_na=False,
                                 na_values=[""], chunksize=CHUNK_ROWS):
            rows_read += len(chunk)
            good, bad = validate(chunk, cfg)

            if len(good):
                out = good[header].copy()
                out["_batch_id"] = batch_id
                out["_source_file"] = path.name
                buf = StringIO()
                out.to_csv(buf, index=False, header=False, na_rep="")
                buf.seek(0)
                cur.copy_expert(f'COPY stg."{table}" ({collist}) FROM STDIN WITH (FORMAT csv)', buf)
                rows_loaded += len(good)

            if len(bad):
                recs = json.loads(bad.drop(columns="_reason").to_json(orient="records"))
                cur.executemany(
                    "INSERT INTO stg.rejected_records "
                    "(batch_id, source_name, file_name, reject_reason, row_data) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    [(batch_id, source, path.name, r, json.dumps(d))
                     for r, d in zip(bad["_reason"], recs)],
                )
                rows_rejected += len(bad)
        raw.commit()
    except Exception as e:
        raw.rollback()
        status, err = "FAILED", str(e)[:500]
        rows_loaded = 0
    finally:
        raw.close()

    end_ts = datetime.now()
    with engine.begin() as c:
        c.execute(
            text(
                "INSERT INTO ops.batch_ingestion_log "
                "(batch_id, source_name, file_name, start_ts, end_ts, duration_sec, "
                " rows_read, rows_loaded, rows_rejected, status, error_msg) "
                "VALUES (:b,:s,:f,:st,:et,:d,:rr,:rl,:rj,:status,:err)"
            ),
            {"b": batch_id, "s": source, "f": path.name, "st": start_ts, "et": end_ts,
             "d": round(time.time() - t0, 2), "rr": rows_read, "rl": rows_loaded,
             "rj": rows_rejected, "status": status, "err": err},
        )

    _move(path, ARCHIVE if status == "SUCCESS" else REJECTED)
    return {"file": path.name, "status": status, "rows_read": rows_read,
            "rows_loaded": rows_loaded, "rows_rejected": rows_rejected, "error": err}


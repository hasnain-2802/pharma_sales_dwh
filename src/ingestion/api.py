"""
Ingestion API
=============
FastAPI service providing:
  POST /api/ingest        – upload CSV files to landing/
  POST /api/stream/iot    – JSON body → direct Postgres insert (streaming simulation)
  GET  /api/metrics/batch – batch ingestion statistics
  GET  /api/metrics/streaming – streaming / IoT statistics
  GET  /api/metrics/storage   – staging table row counts
  GET  /health            – health check
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

app = FastAPI(title="Pharma DWH Ingestion API", version="1.0.0")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
LANDING = DATA_DIR / "landing"

STAGING_TABLES = [
    "master_data", "manufacturing_data", "manufacturer_delivery_data",
    "warehouse_data", "pharmacy_sales_data", "hospital_demand_data",
    "iot_streaming_data",
]


def _engine():
    return create_engine(os.environ["STAGING_DB_URL"], pool_pre_ping=True)


# ── Health ────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    """Basic liveness probe."""
    return {"status": "ok"}


# ── File Upload ───────────────────────────────────────────────────────
@app.post("/api/ingest")
async def upload_csv(file: UploadFile = File(...)):
    """
    Upload a CSV file to the landing directory.
    The file-watcher in dag_02_ingestion will pick it up automatically.
    """
    if not file.filename or not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only .csv files are accepted")

    LANDING.mkdir(parents=True, exist_ok=True)
    dest = LANDING / file.filename
    content = await file.read()
    dest.write_bytes(content)

    return {
        "status": "accepted",
        "filename": file.filename,
        "size_bytes": len(content),
        "landing_path": str(dest),
        "message": "File saved to landing/. The ingestion DAG will pick it up on next scan.",
    }


# ── Streaming IoT ────────────────────────────────────────────────────
class IoTReading(BaseModel):
    SensorReadingID: str
    DeviceID: str
    DeviceType: str = "Temperature-humidity logger"
    AssetType: str = "WarehouseZone"
    ShipmentID: str = ""
    SSCC18: str = ""
    WarehouseID: str = ""
    StorageZoneID: str = ""
    EventTimestampUTC: str = ""
    IngestionTimestampUTC: str = ""
    EventSequenceNumber: str = ""
    TemperatureC: float
    RelativeHumidityPct: float = 50.0
    Latitude: Optional[float] = None
    Longitude: Optional[float] = None
    DoorOpenFlag: str = "N"
    ShockG: float = 0.0
    BatteryLevelPct: float = 100.0
    SignalStrengthDbm: int = -50
    StorageMinTempC: float = 15
    StorageMaxTempC: float = 25
    MaxHumidityPct: float = 65
    TemperatureBreachFlag: str = "N"
    HumidityBreachFlag: str = "N"
    ThresholdBreachFlag: str = "N"
    AlertSeverity: str = "Normal"
    ReadingQuality: str = "Valid"
    DataLatencySeconds: int = 0
    LateArrivalFlag: str = "N"
    SourceStream: str = "api/pharma/stream/iot"


@app.post("/api/stream/iot")
def stream_iot(readings: List[IoTReading]):
    """
    Streaming-style IoT ingestion: accepts a JSON array of sensor readings
    and inserts them directly into Postgres staging.
    """
    if not readings:
        raise HTTPException(status_code=400, detail="Empty readings array")

    engine = _engine()
    batch_id = f"stream_iot_{datetime.now():%Y%m%d%H%M%S}"
    t0 = time.time()
    loaded = 0
    rejected = 0
    errors = []

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        # Ensure table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stg.iot_streaming_data (
                sensorreadingid TEXT, deviceid TEXT, devicetype TEXT,
                assettype TEXT, shipmentid TEXT, sscc18 TEXT,
                warehouseid TEXT, storagezoneid TEXT,
                eventtimestamputc TEXT, ingestiontimestamputc TEXT,
                eventsequencenumber TEXT, temperaturec TEXT,
                relativehumiditypct TEXT, latitude TEXT, longitude TEXT,
                dooropenflag TEXT, shockg TEXT, batterylevelpct TEXT,
                signalstrengthdbm TEXT, storageminttempc TEXT,
                storagemaxttempc TEXT, maxhumiditypct TEXT,
                temperaturebreachflag TEXT, humiditybreachflag TEXT,
                thresholdbreachflag TEXT, alertseverity TEXT,
                readingquality TEXT, datalatencyseconds TEXT,
                latearrivalflag TEXT, sourcestream TEXT,
                _batch_id TEXT, _source_file TEXT,
                _ingested_at TIMESTAMP DEFAULT now()
            )
        """)
        for r in readings:
            try:
                if not r.SensorReadingID or r.TemperatureC < -40.0 or r.TemperatureC > 90.0:
                    raise ValueError(f"Malformed telemetry: SensorReadingID='{r.SensorReadingID}', Temp={r.TemperatureC}C")
                vals = (
                    r.SensorReadingID, r.DeviceID, r.DeviceType, r.AssetType,
                    r.ShipmentID, r.SSCC18, r.WarehouseID, r.StorageZoneID,
                    r.EventTimestampUTC, r.IngestionTimestampUTC,
                    r.EventSequenceNumber, str(r.TemperatureC),
                    str(r.RelativeHumidityPct),
                    str(r.Latitude) if r.Latitude else "",
                    str(r.Longitude) if r.Longitude else "",
                    r.DoorOpenFlag, str(r.ShockG), str(r.BatteryLevelPct),
                    str(r.SignalStrengthDbm), str(r.StorageMinTempC),
                    str(r.StorageMaxTempC), str(r.MaxHumidityPct),
                    r.TemperatureBreachFlag, r.HumidityBreachFlag,
                    r.ThresholdBreachFlag, r.AlertSeverity, r.ReadingQuality,
                    str(r.DataLatencySeconds), r.LateArrivalFlag, r.SourceStream,
                    batch_id, "api_stream",
                )
                cur.execute(
                    "INSERT INTO stg.iot_streaming_data VALUES ("
                    + ",".join(["%s"] * len(vals)) + ")", vals,
                )
                loaded += 1
            except Exception as e:
                rejected += 1
                errors.append(str(e)[:200])
                try:
                    r_dict = r.dict() if hasattr(r, 'dict') else r.model_dump()
                    cur.execute(
                        "INSERT INTO stg.rejected_records "
                        "(batch_id, source_name, file_name, reject_reason, row_data) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        (batch_id, "07_iot_streaming_data", "api_stream", str(e)[:200], json.dumps(r_dict))
                    )
                except Exception as inner_e:
                    errors.append(f"insert_reject_fail: {inner_e}"[:100])
        raw.commit()
    except Exception as e:
        raw.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        raw.close()

    duration = round(time.time() - t0, 3)

    # Log metrics
    with engine.begin() as c:
        c.execute(text("""
            INSERT INTO ops.batch_ingestion_log
            (batch_id, source_name, file_name, start_ts, end_ts, duration_sec,
             rows_read, rows_loaded, rows_rejected, status, error_msg)
            VALUES (:b, :s, :f, :st, :et, :d, :rr, :rl, :rj, :status, :err)
        """), {
            "b": batch_id, "s": "07_iot_streaming_data", "f": "api_stream",
            "st": datetime.now(), "et": datetime.now(), "d": duration,
            "rr": len(readings), "rl": loaded, "rj": rejected,
            "status": "SUCCESS" if rejected == 0 else "PARTIAL",
            "err": "; ".join(errors[:3]) if errors else None,
        })

    throughput = round(loaded / max(duration, 0.001), 2)
    return {
        "status": "ok",
        "batch_id": batch_id,
        "records_received": len(readings),
        "records_loaded": loaded,
        "records_rejected": rejected,
        "duration_sec": duration,
        "throughput_records_per_sec": throughput,
    }


# ── Metrics ───────────────────────────────────────────────────────────
@app.get("/api/metrics/batch")
def metrics_batch() -> List[Dict[str, Any]]:
    """Batch ingestion statistics per source."""
    engine = _engine()
    with engine.connect() as c:
        rows = c.execute(text("""
            SELECT source_name,
                   count(*) AS batches,
                   sum(rows_loaded) AS total_rows,
                   round(avg(duration_sec)::numeric, 2) AS avg_duration_sec,
                   round(100.0 * count(*) FILTER (WHERE status = 'SUCCESS')
                         / count(*), 2) AS success_rate_pct
            FROM ops.batch_ingestion_log
            GROUP BY source_name
            ORDER BY source_name
        """)).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/api/metrics/streaming")
def metrics_streaming() -> Dict[str, Any]:
    """IoT / streaming ingestion statistics."""
    engine = _engine()
    with engine.connect() as c:
        row = c.execute(text("""
            SELECT count(*) AS batches,
                   sum(rows_loaded) AS total_records,
                   round(avg(CASE WHEN duration_sec > 0
                             THEN rows_loaded::numeric / duration_sec END)::numeric, 2)
                       AS avg_throughput_per_sec,
                   sum(rows_rejected) AS total_rejected
            FROM ops.batch_ingestion_log
            WHERE source_name = '07_iot_streaming_data'
        """)).first()
    return dict(row._mapping) if row else {}


@app.get("/api/metrics/storage")
def metrics_storage() -> List[Dict[str, Any]]:
    """Row counts for each staging table."""
    engine = _engine()
    results = []
    with engine.connect() as c:
        for table in STAGING_TABLES:
            try:
                cnt = c.execute(
                    text(f'SELECT count(*) AS n FROM stg."{table}"')
                ).scalar()
                results.append({"table": table, "rows": cnt})
            except Exception:
                results.append({"table": table, "rows": 0, "note": "table not created yet"})
    return results

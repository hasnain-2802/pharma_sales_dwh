import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
OUT = DATA_DIR / "oversampled" / "p1_pending"
N_SALES = 25000
WINDOW_DAYS = 45
MONTHS = 2

SALE_NUM = ["QuantitySoldPacks", "GrossSalesAmountUSD", "DiscountAmountUSD", "NetSalesAmountUSD",
            "SalesTaxUSD", "CostOfGoodsSoldUSD", "GrossMarginUSD"]
DEMAND_NUM = ["EstimatedDiseaseCases", "DiseaseRelatedAdmissions", "OutpatientEncounters",
              "ForecastDemandPacks", "RequestedQuantityPacks", "ApprovedQuantityPacks",
              "ReceivedQuantityPacks", "EmergencyRequestQuantityPacks", "OpeningStockPacks",
              "DispensedQuantityPacks", "ConsumedQuantityPacks", "WastageQuantityPacks",
              "ClosingStockPacks", "UnfulfilledDemandPacks"]


def _tmpl(name):
    for d in ("archive", "source"):
        p = DATA_DIR / d / name
        if p.exists():
            return pd.read_csv(p, dtype=str, keep_default_na=False)
    raise FileNotFoundError(name)


def _state():
    eng = create_engine(os.environ["STAGING_DB_URL"])
    with eng.connect() as c:
        s = lambda q: c.execute(text(q)).scalar()
        return {
            "sale_id": s("SELECT coalesce(max(substring(salestransactionid from '[0-9]+$')::bigint),0) FROM stg.pharmacy_sales_data"),
            "max_sale": s("SELECT max(salesdate::date) FROM stg.pharmacy_sales_data"),
            "dem_id": s("SELECT coalesce(max(substring(hospitaldemandrecordid from '[0-9]+$')::bigint),0) FROM stg.hospital_demand_data"),
            "max_month": s("SELECT max(demandmonth::date) FROM stg.hospital_demand_data"),
        }


def _scale(row, cols, k):
    for c in cols:
        try:
            row[c] = str(max(0, int(round(float(row[c]) * k))))
        except ValueError:
            pass


def _gen_sales(st, rng):
    t = _tmpl("05_pharmacy_sales_data.csv")
    t = t[(t["UnitRetailPriceUSD"].astype(float) > 0) & (t["QuantitySoldPacks"].astype(float) > 0)]
    sample = t.sample(N_SALES, random_state=rng.randint(0, 10**9)).copy()
    start = st["max_sale"] + timedelta(days=1)
    n = st["sale_id"]
    rows = []
    for rec in sample.to_dict("records"):
        n += 1
        d = start + timedelta(days=rng.randint(0, WINDOW_DAYS - 1))
        rec["SalesTransactionID"] = f"SALE-{n:09d}"
        rec["SalesDate"] = d.strftime("%Y-%m-%d")
        rec["SalesTimestampUTC"] = f"{d:%Y-%m-%d} {rng.randint(6,22):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}"
        rec["PharmacyOrderID"] = f"PHO-{n:07d}"
        rec["PharmacyDeliveryID"] = f"PHD-{n:07d}"
        rec["PharmacyDeliveryDate"] = (d - timedelta(days=rng.randint(1, 10))).strftime("%Y-%m-%d")
        if rec["PrescriptionID"]:
            rec["PrescriptionID"] = f"RX-{n:09d}"
        rows.append(rec)
    return pd.DataFrame(rows)[list(t.columns)], start, d


def _gen_demand(st, rng):
    t = _tmpl("06_hospital_demand_data.csv")
    last = t[t["DemandMonth"] == t["DemandMonth"].max()]
    n = st["dem_id"]
    month = st["max_month"]
    rows = []
    for _ in range(MONTHS):
        year, mon = (month.year + (month.month // 12), month.month % 12 + 1)
        month = month.replace(year=year, month=mon, day=1)
        for rec in last.to_dict("records"):
            n += 1
            rec = dict(rec)
            rec["HospitalDemandRecordID"] = f"HDEM-{n:08d}"
            rec["DemandMonth"] = month.strftime("%Y-%m-%d")
            _scale(rec, DEMAND_NUM, rng.uniform(0.85, 1.15))
            rows.append(rec)
    return pd.DataFrame(rows)[list(t.columns)], month


def generate_refresh(seed=None):
    st = _state()
    rng = random.Random(seed or int(datetime.now().timestamp()))
    OUT.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    sales, s0, s1 = _gen_sales(st, rng)
    dem, m_last = _gen_demand(st, rng)
    f5 = OUT / f"05_pharmacy_sales_data_p1_{ts}.csv"
    f6 = OUT / f"06_hospital_demand_data_p1_{ts}.csv"
    sales.to_csv(f5, index=False)
    dem.to_csv(f6, index=False)
    return {"sales_rows": len(sales), "demand_rows": len(dem),
            "sales_window": f"{s0} to {s1}", "last_demand_month": str(m_last),
            "files": [f5.name, f6.name]}


def ingest_pending():
    from src.ingestion.loader import ingest_file
    res = [ingest_file(p) for p in sorted(OUT.glob("*.csv"))]
    bad = [r for r in res if r["status"] == "FAILED"]
    if bad:
        raise RuntimeError(str(bad))
    return res

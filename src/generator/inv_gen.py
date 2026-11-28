import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
OUT = DATA_DIR / "oversampled" / "p2_pending"
N_SHIP = 150            # new shipment lines per refresh
WINDOW = 45             # days of snapshots per refresh
SHORT_DATED_SHARE = 0.15
FEFO_P = {"WH001": .95, "WH002": .93, "WH003": .94, "WH004": .90, "WH005": .92, "WH006": .82}


def _tmpl(name):
    for d in ("archive", "source"):
        p = DATA_DIR / d / name
        if p.exists():
            return pd.read_csv(p, dtype=str, keep_default_na=False)
    raise FileNotFoundError(name)


def _max_ids():
    eng = create_engine(os.environ["STAGING_DB_URL"])
    with eng.connect() as c:
        s = lambda q: c.execute(text(q)).scalar()
        return {
            "ship": s("SELECT coalesce(max(substring(shipmentid from '[0-9]+$')::bigint),0) FROM stg.manufacturer_delivery_data WHERE shipmentid LIKE 'MFG-SHP-%'"),
            "lot": s("SELECT coalesce(max(substring(warehouselotid from '[0-9]+$')::bigint),0) FROM stg.warehouse_data"),
            "inv": s("SELECT coalesce(max(substring(inventorysnapshotid from '[0-9]+$')::bigint),0) FROM stg.warehouse_data"),
            "last_snap": s("SELECT max(snapshotdate::date) FROM stg.warehouse_data"),
            "last_disp": s("SELECT max(dispatchdate::date) FROM stg.manufacturer_delivery_data"),
        }


def generate_refresh(seed=None):
    ids = _max_ids()
    rng = random.Random(seed or int(datetime.now().timestamp()))
    t03, t04 = _tmpl("03_manufacturer_delivery_data.csv"), _tmpl("04_warehouse_data.csv")
    wh_ids = sorted(t04["WarehouseID"].unique())
    start = max(ids["last_snap"], ids["last_disp"]) + timedelta(days=1)
    end = start + timedelta(days=WINDOW - 1)
    ship_n, lot_n, inv_n = ids["ship"], ids["lot"], ids["inv"]
    ship_rows, snap_rows = [], []
    iso = lambda d: d.strftime("%Y-%m-%d")

    for _ in range(N_SHIP):
        wh = rng.choice(wh_ids)
        s = t03[t03["DestinationWarehouseID"] == wh].sample(1, random_state=rng.randint(0, 10**9)).iloc[0].to_dict()
        w = t04[t04["WarehouseID"] == wh].sample(1, random_state=rng.randint(0, 10**9)).iloc[0].to_dict()
        ship_n += 1
        lot_n += 1
        disp = start + timedelta(days=rng.randint(0, 29))
        promised = disp + timedelta(days=rng.randint(2, 5))
        actual = promised + timedelta(days=rng.choice([0, 0, 0, 1, 2]))
        qty = rng.randint(2000, 15000)
        damaged = rng.choice([0, 0, 0, rng.randint(1, 60)])
        short = rng.choice([0, 0, 0, rng.randint(1, 200)])
        received = qty - damaged - short
        on_time = "Y" if actual <= promised else "N"
        in_full = "Y" if (damaged == 0 and short == 0) else "N"
        freight = round(qty * rng.uniform(0.12, 0.30), 2)
        s.update({
            "ShipmentID": f"MFG-SHP-{ship_n:07d}", "ShipmentLineNumber": "1",
            "SSCC18": f"95060010{ship_n:09d}", "SealNumber": f"SEAL-{ship_n:08d}",
            "DispatchDate": iso(disp), "PromisedDeliveryDate": iso(promised),
            "ActualDeliveryDate": iso(actual), "ShipmentStatus": "Delivered",
            "ShippedQuantityPacks": str(qty), "ReceivedQuantityPacks": str(received),
            "DamagedQuantityPacks": str(damaged), "ShortQuantityPacks": str(short),
            "TransitHours": str((actual - disp).days * 24),
            "OnTimeDeliveryFlag": on_time, "InFullDeliveryFlag": in_full,
            "OTIFFlag": "Y" if on_time == "Y" and in_full == "Y" else "N",
            "FreightCostUSD": str(freight), "FreightCostPerPackUSD": str(round(freight / qty, 4)),
        })
        ship_rows.append(s)

        short_dated = rng.random() < SHORT_DATED_SHARE
        expiry = actual + timedelta(days=rng.randint(20, 100) if short_dated else rng.randint(540, 720))
        lot = f"WHLOT-{lot_n:07d}"
        day, stock, first = actual, 0, True
        while day <= end:
            inv_n += 1
            days = (expiry - day).days
            opening = 0 if first else stock
            rec = received if first else 0
            if first:
                picked = exp_q = 0
            elif days < 0:
                picked, exp_q = 0, stock
            else:
                picked, exp_q = min(stock, int(stock * rng.uniform(0.01, 0.06))), 0
            closing = opening + rec - picked - exp_q
            if first:
                status = "Quarantined"
            elif days < 0:
                status = "Expired"
            elif closing == 0:
                status = "Depleted"
            elif days <= 90:
                status = "Near expiry"
            else:
                status = "Available"
            row = dict(w)
            row.update({
                "InventorySnapshotID": f"INV-{inv_n:09d}", "SnapshotDate": iso(day),
                "WarehouseLotID": lot, "InboundShipmentID": s["ShipmentID"],
                "InboundShipmentLineNumber": "1", "ProductionBatchID": s["ProductionBatchID"],
                "MedicineID": s["MedicineID"], "GTIN14": s["GTIN14"],
                "BatchExpiryDate": iso(expiry), "DaysToExpiry": str(days), "InventoryStatus": status,
                "OpeningQuantityPacks": str(opening), "ReceivedQuantityPacks": str(rec),
                "PutawayQuantityPacks": str(rec), "QuarantineQuantityPacks": str(rec),
                "AvailableQuantityPacks": "0" if (first or days < 0) else str(closing),
                "ReservedQuantityPacks": "0", "PickedQuantityPacks": str(picked),
                "ShippedQuantityPacks": str(picked), "TransferInQuantityPacks": "0",
                "TransferOutQuantityPacks": "0", "DamagedQuantityPacks": "0",
                "ExpiredQuantityPacks": str(exp_q), "AdjustmentQuantityPacks": "0",
                "ClosingQuantityPacks": str(closing),
                "StockoutFlag": "Y" if (closing == 0 and not first) else "N",
                "NearExpiryFlag": "Y" if (0 <= days <= 90 and closing > 0) else "N",
                "FEFOComplianceFlag": "Y" if rng.random() < FEFO_P.get(wh, .92) else "N",
            })
            snap_rows.append(row)
            stock, day, first = closing, day + timedelta(days=1), False

    OUT.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    f3, f4 = OUT / f"03_manufacturer_delivery_data_p2_{ts}.csv", OUT / f"04_warehouse_data_p2_{ts}.csv"
    pd.DataFrame(ship_rows)[list(t03.columns)].to_csv(f3, index=False)
    pd.DataFrame(snap_rows)[list(t04.columns)].to_csv(f4, index=False)
    return {"ship_rows": len(ship_rows), "snapshot_rows": len(snap_rows),
            "window": f"{start} to {end}", "files": [f3.name, f4.name]}


def ingest_pending():
    from src.ingestion.loader import ingest_file
    res = [ingest_file(p) for p in sorted(OUT.glob("*.csv"))]
    bad = [r for r in res if r["status"] == "FAILED"]
    if bad:
        raise RuntimeError(str(bad))
    return res

"""
Data Generator / Oversampling Module
=====================================
Generates synthetic records for all 7 source CSV files to simulate a
live 24/7 pipeline.  Each call to ``generate_all(cycle)`` writes new
CSVs into ``data/landing/`` where dag_02_ingestion picks them up.

Cycle 1  → dates  Jul – Sep 2025
Cycle 2  → dates  Oct – Dec 2025  (+ price changes → SCD‑2 trigger)
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

DATA_DIR = Path(os.environ.get("DATA_DIR", "/opt/airflow/data"))
SOURCE = DATA_DIR / "source"
LANDING = DATA_DIR / "landing"

# reproducibility per cycle
_RNG_BASE = 42

# ── helpers ───────────────────────────────────────────────────────────
def _rng(cycle: int):
    return np.random.default_rng(_RNG_BASE + cycle)


def _rand_dates(rng, start, end, n):
    """Return *n* random dates between *start* and *end* (inclusive)."""
    days = (end - start).days
    offsets = rng.integers(0, days + 1, size=n)
    return [start + timedelta(days=int(d)) for d in offsets]


def _ts(dt):
    """datetime  → string  '2025-07-15 14:23:45'"""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _date(dt):
    return dt.strftime("%Y-%m-%d")


def _load_source(name):
    return pd.read_csv(SOURCE / name, dtype=str, keep_default_na=False, na_values=[""])


# ── per-file generators ──────────────────────────────────────────────

def _gen_master(rng, cycle: int):
    """Cycle 1: 5 brand-new medicines.  Cycle 2: 5 price-updated rows (SCD-2 trigger)."""
    master = _load_source("01_master_data.csv")
    rows = []
    if cycle == 1:
        base_id = 63  # existing count
        categories = master["DiseaseCategory"].unique().tolist()
        classes = master["TherapeuticClass"].unique().tolist()
        forms = master["DosageForm"].unique().tolist()
        routes = master["RouteOfAdministration"].unique().tolist()
        strengths = master["Strength"].unique().tolist()
        plants = master["PrimaryPlantID"].unique().tolist()
        suppliers = master["PrimarySupplierID"].unique().tolist()
        for i in range(5):
            mid = base_id + i + 1
            med_id = f"MED{mid:03d}"
            cat = rng.choice(categories)
            rows.append({
                "MedicineID": med_id,
                "InternalSKU": f"SKU-{mid:02d}-{rng.integers(1000,9999):04d}",
                "GTIN14": f"950600{rng.integers(1000000,9999999)}",
                "GenericName": f"SyntheticDrug{mid}",
                "FictionalBrandName": f"SynthBrand{mid}",
                "DiseaseCategory": cat,
                "ICD10Category": rng.choice(master["ICD10Category"].unique()),
                "ATCCode": f"X{rng.integers(10,99)}{chr(65+i)}{rng.integers(10,99):02d}",
                "TherapeuticClass": rng.choice(classes),
                "DosageForm": rng.choice(forms),
                "Strength": rng.choice(strengths),
                "RouteOfAdministration": rng.choice(routes),
                "PackSize": str(rng.choice([10, 20, 28, 30, 60, 90])),
                "BaseUnitOfMeasure": "pack",
                "PrescriptionClass": rng.choice(["Prescription", "OTC"]),
                "ControlledMedicineFlag": rng.choice(["Y", "N"], p=[0.1, 0.9]),
                "ColdChainFlag": rng.choice(["Y", "N"], p=[0.25, 0.75]),
                "StorageMinTempC": str(rng.choice([2, 8, 15])),
                "StorageMaxTempC": str(rng.choice([8, 25, 30])),
                "MaxHumidityPct": str(rng.choice([60, 65, 70, 75])),
                "ShelfLifeMonths": str(rng.choice([12, 18, 24, 36])),
                "MinimumRemainingShelfLifeDays": str(rng.integers(90, 365)),
                "PrimarySupplierID": rng.choice(suppliers),
                "PrimaryPlantID": rng.choice(plants),
                "ManufacturingLeadTimeDays": str(rng.integers(14, 35)),
                "StandardBatchSizePacks": str(rng.choice([5000, 10000, 15000, 20000])),
                "SafetyStockDays": str(rng.integers(20, 50)),
                "ReorderReviewCycleDays": str(rng.choice([7, 14])),
                "UnitManufacturingCostUSD": f"{rng.uniform(0.3, 5.0):.2f}",
                "WholesalePriceUSD": f"{rng.uniform(1.5, 20.0):.2f}",
                "RecommendedRetailPriceUSD": f"{rng.uniform(2.0, 30.0):.2f}",
                "ProductStatus": "Active",
                "EffectiveFromDate": "2025-07-01",
                "SourceSystem": "ERP_PRODUCT_MASTER",
            })
    else:
        # Cycle 2: update prices for 5 existing medicines  → triggers SCD-2
        update_ids = ["MED005", "MED012", "MED025", "MED040", "MED058"]
        for med_id in update_ids:
            row = master[master["MedicineID"] == med_id].iloc[0].to_dict()
            old_w = float(row["WholesalePriceUSD"])
            old_r = float(row["RecommendedRetailPriceUSD"])
            row["WholesalePriceUSD"] = f"{old_w * rng.uniform(1.05, 1.20):.2f}"
            row["RecommendedRetailPriceUSD"] = f"{old_r * rng.uniform(1.05, 1.20):.2f}"
            row["EffectiveFromDate"] = "2025-10-01"
            rows.append(row)
    return pd.DataFrame(rows)


def _gen_manufacturing(rng, cycle: int):
    """50 new manufacturing orders per cycle."""
    master = _load_source("01_master_data.csv")
    med_ids = master["MedicineID"].tolist()
    gtins = dict(zip(master["MedicineID"], master["GTIN14"]))
    plants = master["PrimaryPlantID"].unique().tolist()
    batch_sizes = master["StandardBatchSizePacks"].dropna().astype(int).tolist()

    base_offset = 561 + (cycle - 1) * 50
    start_dt = datetime(2025, 7, 1) if cycle == 1 else datetime(2025, 10, 1)
    end_dt = datetime(2025, 9, 30) if cycle == 1 else datetime(2025, 12, 31)
    rows = []
    for i in range(50):
        idx = base_offset + i + 1
        med = rng.choice(med_ids)
        plant = rng.choice(plants)
        order_date = start_dt + timedelta(days=int(rng.integers(0, (end_dt - start_dt).days)))
        planned_start = order_date + timedelta(days=int(rng.integers(3, 10)))
        lead = int(rng.integers(14, 30))
        planned_end = planned_start + timedelta(days=lead)
        delay = int(rng.choice([0, 0, 0, 0, 1, 2, 3]))
        actual_start = planned_start + timedelta(days=int(rng.integers(0, 2)))
        actual_end = planned_end + timedelta(days=delay)
        std_size = int(rng.choice(batch_sizes)) if batch_sizes else 15000
        actual_qty = int(std_size * rng.uniform(0.93, 1.02))
        rejected = int(actual_qty * rng.uniform(0.01, 0.05))
        released = actual_qty - rejected
        yield_pct = round(100 * released / std_size, 2)
        labor = round(rng.uniform(500, 1500), 2)
        material = round(rng.uniform(3000, 10000), 2)
        overhead = round(rng.uniform(1000, 3000), 2)
        total_cost = round(labor + material + overhead, 2)
        cost_per = round(total_cost / max(released, 1), 4)
        rows.append({
            "ManufacturingOrderID": f"MO-2025-{idx:05d}",
            "ProductionBatchID": f"B25-{plant}-{idx:06d}",
            "MedicineID": med,
            "GTIN14": gtins.get(med, ""),
            "PlantID": plant,
            "OrderDate": _date(order_date),
            "DemandSignalSource": rng.choice(["Safety stock replenishment", "Forecast-driven",
                                               "Emergency order", "Seasonal build"]),
            "PlanningRunType": rng.choice(["Weekly MRP run", "Ad-hoc planning"]),
            "ManufacturingPriority": rng.choice(["High", "Medium", "Low"]),
            "PlannedStartDate": _date(planned_start),
            "PlannedCompletionDate": _date(planned_end),
            "ActualStartDate": _date(actual_start),
            "ActualCompletionDate": _date(actual_end),
            "PlannedQuantityPacks": str(std_size),
            "ActualQuantityPacks": str(actual_qty),
            "RejectedQuantityPacks": str(rejected),
            "ReleasedQuantityPacks": str(released),
            "YieldPct": str(yield_pct),
            "BatchStatus": "Released",
            "QualityTestStatus": rng.choice(["Passed", "Passed", "Passed", "Conditional"]),
            "QualitySampleDate": _date(actual_end + timedelta(days=1)),
            "BatchReleaseDate": _date(actual_end + timedelta(days=int(rng.integers(1, 4)))),
            "ExpiryDate": _date(actual_end + timedelta(days=int(rng.integers(365, 730)))),
            "StandardBatchSizePacks": str(std_size),
            "ManufacturingLeadTimeDays": str(lead),
            "SetupHours": f"{rng.uniform(2, 8):.1f}",
            "RunHours": f"{rng.uniform(8, 20):.1f}",
            "DowntimeHours": f"{rng.uniform(0, 4):.1f}",
            "DelayDays": str(delay),
            "DelayReason": rng.choice(["", "", "", "Equipment maintenance",
                                        "Material shortage", "Traffic disruption"]) if delay > 0 else "",
            "LaborCostUSD": str(labor),
            "MaterialCostUSD": str(material),
            "OverheadCostUSD": str(overhead),
            "TotalManufacturingCostUSD": str(total_cost),
            "CostPerReleasedPackUSD": str(cost_per),
            "DeviationFlag": rng.choice(["N", "N", "N", "Y"]),
            "DeviationCategory": "",
            "SourceSystem": "MES_QMS_INTEGRATED",
            "RecordCreatedTimestampUTC": _ts(actual_end + timedelta(days=int(rng.integers(2, 6)))),
        })
    return pd.DataFrame(rows)


def _gen_delivery(rng, cycle: int):
    """80 new shipment lines per cycle."""
    master = _load_source("01_master_data.csv")
    med_ids = master["MedicineID"].tolist()
    gtins = dict(zip(master["MedicineID"], master["GTIN14"]))
    warehouses = {
        "WH001": ("950600200010", "Northeast Distribution Center", "Philadelphia", "PA"),
        "WH002": ("950600200021", "Great Lakes Hub", "Chicago", "IL"),
        "WH003": ("950600200032", "Southern Distribution Center", "Dallas", "TX"),
        "WH004": ("950600200043", "Western Fulfillment Center", "Phoenix", "AZ"),
        "WH005": ("950600200054", "Pacific Coast Warehouse", "Los Angeles", "CA"),
        "WH006": ("950600200065", "Southeast Logistics Hub", "Atlanta", "GA"),
    }
    carriers = [
        ("CAR01", "ColdStar Logistics"), ("CAR02", "BlueLine Healthcare Freight"),
        ("CAR03", "MediTransit Express"), ("CAR04", "PharmaRoute National"),
    ]
    plants_gln = {"PLT01": "950600100000", "PLT02": "950600100011",
                  "PLT03": "950600100022", "PLT04": "950600100033"}
    plant_names = {"PLT01": "Clarity Pharma North Plant", "PLT02": "Clarity Pharma Central Plant",
                   "PLT03": "Clarity Pharma South Plant", "PLT04": "Clarity Pharma West Plant"}
    base_offset = 999 + (cycle - 1) * 80
    base_mo = 561 + (cycle - 1) * 50
    start_dt = datetime(2025, 7, 15) if cycle == 1 else datetime(2025, 10, 15)
    end_dt = datetime(2025, 9, 30) if cycle == 1 else datetime(2025, 12, 31)
    rows = []
    for i in range(80):
        idx = base_offset + i + 1
        med = rng.choice(med_ids)
        plant = rng.choice(list(plants_gln.keys()))
        wh_id = rng.choice(list(warehouses.keys()))
        wh_gln, wh_name, wh_city, wh_state = warehouses[wh_id]
        car_id, car_name = carriers[int(rng.integers(0, len(carriers)))]
        mo_idx = base_mo + int(rng.integers(1, 51))
        dispatch = start_dt + timedelta(days=int(rng.integers(0, (end_dt - start_dt).days)))
        transit_hrs = int(rng.integers(24, 168))
        promised = dispatch + timedelta(days=int(rng.integers(2, 5)))
        actual_delivery = promised + timedelta(days=int(rng.choice([-1, 0, 0, 0, 1, 2, 3])))
        on_time = "Y" if actual_delivery <= promised else "N"
        shipped = int(rng.integers(1000, 8000))
        damaged = int(rng.integers(0, max(1, shipped // 100)))
        short = int(rng.integers(0, max(1, shipped // 200)))
        received = shipped - damaged - short
        in_full = "Y" if received >= shipped * 0.98 else "N"
        otif = "Y" if on_time == "Y" and in_full == "Y" else "N"
        cold_flag = rng.choice(["Y", "N"], p=[0.25, 0.75])
        rows.append({
            "ShipmentID": f"MFG-SHP-{idx:07d}",
            "ShipmentLineNumber": "1",
            "SSCC18": f"9506001000{rng.integers(10000000, 99999999)}",
            "ManufacturingOrderID": f"MO-2025-{mo_idx:05d}",
            "ProductionBatchID": f"B25-{plant}-{mo_idx:06d}",
            "MedicineID": med,
            "GTIN14": gtins.get(med, ""),
            "OriginPlantID": plant,
            "OriginPlantGLN": plants_gln[plant],
            "OriginPlantName": plant_names[plant],
            "DestinationWarehouseID": wh_id,
            "DestinationWarehouseGLN": wh_gln,
            "DestinationWarehouseName": wh_name,
            "DestinationCity": wh_city,
            "DestinationState": wh_state,
            "DestinationCountryCode": "US",
            "CarrierID": car_id,
            "CarrierName": car_name,
            "TransportMode": rng.choice(["Road", "Road", "Air", "Multimodal"]),
            "ColdChainRequiredFlag": cold_flag,
            "DispatchDate": _date(dispatch),
            "PromisedDeliveryDate": _date(promised),
            "ActualDeliveryDate": _date(actual_delivery),
            "ShipmentStatus": "Delivered",
            "ShippedQuantityPacks": str(shipped),
            "ReceivedQuantityPacks": str(received),
            "DamagedQuantityPacks": str(damaged),
            "ShortQuantityPacks": str(short),
            "DistanceMiles": str(int(rng.integers(200, 2500))),
            "TransitHours": str(transit_hrs),
            "OnTimeDeliveryFlag": on_time,
            "InFullDeliveryFlag": in_full,
            "OTIFFlag": otif,
            "FreightCostUSD": f"{rng.uniform(500, 3000):.2f}",
            "FreightCostPerPackUSD": f"{rng.uniform(0.05, 0.5):.4f}",
            "SealNumber": f"SEAL-{rng.integers(10000000, 99999999)}",
            "TemperatureMonitoringRequiredFlag": cold_flag,
            "TemperatureExcursionFlag": rng.choice(["N", "N", "N", "Y"]),
            "ProofOfDeliveryFlag": "Y",
            "DeliveryExceptionReason": rng.choice(["", "", "", "Traffic disruption",
                                                    "Weather delay", "Dock congestion"]),
            "EPCISEventType": "ShippingEvent / ReceivingEvent",
            "SourceSystem": "TMS_EPCIS_GATEWAY",
        })
    return pd.DataFrame(rows)


def _gen_warehouse(rng, cycle: int):
    """5 000 new daily inventory snapshots per cycle."""
    master = _load_source("01_master_data.csv")
    med_ids = master["MedicineID"].tolist()
    gtins = dict(zip(master["MedicineID"], master["GTIN14"]))
    warehouses = {
        "WH001": ("950600200010", "Northeast Distribution Center"),
        "WH002": ("950600200021", "Great Lakes Hub"),
        "WH003": ("950600200032", "Southern Distribution Center"),
        "WH004": ("950600200043", "Western Fulfillment Center"),
        "WH005": ("950600200054", "Pacific Coast Warehouse"),
        "WH006": ("950600200065", "Southeast Logistics Hub"),
    }
    zones_per_wh = {"WH001": ["WH001-AMB-01", "WH001-COLD-01"],
                    "WH002": ["WH002-AMB-01", "WH002-COLD-01"],
                    "WH003": ["WH003-AMB-01", "WH003-AMB-03"],
                    "WH004": ["WH004-AMB-01", "WH004-COLD-01"],
                    "WH005": ["WH005-AMB-01"],
                    "WH006": ["WH006-AMB-01", "WH006-COLD-01"]}
    base_id = 112184 + (cycle - 1) * 5000
    start_dt = datetime(2025, 7, 1) if cycle == 1 else datetime(2025, 10, 1)
    end_dt = datetime(2025, 9, 30) if cycle == 1 else datetime(2025, 12, 31)
    rows = []
    for i in range(5000):
        idx = base_id + i + 1
        wh_id = rng.choice(list(warehouses.keys()))
        wh_gln, wh_name = warehouses[wh_id]
        zone = rng.choice(zones_per_wh[wh_id])
        med = rng.choice(med_ids)
        snap_date = start_dt + timedelta(days=int(rng.integers(0, (end_dt - start_dt).days)))
        expiry = snap_date + timedelta(days=int(rng.integers(90, 700)))
        days_to_exp = (expiry - snap_date).days
        opening = int(rng.integers(100, 5000))
        received = int(rng.integers(0, 2000)) if rng.random() < 0.3 else 0
        total_avail = opening + received
        shipped = int(rng.integers(0, min(total_avail, 1500) + 1))
        closing = max(0, total_avail - shipped)
        stockout = "Y" if closing == 0 else "N"
        near_expiry = "Y" if days_to_exp < 90 else "N"
        storage_cond = "2-8 C" if "COLD" in zone else "15-25 C"
        rows.append({
            "InventorySnapshotID": f"INV-{idx:09d}",
            "SnapshotDate": _date(snap_date),
            "WarehouseLotID": f"WHLOT-{rng.integers(1, 2000):07d}",
            "InboundShipmentID": f"MFG-SHP-{rng.integers(1, 1200):07d}",
            "InboundShipmentLineNumber": "1",
            "ProductionBatchID": f"B25-PLT{rng.integers(1,5):02d}-{rng.integers(1,700):06d}",
            "MedicineID": med,
            "GTIN14": gtins.get(med, ""),
            "WarehouseID": wh_id,
            "WarehouseGLN": wh_gln,
            "WarehouseName": wh_name,
            "StorageZoneID": zone,
            "StorageCondition": storage_cond,
            "BatchExpiryDate": _date(expiry),
            "DaysToExpiry": str(days_to_exp),
            "InventoryStatus": rng.choice(["Available", "Available", "Available", "Quarantined", "Reserved"]),
            "OpeningQuantityPacks": str(opening),
            "ReceivedQuantityPacks": str(received),
            "PutawayQuantityPacks": str(received),
            "QuarantineQuantityPacks": str(int(rng.integers(0, max(1, received // 20)))),
            "AvailableQuantityPacks": str(max(0, closing - int(rng.integers(0, 100)))),
            "ReservedQuantityPacks": str(int(rng.integers(0, 200))),
            "PickedQuantityPacks": str(int(rng.integers(0, max(1, shipped)))),
            "ShippedQuantityPacks": str(shipped),
            "TransferInQuantityPacks": "0",
            "TransferOutQuantityPacks": "0",
            "DamagedQuantityPacks": str(int(rng.integers(0, 10))),
            "ExpiredQuantityPacks": "0",
            "AdjustmentQuantityPacks": "0",
            "ClosingQuantityPacks": str(max(0, closing)),
            "StockoutFlag": stockout,
            "NearExpiryFlag": near_expiry,
            "FEFOComplianceFlag": rng.choice(["Y", "Y", "Y", "N"]),
            "CycleCountPerformedFlag": rng.choice(["Y", "N"]),
            "CycleCountVariancePacks": str(int(rng.integers(0, 5))),
            "DockToStockHours": f"{rng.uniform(2, 24):.1f}",
            "OrderPickingAccuracyPct": f"{rng.uniform(95, 100):.1f}",
            "AllocatedBinCapacityPacks": str(int(rng.integers(5000, 15000))),
            "WarehouseTotalCapacityPacks": "900000",
            "WarehouseActivityStatus": rng.choice(["Normal operations", "Receive and putaway",
                                                    "Pick and ship", "Cycle count"]),
            "SourceSystem": "WMS_DAILY_SNAPSHOT",
        })
    return pd.DataFrame(rows)


def _gen_sales(rng, cycle: int):
    """25 000 new pharmacy sales transactions per cycle."""
    master = _load_source("01_master_data.csv")
    sales = _load_source("05_pharmacy_sales_data.csv")
    med_ids = master["MedicineID"].tolist()
    gtins = dict(zip(master["MedicineID"], master["GTIN14"]))
    med_names = dict(zip(master["MedicineID"], master["GenericName"]))
    med_cats = dict(zip(master["MedicineID"], master["DiseaseCategory"]))

    # sample pharmacies from existing data
    pharm_cols = ["PharmacyID", "PharmacyGLN", "PharmacyName", "PharmacyType",
                  "HospitalID", "PharmacyCity", "PharmacyState"]
    pharmacies = sales[pharm_cols].drop_duplicates(subset=["PharmacyID"]).head(50).to_dict("records")

    channels = ["Physical Pharmacy", "Hospital Pharmacy", "E-commerce Pharmacy"]
    segments = ["Adult chronic care", "Elderly polypharmacy", "Pediatric care",
                "Surgical/post-op", "General acute care"]
    payers = ["Private insurance", "Government program", "Self-pay", "Employer plan"]
    payments = ["Cash", "Credit card", "Insurance claim", "Digital wallet"]
    promos = ["NONE", "LOYALTY5", "SEASONAL10", "NEWCUST15", "HEALTH20"]

    base_id = 500000 + (cycle - 1) * 25000
    start_dt = datetime(2025, 7, 1) if cycle == 1 else datetime(2025, 10, 1)
    end_dt = datetime(2025, 9, 30) if cycle == 1 else datetime(2025, 12, 31)
    rows = []
    for i in range(25000):
        idx = base_id + i + 1
        med = rng.choice(med_ids)
        pharm = pharmacies[int(rng.integers(0, len(pharmacies)))]
        sale_date = start_dt + timedelta(days=int(rng.integers(0, (end_dt - start_dt).days)))
        sale_time = sale_date + timedelta(hours=int(rng.integers(6, 22)),
                                          minutes=int(rng.integers(0, 60)),
                                          seconds=int(rng.integers(0, 60)))
        channel = rng.choice(channels)
        qty = int(rng.integers(1, 10))
        unit_price = round(rng.uniform(1.5, 50.0), 2)
        gross = round(qty * unit_price, 2)
        discount = round(gross * rng.uniform(0, 0.15), 2)
        net = round(gross - discount, 2)
        tax = round(net * rng.uniform(0.04, 0.10), 2)
        cogs = round(net * rng.uniform(0.4, 0.7), 2)
        margin = round(net - cogs, 2)
        return_flag = rng.choice(["N", "N", "N", "N", "Y"])
        returned_qty = int(rng.integers(1, qty + 1)) if return_flag == "Y" else 0
        delivery_date = sale_date - timedelta(days=int(rng.integers(1, 30)))
        expiry = sale_date + timedelta(days=int(rng.integers(60, 700)))
        ecomm_status = "Not applicable"
        if "E-commerce" in channel:
            ecomm_status = rng.choice(["Delivered", "Delivered", "Delivered",
                                        "In transit", "Returned to sender"])
        rows.append({
            "SalesTransactionID": f"SALE-{idx:09d}",
            "SalesDate": _date(sale_date),
            "SalesTimestampUTC": _ts(sale_time),
            "SalesChannel": channel,
            "PharmacyID": pharm["PharmacyID"],
            "PharmacyGLN": pharm["PharmacyGLN"],
            "PharmacyName": pharm["PharmacyName"],
            "PharmacyType": pharm["PharmacyType"],
            "HospitalID": pharm.get("HospitalID", ""),
            "PharmacyCity": pharm["PharmacyCity"],
            "PharmacyState": pharm["PharmacyState"],
            "CountryCode": "US",
            "PharmacyOrderID": f"PHO-{rng.integers(100000, 999999)}",
            "PharmacyDeliveryID": f"PHD-{rng.integers(100000, 999999)}",
            "PharmacyDeliveryDate": _date(delivery_date),
            "DeliveryQuantityPacks": str(qty),
            "DeliveryOTIFFlag": rng.choice(["Y", "Y", "Y", "N"]),
            "WarehouseLotID": f"WHLOT-{rng.integers(1, 2000):07d}",
            "SourceWarehouseID": rng.choice(["WH001", "WH002", "WH003", "WH004", "WH005", "WH006"]),
            "ProductionBatchID": f"B25-PLT{rng.integers(1,5):02d}-{rng.integers(1,700):06d}",
            "MedicineID": med,
            "GTIN14": gtins.get(med, ""),
            "DiseaseCategory": med_cats.get(med, ""),
            "GenericName": med_names.get(med, ""),
            "BatchExpiryDate": _date(expiry),
            "DaysToExpiryAtSale": str((expiry - sale_date).days),
            "QuantitySoldPacks": str(qty),
            "UnitRetailPriceUSD": str(unit_price),
            "GrossSalesAmountUSD": str(gross),
            "DiscountAmountUSD": str(discount),
            "NetSalesAmountUSD": str(net),
            "SalesTaxUSD": str(tax),
            "CostOfGoodsSoldUSD": str(cogs),
            "GrossMarginUSD": str(margin),
            "PromotionCode": rng.choice(promos),
            "PrescriptionRequiredFlag": rng.choice(["Y", "N"]),
            "PrescriptionID": f"RX-{idx:09d}" if rng.random() > 0.3 else "",
            "CustomerSegment": rng.choice(segments),
            "PayerType": rng.choice(payers),
            "PaymentMethod": rng.choice(payments),
            "EcommerceDeliveryStatus": ecomm_status,
            "ReturnFlag": return_flag,
            "ReturnedQuantityPacks": str(returned_qty),
            "SaleStatus": "Completed",
            "SourceSystem": "PHARMACY_POS",
        })
    return pd.DataFrame(rows)


def _gen_demand(rng, cycle: int):
    """1 500 new hospital demand records per cycle."""
    demand = _load_source("06_hospital_demand_data.csv")
    master = _load_source("01_master_data.csv")
    med_ids = master["MedicineID"].tolist()
    gtins = dict(zip(master["MedicineID"], master["GTIN14"]))
    med_names = dict(zip(master["MedicineID"], master["GenericName"]))
    med_cats = dict(zip(master["MedicineID"], master["DiseaseCategory"]))
    med_icd = dict(zip(master["MedicineID"], master["ICD10Category"]))

    hosp_cols = ["HospitalID", "HospitalGLN", "HospitalName", "HospitalType",
                 "HospitalPharmacyID", "City", "State", "LicensedBeds", "ICUBeds",
                 "AverageOccupancyRatePct", "TraumaLevel", "CatchmentPopulation"]
    hospitals = demand[hosp_cols].drop_duplicates(subset=["HospitalID"]).to_dict("records")

    base_id = 27216 + (cycle - 1) * 1500
    months = ["2025-07-01", "2025-08-01", "2025-09-01"] if cycle == 1 else \
             ["2025-10-01", "2025-11-01", "2025-12-01"]
    rows = []
    for i in range(1500):
        idx = base_id + i + 1
        med = rng.choice(med_ids)
        hosp = hospitals[int(rng.integers(0, len(hospitals)))]
        forecast = int(rng.integers(50, 500))
        requested = int(forecast * rng.uniform(0.8, 1.2))
        approved = int(requested * rng.uniform(0.85, 1.0))
        received = int(approved * rng.uniform(0.85, 1.0))
        emergency = int(rng.integers(0, max(1, forecast // 10)))
        opening = int(rng.integers(100, 800))
        dispensed = int(rng.integers(50, received + opening))
        consumed = int(dispensed * rng.uniform(0.9, 1.0))
        wastage = int(rng.integers(0, max(1, dispensed // 20)))
        closing = max(0, opening + received - dispensed - wastage)
        unfulfilled = max(0, forecast - received)
        stockout_days = int(rng.integers(0, 5)) if unfulfilled > 0 else 0
        service_level = round(min(100, received / max(forecast, 1) * 100), 1)
        rows.append({
            "HospitalDemandRecordID": f"HDEM-{idx:08d}",
            "DemandMonth": rng.choice(months),
            "HospitalID": hosp["HospitalID"],
            "HospitalGLN": hosp["HospitalGLN"],
            "HospitalName": hosp["HospitalName"],
            "HospitalType": hosp["HospitalType"],
            "HospitalPharmacyID": hosp["HospitalPharmacyID"],
            "City": hosp["City"],
            "State": hosp["State"],
            "CountryCode": "US",
            "LicensedBeds": hosp["LicensedBeds"],
            "ICUBeds": hosp["ICUBeds"],
            "AverageOccupancyRatePct": hosp["AverageOccupancyRatePct"],
            "TraumaLevel": hosp["TraumaLevel"],
            "CatchmentPopulation": hosp["CatchmentPopulation"],
            "MedicineID": med,
            "GTIN14": gtins.get(med, ""),
            "GenericName": med_names.get(med, ""),
            "DiseaseCategory": med_cats.get(med, ""),
            "ICD10Category": med_icd.get(med, ""),
            "EstimatedDiseaseCases": str(int(rng.integers(500, 5000))),
            "DiseaseRelatedAdmissions": str(int(rng.integers(200, 3000))),
            "OutpatientEncounters": str(int(rng.integers(1000, 8000))),
            "ForecastDemandPacks": str(forecast),
            "RequestedQuantityPacks": str(requested),
            "ApprovedQuantityPacks": str(approved),
            "ReceivedQuantityPacks": str(received),
            "EmergencyRequestQuantityPacks": str(emergency),
            "OpeningStockPacks": str(opening),
            "DispensedQuantityPacks": str(dispensed),
            "ConsumedQuantityPacks": str(consumed),
            "WastageQuantityPacks": str(wastage),
            "ClosingStockPacks": str(closing),
            "UnfulfilledDemandPacks": str(unfulfilled),
            "StockoutDays": str(stockout_days),
            "ServiceLevelPct": str(service_level),
            "MedicationRequestStatus": rng.choice(["Completed", "Completed", "Completed",
                                                     "Partial", "Pending"]),
            "FormularyStatus": rng.choice(["Formulary", "Formulary", "Non-formulary", "Restricted"]),
            "AverageLeadTimeDays": f"{rng.uniform(5, 20):.1f}",
            "DemandTrendIndex": f"{rng.uniform(0.8, 1.5):.3f}",
            "SourceSystem": "HOSPITAL_ERP_FHIR_EXPORT",
        })
    return pd.DataFrame(rows)


def _gen_iot(rng, cycle: int):
    """15 000 new IoT sensor readings per cycle."""
    base_id = 293425 + (cycle - 1) * 15000
    devices_wh = [
        ("IOT-WH001-AMB-01", "WH001", "WH001-AMB-01"),
        ("IOT-WH001-COLD-01", "WH001", "WH001-COLD-01"),
        ("IOT-WH002-AMB-01", "WH002", "WH002-AMB-01"),
        ("IOT-WH003-AMB-01", "WH003", "WH003-AMB-01"),
        ("IOT-WH004-AMB-01", "WH004", "WH004-AMB-01"),
        ("IOT-WH004-COLD-01", "WH004", "WH004-COLD-01"),
        ("IOT-WH005-AMB-01", "WH005", "WH005-AMB-01"),
        ("IOT-WH006-AMB-01", "WH006", "WH006-AMB-01"),
    ]
    devices_ship = [
        ("IOT-SHIP-001", "MFG-SHP-0001001"),
        ("IOT-SHIP-002", "MFG-SHP-0001002"),
        ("IOT-SHIP-003", "MFG-SHP-0001003"),
    ]
    start_dt = datetime(2025, 7, 1) if cycle == 1 else datetime(2025, 10, 1)
    end_dt = datetime(2025, 9, 30) if cycle == 1 else datetime(2025, 12, 31)
    rows = []
    for i in range(15000):
        idx = base_id + i + 1
        is_wh = rng.random() > 0.15
        if is_wh:
            dev_id, wh_id, zone = devices_wh[int(rng.integers(0, len(devices_wh)))]
            ship_id, sscc = "", ""
            asset_type = "WarehouseZone"
            lat = round(rng.uniform(25, 48), 4)
            lon = round(rng.uniform(-120, -70), 4)
            is_cold = "COLD" in zone
            min_t, max_t = (2, 8) if is_cold else (15, 25)
        else:
            dev_id, ship_id = devices_ship[int(rng.integers(0, len(devices_ship)))]
            sscc = f"9506001000{rng.integers(10000000, 99999999)}"
            wh_id, zone = "", ""
            asset_type = "ShipmentContainer"
            lat = round(rng.uniform(25, 48), 4)
            lon = round(rng.uniform(-120, -70), 4)
            is_cold = rng.random() > 0.5
            min_t, max_t = (2, 8) if is_cold else (15, 25)

        event_ts = start_dt + timedelta(seconds=int(rng.integers(0, int((end_dt - start_dt).total_seconds()))))
        latency_sec = int(rng.integers(5, 120))
        ingest_ts = event_ts + timedelta(seconds=latency_sec)

        # Temperature: mostly normal, occasional breach
        temp_mean = (min_t + max_t) / 2
        temp = round(rng.normal(temp_mean, 2.0), 2)
        humidity = round(rng.uniform(40, 75), 2)
        temp_breach = "Y" if (temp < min_t or temp > max_t) else "N"
        hum_breach = "Y" if humidity > 70 else "N"
        threshold_breach = "Y" if temp_breach == "Y" or hum_breach == "Y" else "N"
        alert = "Critical" if temp_breach == "Y" else ("Warning" if hum_breach == "Y" else "Normal")

        rows.append({
            "SensorReadingID": f"IOTR-{idx:010d}",
            "DeviceID": dev_id,
            "DeviceType": "Temperature-humidity logger",
            "AssetType": asset_type,
            "ShipmentID": ship_id,
            "SSCC18": sscc,
            "WarehouseID": wh_id,
            "StorageZoneID": zone,
            "EventTimestampUTC": _ts(event_ts),
            "IngestionTimestampUTC": _ts(ingest_ts),
            "EventSequenceNumber": str(i + 1),
            "TemperatureC": str(temp),
            "RelativeHumidityPct": str(humidity),
            "Latitude": str(lat),
            "Longitude": str(lon),
            "DoorOpenFlag": rng.choice(["N", "N", "N", "Y"]),
            "ShockG": f"{rng.uniform(0, 0.5):.3f}",
            "BatteryLevelPct": f"{rng.uniform(60, 100):.1f}",
            "SignalStrengthDbm": str(int(rng.integers(-80, -30))),
            "StorageMinTempC": str(min_t),
            "StorageMaxTempC": str(max_t),
            "MaxHumidityPct": "65" if is_cold else "70",
            "TemperatureBreachFlag": temp_breach,
            "HumidityBreachFlag": hum_breach,
            "ThresholdBreachFlag": threshold_breach,
            "AlertSeverity": alert,
            "ReadingQuality": rng.choice(["Valid", "Valid", "Valid", "Estimated"]),
            "DataLatencySeconds": str(latency_sec),
            "LateArrivalFlag": "Y" if latency_sec > 60 else "N",
            "SourceStream": "mqtt/pharma/warehouse/environment" if is_wh
                            else "mqtt/pharma/shipment/environment",
        })
    return pd.DataFrame(rows)


# ── main entry point ──────────────────────────────────────────────────

GENERATORS = [
    ("01_master_data", _gen_master),
    ("02_manufacturing_data", _gen_manufacturing),
    ("03_manufacturer_delivery_data", _gen_delivery),
    ("04_warehouse_data", _gen_warehouse),
    ("05_pharmacy_sales_data", _gen_sales),
    ("06_hospital_demand_data", _gen_demand),
    ("07_iot_streaming_data", _gen_iot),
]


def generate_all(cycle: int = 1) -> dict:
    """
    Generate synthetic records for all 7 source files and write to landing/.

    Parameters
    ----------
    cycle : int (1 or 2)
        Refresh cycle.  Cycle 1 = Jul-Sep 2025, Cycle 2 = Oct-Dec 2025.

    Returns
    -------
    dict : {filename: rows_generated}
    """
    LANDING.mkdir(parents=True, exist_ok=True)
    rng = _rng(cycle)
    stats = {}
    ts = datetime.now().strftime("%Y%m%d%H%M%S")

    for prefix, gen_fn in GENERATORS:
        df = gen_fn(rng, cycle)
        fname = f"{prefix}_cycle{cycle}_{ts}.csv"
        out_path = LANDING / fname
        df.to_csv(out_path, index=False)
        stats[fname] = len(df)
        print(f"  ✓ {fname}: {len(df):,} rows")

    return stats

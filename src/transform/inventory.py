from src.transform.dwh import connect, u, dk, conv, _extract

# canonical warehouse reference (from the original 04_warehouse_data.csv)
WAREHOUSES = [
    ("WH001", "Northeast Distribution Center", "950600200018", 650000),
    ("WH002", "Midwest Distribution Center", "950600200025", 800000),
    ("WH003", "Southern Distribution Center", "950600200032", 900000),
    ("WH004", "Western Distribution Center", "950600200049", 720000),
    ("WH005", "Southeast Distribution Center", "950600200056", 680000),
    ("WH006", "New England Cold Chain Hub", "950600200063", 280000),
]

INV_MEASURES = {
    "opening_qty": ("openingquantitypacks", "BIGINT"),
    "received_qty": ("receivedquantitypacks", "BIGINT"),
    "available_qty": ("availablequantitypacks", "BIGINT"),
    "reserved_qty": ("reservedquantitypacks", "BIGINT"),
    "picked_qty": ("pickedquantitypacks", "BIGINT"),
    "shipped_qty": ("shippedquantitypacks", "BIGINT"),
    "damaged_qty": ("damagedquantitypacks", "BIGINT"),
    "expired_qty": ("expiredquantitypacks", "BIGINT"),
    "closing_qty": ("closingquantitypacks", "BIGINT"),
    "days_to_expiry": ("daystoexpiry", "INTEGER"),
    "bin_capacity_packs": ("allocatedbincapacitypacks", "BIGINT"),
}
SHIP_MEASURES = {
    "shipped_qty": ("shippedquantitypacks", "BIGINT"),
    "received_qty": ("receivedquantitypacks", "BIGINT"),
    "damaged_qty": ("damagedquantitypacks", "BIGINT"),
    "short_qty": ("shortquantitypacks", "BIGINT"),
    "distance_miles": ("distancemiles", "DOUBLE"),
    "transit_hours": ("transithours", "DOUBLE"),
    "freight_cost_usd": ("freightcostusd", "DOUBLE"),
}

INV_COLS = ["inventorysnapshotid", "snapshotdate", "warehouselotid", "inboundshipmentid",
            "productionbatchid", "medicineid", "warehouseid", "storagezoneid", "storagecondition",
            "batchexpirydate", "inventorystatus", "stockoutflag", "nearexpiryflag",
            "fefocomplianceflag", "cyclecountperformedflag", "warehouseactivitystatus"] + \
           [v[0] for v in INV_MEASURES.values()]
SHIP_COLS = ["shipmentid", "shipmentlinenumber", "sscc18", "manufacturingorderid",
             "productionbatchid", "medicineid", "destinationwarehouseid", "destinationcity",
             "destinationstate", "carrierid", "carriername", "transportmode",
             "coldchainrequiredflag", "dispatchdate", "promiseddeliverydate", "actualdeliverydate",
             "shipmentstatus", "ontimedeliveryflag", "infulldeliveryflag", "otifflag",
             "temperatureexcursionflag"] + [v[0] for v in SHIP_MEASURES.values()]


def _cols(m):
    return ",\n    ".join(f"{k} {v[1]}" for k, v in m.items())


DDL = f"""
CREATE SEQUENCE IF NOT EXISTS seq_wh START 1;
CREATE SEQUENCE IF NOT EXISTS seq_carrier START 1;
CREATE SEQUENCE IF NOT EXISTS seq_inv_profile START 1;
CREATE SEQUENCE IF NOT EXISTS seq_ship_profile START 1;

CREATE TABLE IF NOT EXISTS dim_warehouse (
    wh_key INTEGER DEFAULT nextval('seq_wh'), wh_id VARCHAR, wh_gln VARCHAR, wh_name VARCHAR,
    city VARCHAR, state VARCHAR, total_capacity_packs BIGINT
);
CREATE TABLE IF NOT EXISTS dim_carrier (
    carrier_key INTEGER DEFAULT nextval('seq_carrier'), carrier_id VARCHAR, carrier_name VARCHAR
);
CREATE TABLE IF NOT EXISTS dim_inventory_profile (
    inv_profile_key INTEGER DEFAULT nextval('seq_inv_profile'),
    inventory_status VARCHAR, storage_condition VARCHAR, stockout_flag VARCHAR,
    near_expiry_flag VARCHAR, fefo_flag VARCHAR, cycle_count_flag VARCHAR, activity_status VARCHAR
);
CREATE TABLE IF NOT EXISTS dim_shipment_profile (
    ship_profile_key INTEGER DEFAULT nextval('seq_ship_profile'),
    transport_mode VARCHAR, shipment_status VARCHAR, cold_chain_required VARCHAR
);

CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
    inventory_snapshot_id VARCHAR,
    snapshot_date_key INTEGER, expiry_date_key INTEGER,
    wh_key INTEGER, med_key INTEGER, inv_profile_key INTEGER,
    warehouse_lot_id VARCHAR, inbound_shipment_id VARCHAR, production_batch_id VARCHAR,
    storage_zone_id VARCHAR,
    {_cols(INV_MEASURES)},
    batch_id VARCHAR, loaded_at TIMESTAMP DEFAULT now()
);
CREATE TABLE IF NOT EXISTS fact_shipment (
    shipment_id VARCHAR, shipment_line_no INTEGER,
    dispatch_date_key INTEGER, promised_date_key INTEGER, actual_date_key INTEGER,
    wh_key INTEGER, carrier_key INTEGER, med_key INTEGER, ship_profile_key INTEGER,
    sscc18 VARCHAR, manufacturing_order_id VARCHAR, production_batch_id VARCHAR,
    {_cols(SHIP_MEASURES)},
    on_time INTEGER, in_full INTEGER, otif INTEGER, temp_excursion INTEGER,
    batch_id VARCHAR, loaded_at TIMESTAMP DEFAULT now()
);

INSERT INTO dim_warehouse (wh_key, wh_id, wh_name)
SELECT -1, 'UNKNOWN', 'Unknown' WHERE NOT EXISTS (SELECT 1 FROM dim_warehouse WHERE wh_key = -1);
INSERT INTO dim_carrier (carrier_key, carrier_id, carrier_name)
SELECT -1, 'UNKNOWN', 'Unknown' WHERE NOT EXISTS (SELECT 1 FROM dim_carrier WHERE carrier_key = -1);
INSERT INTO dim_inventory_profile (inv_profile_key, inventory_status)
SELECT -1, 'Unknown' WHERE NOT EXISTS (SELECT 1 FROM dim_inventory_profile WHERE inv_profile_key = -1);
INSERT INTO dim_shipment_profile (ship_profile_key, transport_mode)
SELECT -1, 'Unknown' WHERE NOT EXISTS (SELECT 1 FROM dim_shipment_profile WHERE ship_profile_key = -1);
"""


def init_inventory_schema():
    with connect() as con:
        for stmt in DDL.split(";"):
            if stmt.strip():
                con.execute(stmt)
        for wid, name, gln, cap in WAREHOUSES:
            con.execute("""
                INSERT INTO dim_warehouse (wh_id, wh_gln, wh_name, total_capacity_packs)
                SELECT ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM dim_warehouse WHERE wh_id = ?)
            """, [wid, gln, name, cap, wid])
        # more role-playing views over the same physical dim_date
        for role in ["snapshot", "dispatch", "promised"]:
            con.execute(f"""
                CREATE OR REPLACE VIEW dim_{role}_date AS
                SELECT date_key AS {role}_date_key, full_date AS {role}_date,
                       cal_year AS {role}_year, cal_quarter AS {role}_quarter,
                       cal_month AS {role}_month, month_name AS {role}_month_name,
                       year_month AS {role}_year_month
                FROM dim_date
            """)
    return "inventory schema ready"


def extract_inventory():
    with connect() as con:
        return {
            "inv": _extract(con, "inv", "stg.warehouse_data", INV_COLS),
            "ship": _extract(con, "ship", "stg.manufacturer_delivery_data", SHIP_COLS),
        }


def _views(con):
    im = ",\n".join(f"{conv(s, t)} AS {k}" for k, (s, t) in INV_MEASURES.items())
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW v_inv AS
    SELECT inventorysnapshotid AS inventory_snapshot_id,
           TRY_CAST(snapshotdate AS DATE) AS snapshot_date,
           TRY_CAST(batchexpirydate AS DATE) AS expiry_date,
           {u('warehouseid', 'UNKNOWN')} AS wh_id, medicineid AS med_id,
           warehouselotid AS warehouse_lot_id, inboundshipmentid AS inbound_shipment_id,
           productionbatchid AS production_batch_id, storagezoneid AS storage_zone_id,
           {u('inventorystatus')} AS inventory_status, {u('storagecondition')} AS storage_condition,
           {u('stockoutflag')} AS stockout_flag, {u('nearexpiryflag')} AS near_expiry_flag,
           {u('fefocomplianceflag')} AS fefo_flag, {u('cyclecountperformedflag')} AS cycle_count_flag,
           {u('warehouseactivitystatus')} AS activity_status,
           {im}, _batch_id AS batch_id
    FROM raw_inv
    QUALIFY row_number() OVER (PARTITION BY inventorysnapshotid ORDER BY _batch_id) = 1
    """)
    sm = ",\n".join(f"{conv(s, t)} AS {k}" for k, (s, t) in SHIP_MEASURES.items())
    con.execute(f"""
    CREATE OR REPLACE TEMP VIEW v_ship AS
    SELECT shipmentid AS shipment_id,
           TRY_CAST(TRY_CAST(shipmentlinenumber AS DOUBLE) AS INTEGER) AS shipment_line_no,
           sscc18, manufacturingorderid AS manufacturing_order_id,
           productionbatchid AS production_batch_id, medicineid AS med_id,
           {u('destinationwarehouseid', 'UNKNOWN')} AS wh_id,
           {u('carrierid', 'UNKNOWN')} AS carrier_id, {u('carriername')} AS carrier_name,
           {u('transportmode')} AS transport_mode, {u('shipmentstatus')} AS shipment_status,
           {u('coldchainrequiredflag')} AS cold_chain_required,
           TRY_CAST(dispatchdate AS DATE) AS dispatch_date,
           TRY_CAST(promiseddeliverydate AS DATE) AS promised_date,
           TRY_CAST(actualdeliverydate AS DATE) AS actual_date,
           CASE WHEN ontimedeliveryflag = 'Y' THEN 1 ELSE 0 END AS on_time,
           CASE WHEN infulldeliveryflag = 'Y' THEN 1 ELSE 0 END AS in_full,
           CASE WHEN otifflag = 'Y' THEN 1 ELSE 0 END AS otif,
           CASE WHEN temperatureexcursionflag = 'Y' THEN 1 ELSE 0 END AS temp_excursion,
           {sm}, _batch_id AS batch_id
    FROM raw_ship
    QUALIFY row_number() OVER (PARTITION BY shipmentid, shipmentlinenumber ORDER BY _batch_id) = 1
    """)


def load_inventory_dims():
    with connect() as con:
        _views(con)
        con.execute("""
        UPDATE dim_warehouse d SET city = s.city, state = s.state
        FROM (SELECT destinationwarehouseid AS wh_id, mode(destinationcity) AS city,
                     mode(destinationstate) AS state
              FROM raw_ship GROUP BY 1) s
        WHERE d.wh_id = s.wh_id AND d.wh_key <> -1
        """)
        con.execute("""
        INSERT INTO dim_carrier (carrier_id, carrier_name)
        SELECT carrier_id, any_value(carrier_name) FROM v_ship v
        WHERE NOT EXISTS (SELECT 1 FROM dim_carrier d WHERE d.carrier_id = v.carrier_id)
        GROUP BY carrier_id
        """)
        con.execute("""
        INSERT INTO dim_inventory_profile (inventory_status, storage_condition, stockout_flag,
            near_expiry_flag, fefo_flag, cycle_count_flag, activity_status)
        SELECT DISTINCT inventory_status, storage_condition, stockout_flag, near_expiry_flag,
               fefo_flag, cycle_count_flag, activity_status
        FROM v_inv v WHERE NOT EXISTS (SELECT 1 FROM dim_inventory_profile d
            WHERE d.inventory_status = v.inventory_status AND d.storage_condition = v.storage_condition
              AND d.stockout_flag = v.stockout_flag AND d.near_expiry_flag = v.near_expiry_flag
              AND d.fefo_flag = v.fefo_flag AND d.cycle_count_flag = v.cycle_count_flag
              AND d.activity_status = v.activity_status)
        """)
        con.execute("""
        INSERT INTO dim_shipment_profile (transport_mode, shipment_status, cold_chain_required)
        SELECT DISTINCT transport_mode, shipment_status, cold_chain_required
        FROM v_ship v WHERE NOT EXISTS (SELECT 1 FROM dim_shipment_profile d
            WHERE d.transport_mode = v.transport_mode AND d.shipment_status = v.shipment_status
              AND d.cold_chain_required = v.cold_chain_required)
        """)
    return "inventory dims loaded"


def load_fact_inventory():
    with connect() as con:
        _views(con)
        mc = ", ".join(INV_MEASURES)
        mv = ", ".join(f"v.{c}" for c in INV_MEASURES)
        con.execute(f"""
        INSERT INTO fact_inventory_snapshot
            (inventory_snapshot_id, snapshot_date_key, expiry_date_key, wh_key, med_key,
             inv_profile_key, warehouse_lot_id, inbound_shipment_id, production_batch_id,
             storage_zone_id, {mc}, batch_id)
        SELECT v.inventory_snapshot_id, {dk('v.snapshot_date')}, {dk('v.expiry_date')},
               COALESCE(w.wh_key, -1), COALESCE(m.med_key, mc.med_key, -1),
               COALESCE(p.inv_profile_key, -1), v.warehouse_lot_id, v.inbound_shipment_id,
               v.production_batch_id, v.storage_zone_id, {mv}, v.batch_id
        FROM v_inv v
        ASOF LEFT JOIN dim_medicine m ON m.med_id = v.med_id AND v.snapshot_date >= m.eff_from
        LEFT JOIN dim_medicine mc ON mc.med_id = v.med_id AND mc.is_current
        LEFT JOIN dim_warehouse w ON w.wh_id = v.wh_id
        LEFT JOIN dim_inventory_profile p ON p.inventory_status = v.inventory_status
             AND p.storage_condition = v.storage_condition AND p.stockout_flag = v.stockout_flag
             AND p.near_expiry_flag = v.near_expiry_flag AND p.fefo_flag = v.fefo_flag
             AND p.cycle_count_flag = v.cycle_count_flag AND p.activity_status = v.activity_status
        WHERE NOT EXISTS (SELECT 1 FROM fact_inventory_snapshot f
                          WHERE f.inventory_snapshot_id = v.inventory_snapshot_id)
        ORDER BY v.snapshot_date
        """)
        return con.execute("SELECT count(*) FROM fact_inventory_snapshot").fetchone()[0]


def load_fact_shipment():
    with connect() as con:
        _views(con)
        mc = ", ".join(SHIP_MEASURES)
        mv = ", ".join(f"v.{c}" for c in SHIP_MEASURES)
        con.execute(f"""
        INSERT INTO fact_shipment
            (shipment_id, shipment_line_no, dispatch_date_key, promised_date_key, actual_date_key,
             wh_key, carrier_key, med_key, ship_profile_key, sscc18, manufacturing_order_id,
             production_batch_id, {mc}, on_time, in_full, otif, temp_excursion, batch_id)
        SELECT v.shipment_id, v.shipment_line_no, {dk('v.dispatch_date')}, {dk('v.promised_date')},
               {dk('v.actual_date')}, COALESCE(w.wh_key, -1), COALESCE(c.carrier_key, -1),
               COALESCE(m.med_key, mc.med_key, -1), COALESCE(sp.ship_profile_key, -1),
               v.sscc18, v.manufacturing_order_id, v.production_batch_id, {mv},
               v.on_time, v.in_full, v.otif, v.temp_excursion, v.batch_id
        FROM v_ship v
        ASOF LEFT JOIN dim_medicine m ON m.med_id = v.med_id AND v.dispatch_date >= m.eff_from
        LEFT JOIN dim_medicine mc ON mc.med_id = v.med_id AND mc.is_current
        LEFT JOIN dim_warehouse w ON w.wh_id = v.wh_id
        LEFT JOIN dim_carrier c ON c.carrier_id = v.carrier_id
        LEFT JOIN dim_shipment_profile sp ON sp.transport_mode = v.transport_mode
             AND sp.shipment_status = v.shipment_status AND sp.cold_chain_required = v.cold_chain_required
        WHERE NOT EXISTS (SELECT 1 FROM fact_shipment f
                          WHERE f.shipment_id = v.shipment_id AND f.shipment_line_no = v.shipment_line_no)
        """)
        return con.execute("SELECT count(*) FROM fact_shipment").fetchone()[0]


def commit_inventory_batches():
    with connect() as con:
        for src in ["inv", "ship"]:
            con.execute(f"""INSERT INTO etl_loaded_batches (source, batch_id)
                            SELECT DISTINCT '{src}', _batch_id FROM raw_{src}""")
    return "batches committed"


def export_parquet():
    """Hive-partitioned parquet copy of the snapshot fact (partition evidence for the report)."""
    from src.transform.dwh import DATA_DIR
    out = DATA_DIR / "parquet" / "fact_inventory_snapshot"
    out.mkdir(parents=True, exist_ok=True)
    with connect() as con:
        con.execute(f"""
        COPY (SELECT f.*, d.year_month FROM fact_inventory_snapshot f
              JOIN dim_date d ON d.date_key = f.snapshot_date_key ORDER BY d.year_month)
        TO '{out}' (FORMAT PARQUET, PARTITION_BY (year_month), OVERWRITE_OR_IGNORE)
        """)
    return str(out)


def validate_inventory():
    with connect() as con:
        q = lambda s: con.execute(s).fetchone()[0]
        out = {
            "dim_warehouse": q("SELECT count(*) FROM dim_warehouse"),
            "dim_carrier": q("SELECT count(*) FROM dim_carrier"),
            "dim_inventory_profile": q("SELECT count(*) FROM dim_inventory_profile"),
            "dim_shipment_profile": q("SELECT count(*) FROM dim_shipment_profile"),
            "fact_inventory_snapshot": q("SELECT count(*) FROM fact_inventory_snapshot"),
            "fact_shipment": q("SELECT count(*) FROM fact_shipment"),
            "inv_dupes": q("SELECT count(*) - count(DISTINCT inventory_snapshot_id) FROM fact_inventory_snapshot"),
            "ship_dupes": q("SELECT count(*) - count(DISTINCT shipment_id || '-' || shipment_line_no) FROM fact_shipment"),
            "inv_unknown_wh": q("SELECT count(*) FROM fact_inventory_snapshot WHERE wh_key = -1"),
            "inv_unknown_med": q("SELECT count(*) FROM fact_inventory_snapshot WHERE med_key = -1"),
            "ship_unknown_wh": q("SELECT count(*) FROM fact_shipment WHERE wh_key = -1"),
            "ship_unknown_carrier": q("SELECT count(*) FROM fact_shipment WHERE carrier_key = -1"),
        }
        print(out)
        if out["inv_dupes"] or out["ship_dupes"]:
            raise ValueError(f"duplicate keys in facts: {out}")
        return out
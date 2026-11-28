BEGIN;
DELETE FROM stg.warehouse_data a USING stg.warehouse_data b
 WHERE a.inventorysnapshotid = b.inventorysnapshotid AND a.ctid > b.ctid;
DELETE FROM stg.manufacturer_delivery_data a USING stg.manufacturer_delivery_data b
 WHERE a.shipmentid = b.shipmentid AND a.shipmentlinenumber = b.shipmentlinenumber AND a.ctid > b.ctid;
DELETE FROM stg.pharmacy_sales_data a USING stg.pharmacy_sales_data b
 WHERE a.salestransactionid = b.salestransactionid AND a.ctid > b.ctid;
DELETE FROM stg.hospital_demand_data a USING stg.hospital_demand_data b
 WHERE a.hospitaldemandrecordid = b.hospitaldemandrecordid AND a.ctid > b.ctid;
COMMIT;
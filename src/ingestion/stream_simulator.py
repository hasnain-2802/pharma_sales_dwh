"""
Paced IoT Cold Chain Telemetry Stream Simulator
================================================
Sends realistic sensor telemetry batches in real-time to the Ingestion API
(http://localhost:8001/api/stream/iot or http://ingestion-api:8000/api/stream/iot).

Features:
- Paced emission: batches every 1-2 seconds across multiple minutes
- Realistic latency: 1-8 seconds (occasional late arrival >60s)
- Realistic thermal profile: 2-8 C (cold chain), 15-25 C (ambient)
- Injected ~1% malformed / outlier readings to test quarantine and DLQ logic
- Generates continuous, non-burst time-series curves for Grafana
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta
import urllib.request
import urllib.error

API_URL = os.environ.get("INGESTION_API_URL", "http://ingestion-api:8000/api/stream/iot")

DEVICES_WH = [
    ("IOT-WH001-COLD-01", "WH001", "WH001-COLD-01", True),
    ("IOT-WH001-AMB-01", "WH001", "WH001-AMB-01", False),
    ("IOT-WH002-COLD-01", "WH002", "WH002-COLD-01", True),
    ("IOT-WH002-AMB-01", "WH002", "WH002-AMB-01", False),
    ("IOT-WH004-COLD-01", "WH004", "WH004-COLD-01", True),
    ("IOT-WH006-COLD-01", "WH006", "WH006-COLD-01", True),
]

DEVICES_SHIP = [
    ("IOT-SHIP-001", "MFG-SHP-0001001", True),
    ("IOT-SHIP-002", "MFG-SHP-0001002", False),
    ("IOT-SHIP-003", "MFG-SHP-0001003", True),
]


def generate_batch(batch_seq: int, batch_size: int = 100):
    readings = []
    now = datetime.utcnow()

    for i in range(batch_size):
        seq = batch_seq * batch_size + i + 1
        is_wh = random.random() > 0.3
        
        # 1% malformed row for quarantine / DLQ detection
        is_malformed = random.random() < 0.01

        if is_wh:
            dev_id, wh_id, zone_id, is_cold = random.choice(DEVICES_WH)
            ship_id, sscc = "", ""
            asset_type = "WarehouseZone"
            min_t, max_t = (2.0, 8.0) if is_cold else (15.0, 25.0)
            stream_name = "mqtt/pharma/warehouse/environment"
        else:
            dev_id, ship_id, is_cold = random.choice(DEVICES_SHIP)
            wh_id, zone_id = "", ""
            sscc = f"9506001000{random.randint(10000000, 99999999)}"
            asset_type = "ShipmentContainer"
            min_t, max_t = (2.0, 8.0) if is_cold else (15.0, 25.0)
            stream_name = "mqtt/pharma/shipment/environment"

        # Realistic latency: mostly 1-6s, 3% late arrival (>60s)
        is_late = random.random() < 0.03
        latency_sec = random.randint(65, 120) if is_late else random.randint(1, 6)
        event_time = now - timedelta(seconds=latency_sec)

        # Temperature simulation: mostly in range, occasional excursion
        has_excursion = random.random() < 0.05
        if has_excursion:
            temp = round(max_t + random.uniform(0.5, 4.0), 2) if random.random() < 0.8 else round(min_t - random.uniform(0.5, 2.0), 2)
            alert = "Critical"
            temp_breach = "Y"
        else:
            target_mid = (min_t + max_t) / 2.0
            temp = round(random.gauss(target_mid, 1.0), 2)
            temp = max(min_t + 0.1, min(max_t - 0.1, temp))
            alert = "Normal"
            temp_breach = "N"

        humidity = round(random.uniform(45.0, 72.0), 1)
        hum_breach = "Y" if humidity > 70.0 else "N"
        if hum_breach == "Y" and alert == "Normal":
            alert = "Warning"

        reading = {
            "SensorReadingID": "" if is_malformed else f"IOTR-STRM-{seq:010d}",
            "DeviceID": dev_id,
            "DeviceType": "Temperature-humidity logger",
            "AssetType": asset_type,
            "ShipmentID": ship_id,
            "SSCC18": sscc,
            "WarehouseID": wh_id,
            "StorageZoneID": zone_id,
            "EventTimestampUTC": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "IngestionTimestampUTC": now.strftime("%Y-%m-%d %H:%M:%S"),
            "EventSequenceNumber": str(seq),
            "TemperatureC": temp,
            "RelativeHumidityPct": humidity,
            "Latitude": round(random.uniform(26.0, 47.0), 4),
            "Longitude": round(random.uniform(-119.0, -72.0), 4),
            "DoorOpenFlag": "Y" if random.random() < 0.04 else "N",
            "ShockG": round(random.uniform(0.01, 0.25), 3),
            "BatteryLevelPct": round(random.uniform(75.0, 99.0), 1),
            "SignalStrengthDbm": random.randint(-75, -45),
            "StorageMinTempC": min_t,
            "StorageMaxTempC": max_t,
            "MaxHumidityPct": 65.0 if is_cold else 70.0,
            "TemperatureBreachFlag": temp_breach,
            "HumidityBreachFlag": hum_breach,
            "ThresholdBreachFlag": "Y" if (temp_breach == "Y" or hum_breach == "Y") else "N",
            "AlertSeverity": alert,
            "ReadingQuality": "Valid" if not is_malformed else "Corrupt",
            "DataLatencySeconds": latency_sec,
            "LateArrivalFlag": "Y" if is_late else "N",
            "SourceStream": stream_name,
        }
        readings.append(reading)

    return readings


def send_batch(readings, url: str):
    data = json.dumps(readings).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_simulation(num_batches: int = 60, batch_size: int = 100, interval_sec: float = 1.5, url: str = API_URL):
    print(f"Starting paced IoT stream: {num_batches} batches x {batch_size} events (interval: {interval_sec}s)")
    print(f"Target: {url}")
    total_loaded = 0
    total_rejected = 0

    for b in range(num_batches):
        batch = generate_batch(b, batch_size)
        try:
            res = send_batch(batch, url)
            loaded = res.get("records_loaded", 0)
            rejected = res.get("records_rejected", 0)
            dur = res.get("duration_sec", 0)
            total_loaded += loaded
            total_rejected += rejected
            print(f"[{b+1}/{num_batches}] Staged {loaded} events ({rejected} rejected) in {dur:.2f}s")
        except Exception as e:
            print(f"[{b+1}/{num_batches}] Failed to send batch: {e}", file=sys.stderr)
        time.sleep(interval_sec)

    print(f"\nSimulation complete: {total_loaded:,} events loaded, {total_rejected:,} rejected.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Paced IoT stream simulator")
    parser.add_argument("--batches", type=int, default=40, help="Number of batches")
    parser.add_argument("--size", type=int, default=100, help="Events per batch")
    parser.add_argument("--interval", type=float, default=1.5, help="Seconds between batches")
    parser.add_argument("--url", type=str, default=API_URL, help="Ingestion API endpoint")
    args = parser.parse_args()

    run_simulation(args.batches, args.size, args.interval, args.url)

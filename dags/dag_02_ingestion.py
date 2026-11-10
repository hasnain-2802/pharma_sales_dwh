import time
from datetime import datetime

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException


@dag(
    dag_id="dag_02_ingestion",
    start_date=datetime(2026, 11, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "phase1"],
)
def ingestion():

    @task
    def scan_landing():
        from src.ingestion.loader import LANDING
        LANDING.mkdir(parents=True, exist_ok=True)
        # skip files that are still being written
        return sorted(
            p.name for p in LANDING.glob("*.csv")
            if time.time() - p.stat().st_mtime > 5
        )

    @task(max_active_tis_per_dagrun=2)
    def ingest(filename):
        from src.ingestion.loader import LANDING, ingest_file
        result = ingest_file(LANDING / filename)
        print(result)
        if result["status"] == "FAILED":
            raise AirflowFailException(result["error"])
        return result

    ingest.expand(filename=scan_landing())


ingestion()

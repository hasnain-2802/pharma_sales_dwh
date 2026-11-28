from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="dag_05_inventory_transform",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["transform", "phase2", "duckdb"],
)
def inventory_transform():

    @task
    def init_schema():
        from src.transform import inventory
        return inventory.init_inventory_schema()

    @task
    def extract():
        from src.transform import inventory
        return inventory.extract_inventory()

    @task
    def dims():
        from src.transform import inventory
        return inventory.load_inventory_dims()

    @task
    def fact_inventory():
        from src.transform import inventory
        return inventory.load_fact_inventory()

    @task
    def fact_shipment():
        from src.transform import inventory
        return inventory.load_fact_shipment()

    @task
    def commit_batches():
        from src.transform import inventory
        return inventory.commit_inventory_batches()

    @task
    def parquet_partitions():
        from src.transform import inventory
        return inventory.export_parquet()

    @task
    def validate():
        from src.transform import inventory
        return inventory.validate_inventory()

    (init_schema() >> extract() >> dims() >> fact_inventory() >> fact_shipment()
     >> commit_batches() >> parquet_partitions() >> validate())


inventory_transform()
import importlib
from datetime import datetime

from airflow.decorators import dag, task


def step(task_id, module, func):
    @task(task_id=task_id)
    def _run():
        return getattr(importlib.import_module(module), func)()
    return _run()


@dag(dag_id="dag_06_phase2_refresh", start_date=datetime(2026, 11, 1), schedule=None,
     catchup=False, max_active_runs=1, tags=["phase2", "refresh", "master"])
def phase2_refresh():
    steps = [
        step("generate_files", "src.generator.inv_gen", "generate_refresh"),
        step("ingest_to_staging", "src.generator.inv_gen", "ingest_pending"),
        step("init_schema", "src.transform.inventory", "init_inventory_schema"),
        step("extract_staging", "src.transform.inventory", "extract_inventory"),
        step("load_dimensions", "src.transform.inventory", "load_inventory_dims"),
        step("load_fact_inventory", "src.transform.inventory", "load_fact_inventory"),
        step("load_fact_shipment", "src.transform.inventory", "load_fact_shipment"),
        step("commit_batches", "src.transform.inventory", "commit_inventory_batches"),
        step("parquet_partitions", "src.transform.inventory", "export_parquet"),
        step("validate", "src.transform.inventory", "validate_inventory"),
        step("build_aggregates", "src.transform.inv_mart", "build_inv_aggregates"),
        step("publish_mart", "src.transform.inv_mart", "publish_mart"),
        step("run_queries", "src.transform.inv_mart", "run_inv_queries"),
    ]
    for a, b in zip(steps, steps[1:]):
        a >> b


phase2_refresh()

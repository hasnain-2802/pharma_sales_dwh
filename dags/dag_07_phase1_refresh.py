import importlib
from datetime import datetime

from airflow.decorators import dag, task


def step(task_id, module, func):
    @task(task_id=task_id)
    def _run():
        return getattr(importlib.import_module(module), func)()
    return _run()


@dag(dag_id="dag_07_phase1_refresh", start_date=datetime(2026, 11, 1), schedule=None,
     catchup=False, max_active_runs=1, tags=["phase1", "refresh", "master"])
def phase1_refresh():
    steps = [
        step("generate_files", "src.generator.p1_gen", "generate_refresh"),
        step("ingest_to_staging", "src.generator.p1_gen", "ingest_pending"),
        step("init_schema", "src.transform.dwh", "init_schema"),
        step("extract_staging", "src.transform.dwh", "extract_staging"),
        step("dim_medicine_scd2", "src.transform.dwh", "load_dim_medicine"),
        step("dims_other", "src.transform.dwh", "load_dims"),
        step("fact_sale", "src.transform.dwh", "load_fact_sale"),
        step("fact_demand", "src.transform.dwh", "load_fact_demand"),
        step("commit_batches", "src.transform.dwh", "commit_batches"),
        step("validate", "src.transform.dwh", "validate"),
        step("build_aggregates", "src.query.runner", "build_aggregates"),
        step("publish_mart", "src.transform.p1_mart", "publish_p1"),
        step("run_queries", "src.query.runner", "run_queries"),
    ]
    for a, b in zip(steps, steps[1:]):
        a >> b


phase1_refresh()

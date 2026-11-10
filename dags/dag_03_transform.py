from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="dag_03_transform",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["transform", "phase1", "duckdb"],
)
def transform():

    @task
    def init_schema():
        from src.transform import dwh
        return dwh.init_schema()

    @task
    def extract_staging():
        from src.transform import dwh
        return dwh.extract_staging()

    @task
    def dim_medicine_scd2():
        from src.transform import dwh
        return dwh.load_dim_medicine()

    @task
    def dims_other():
        from src.transform import dwh
        return dwh.load_dims()

    @task
    def fact_sale():
        from src.transform import dwh
        return dwh.load_fact_sale()

    @task
    def fact_demand():
        from src.transform import dwh
        return dwh.load_fact_demand()

    @task
    def commit_batches():
        from src.transform import dwh
        return dwh.commit_batches()

    @task
    def validate():
        from src.transform import dwh
        return dwh.validate()

    (init_schema() >> extract_staging() >> dim_medicine_scd2() >> dims_other()
     >> fact_sale() >> fact_demand() >> commit_batches() >> validate())


transform()

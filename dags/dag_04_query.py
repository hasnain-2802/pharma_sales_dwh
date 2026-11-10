from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="dag_04_query",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["query", "phase1", "duckdb"],
)
def query_layer():

    @task
    def build_aggregates():
        from src.query import runner
        return runner.build_aggregates()

    @task
    def run_queries():
        from src.query import runner
        return runner.run_queries(repeats=3)

    @task
    def summary():
        from src.query import runner
        return runner.summary()

    build_aggregates() >> run_queries() >> summary()


query_layer()

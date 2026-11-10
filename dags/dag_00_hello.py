from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def check_env():
    import duckdb, pandas, os
    from sqlalchemy import create_engine, text
    eng = create_engine(os.environ["STAGING_DB_URL"])
    with eng.connect() as c:
        print("Postgres OK:", c.execute(text("select 1")).scalar())
    con = duckdb.connect(os.environ["DUCKDB_PATH"])
    print("DuckDB OK:", con.execute("select 42").fetchone())
    print("pandas", pandas.__version__)

with DAG("dag_00_hello", start_date=datetime(2026, 11, 1),
         schedule=None, catchup=False) as dag:
    PythonOperator(task_id="check_env", python_callable=check_env)

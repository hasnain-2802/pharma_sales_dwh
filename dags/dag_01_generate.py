from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param


@dag(
    dag_id="dag_01_generate",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    params={"cycle": Param(1, type="integer", minimum=1, maximum=2,
                           description="Refresh cycle: 1 = Jul-Sep 2025, 2 = Oct-Dec 2025")},
    tags=["generator", "oversampling"],
)
def generate():
    """Generate synthetic data for all 7 source files and drop into landing/."""

    @task
    def generate_data(**context):
        from src.generator.generate import generate_all
        cycle = context["params"]["cycle"]
        stats = generate_all(cycle=cycle)
        return stats

    @task
    def log_stats(stats):
        total = sum(stats.values())
        print("=" * 60)
        print(f"  DATA GENERATION SUMMARY")
        print("=" * 60)
        for fname, rows in sorted(stats.items()):
            print(f"  {fname}: {rows:,} rows")
        print("-" * 60)
        print(f"  TOTAL: {total:,} rows")
        print("=" * 60)
        return total

    stats = generate_data()
    log_stats(stats)


generate()


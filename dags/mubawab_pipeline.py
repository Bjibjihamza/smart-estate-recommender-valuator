# dags/mubawab_pipeline.py
from datetime import datetime, timedelta
from typing import Optional

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator

# ---------- Params ----------
default_args = {
    "owner": "mubawab",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Reuse your dev scraper workspace container
SCRAPER_C = "avito-scraper"

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "realestate.mubawab.raw"

def _make_cmd(mode: str, pages: int = 20, limit: Optional[int] = 1000) -> str:
    limit_part = f"--limit {limit} " if limit is not None else ""
    return (
        f"docker exec -i {SCRAPER_C} bash -lc '"
        f"export PYTHONPATH=/app/src && "
        f"python /app/src/pipeline/producer/mubawab_producer.py "
        f"--mode {mode} --pages {pages} "
        f"{limit_part}"
        f"--bootstrap {KAFKA_BOOTSTRAP} --topic {KAFKA_TOPIC}'"
    )

# ---------- Alternating mode ----------
def choose_and_toggle_mode() -> str:
    """
    Reads Airflow Variable 'mubawab_scraper_last_mode' (default: 'acheter')
    so the 1st run goes 'louer', then alternates each run.
    """
    last_mode = Variable.get("mubawab_scraper_last_mode", default_var="acheter")
    next_mode = "louer" if last_mode == "acheter" else "acheter"
    Variable.set("mubawab_scraper_last_mode", next_mode)
    return "scrape_louer" if next_mode == "louer" else "scrape_acheter"

with DAG(
    dag_id="mubawab_scraper",
    default_args=default_args,
    schedule="*/5 * * * *",  # every 5 minutes (same as Avito)
    start_date=datetime(2025, 11, 2),
    catchup=False,
    max_active_runs=1,
    tags=["mubawab", "scraper"],
) as dag:

    choose_mode = BranchPythonOperator(
        task_id="choose_mode",
        python_callable=choose_and_toggle_mode,
    )

    scrape_louer = BashOperator(
        task_id="scrape_louer",
        bash_command=_make_cmd("louer"),
    )

    scrape_acheter = BashOperator(
        task_id="scrape_acheter",
        bash_command=_make_cmd("acheter"),
    )

    # Mark success after either branch
    scrape_done = EmptyOperator(
        task_id="scrape_done",
        trigger_rule="none_failed_min_one_success",
    )

    # PIPELINE (RAW only)
    choose_mode >> [scrape_louer, scrape_acheter] >> scrape_done

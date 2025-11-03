# dags/avito_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable

# ---------- Params ----------
default_args = {
    "owner": "avito",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SCRAPER_C = "avito-scraper"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "realestate.avito.raw"

def _make_cmd(mode: str, pages: int = 1, limit: int | None = 10) -> str:
    limit_part = f"--limit {limit} " if limit is not None else ""
    return (
        f"docker exec -i {SCRAPER_C} bash -lc '"
        f"export PYTHONPATH=/app/src && "
        f"python /app/src/Pipeline/producer/avito_producer.py "
        f"--mode {mode} --pages {pages} "
        f"{limit_part}"
        f"--bootstrap {KAFKA_BOOTSTRAP} --topic {KAFKA_TOPIC}'"
    )

# ---------- Alternating mode ----------
def choose_and_toggle_mode() -> str:
    """
    Reads Airflow Variable 'avito_scraper_last_mode' (default: 'acheter')
    so the 1st run goes 'louer', then alternates each run.
    """
    last_mode = Variable.get("avito_scraper_last_mode", default_var="acheter")
    next_mode = "louer" if last_mode == "acheter" else "acheter"
    Variable.set("avito_scraper_last_mode", next_mode)
    return "scrape_louer" if next_mode == "louer" else "scrape_acheter"

with DAG(
    dag_id="avito_scraper",
    default_args=default_args,
    schedule="*/5 * * * *",     # every 5 minutes
    start_date=datetime(2025, 11, 2),
    catchup=False,
    max_active_runs=1,
    tags=["avito", "scraper"],
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

    # Transform the latest 30 minutes window → Silver (append)
    # We compute the window using Airflow data_interval_end
# dags/avito_pipeline.py

    transform_to_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command=(
            "docker exec -i spark-iceberg bash -lc "
            "'export PYTHONPATH=/opt/work/src && "
            "/opt/spark/bin/spark-submit --master local[*] "
            "/opt/work/src/Pipeline/transform/avito_raw_to_silver.py "
            "--catalog rest --mode append "
            "--since {{ (ts_nodash_with_tz | ts_to_datetime - macros.timedelta(minutes=35)) | ts }} "
            "--until {{ ts }}'"
        ),
    )

    choose_mode >> [scrape_louer, scrape_acheter] >> scrape_done
    scrape_done >> transform_to_silver

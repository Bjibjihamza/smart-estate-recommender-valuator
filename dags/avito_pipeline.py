# dags/avito_pipeline.py
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator


# ========= Parameters (tweak from Airflow UI Variables if you want) =========
DEFAULT_PAGES = int(Variable.get("avito_pages", default_var="10"))
DEFAULT_LIMIT = Variable.get("avito_limit", default_var="1000")  # set to "None" to disable
FALLBACK_WINDOW_MINS = int(Variable.get("avito_window_mins", default_var="35"))

SCRAPER_CONTAINER = Variable.get("scraper_container", default_var="avito-scraper")
SPARK_CONTAINER = Variable.get("spark_container", default_var="spark-iceberg")

CATALOG = Variable.get("iceberg_catalog", default_var="rest")
KAFKA_BOOTSTRAP = Variable.get("kafka_bootstrap", default_var="kafka:9092")
KAFKA_TOPIC = Variable.get("kafka_topic_avito", default_var="realestate.avito.raw")


# ========================== Helpers ==========================
def _make_scrape_cmd(mode: str, pages: int = DEFAULT_PAGES, limit: str | None = DEFAULT_LIMIT) -> str:
    """Build the docker-exec command to run the Avito producer scraper."""
    limit_part = f"--limit {limit} " if (limit and limit.lower() != "none") else ""
    return (
        f"docker exec -i {SCRAPER_CONTAINER} bash -lc '"
        f"export PYTHONPATH=/app/src && "
        f"python /app/src/pipeline/producer/avito_producer.py "
        f"--mode {mode} --pages {pages} "
        f"{limit_part}"
        f"--bootstrap {KAFKA_BOOTSTRAP} --topic {KAFKA_TOPIC}'"
    )


def choose_and_toggle_mode() -> str:
    """
    Alternate between 'louer' and 'acheter' on each run using an Airflow Variable.
    First run defaults to 'acheter' so we start by scraping 'louer'.
    """
    last_mode = Variable.get("avito_scraper_last_mode", default_var="acheter")
    next_mode = "louer" if last_mode == "acheter" else "acheter"
    Variable.set("avito_scraper_last_mode", next_mode)
    return "scrape_louer" if next_mode == "louer" else "scrape_acheter"


# ============================ DAG ============================
default_args = {
    "owner": "avito",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="avito_scraper",
    default_args=default_args,
    description="Scrape Avito → Kafka (RAW) → Spark transform → Iceberg Silver listings_all",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2025, 11, 2),
    catchup=False,
    max_active_runs=1,
    tags=["avito", "scraper", "silver"],
) as dag:

    choose_mode = BranchPythonOperator(
        task_id="choose_mode",
        python_callable=choose_and_toggle_mode,
    )

    scrape_louer = BashOperator(
        task_id="scrape_louer",
        bash_command=_make_scrape_cmd("louer"),
    )

    scrape_acheter = BashOperator(
        task_id="scrape_acheter",
        bash_command=_make_scrape_cmd("acheter"),
    )

    scrape_done = EmptyOperator(
        task_id="scrape_done",
        trigger_rule="none_failed_min_one_success",
    )

    # Transform (append recent window) into rest.silver.listings_all
    transform_to_silver = BashOperator(
        task_id="transform_to_silver",
        bash_command=(
            f"docker exec -i {SPARK_CONTAINER} bash -lc "
            "'export PYTHONPATH=/opt/work/src && "
            "/opt/spark/bin/spark-submit --master local[*] "
            "/opt/work/src/pipeline/transform/avito_raw_to_silver.py "
            "--catalog rest --mode append --fallback-window-mins 35'"
        ),
    )


    choose_mode >> [scrape_louer, scrape_acheter] >> scrape_done
    scrape_done >> transform_to_silver

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "avito",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

SCRAPER_C = "avito-scraper"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "realestate.avito.raw"

# Scrape command
RUN_SCRAPER_CMD = (
    f'docker exec -i {SCRAPER_C} bash -c "'
    f'export PYTHONPATH=/app/src && '
    f'python /app/src/Pipeline/producer/avito_producer.py '
    f'--mode louer --pages 1 --limit 10 '
    f'--bootstrap {KAFKA_BOOTSTRAP} --topic {KAFKA_TOPIC}"'
)

with DAG(
    dag_id="avito_scraper_only",
    default_args=default_args,
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2025, 11, 2),
    catchup=False,
    max_active_runs=1,
    tags=["avito", "scraper"],
) as dag:
    
    scrape = BashOperator(
        task_id="scrape_avito",
        bash_command=RUN_SCRAPER_CMD
    )
# dags/avito_scraper.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable

# ---------- Paramètres ----------
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
    limit_part = f"--limit {limit} " if limit is not None else ""  # <— note l'espace final
    return (
        f"docker exec -i {SCRAPER_C} bash -lc '"
        f"export PYTHONPATH=/app/src && "
        f"python /app/src/Pipeline/producer/avito_producer.py "
        f"--mode {mode} --pages {pages} "
        f"{limit_part}"
        f"--bootstrap {KAFKA_BOOTSTRAP} --topic {KAFKA_TOPIC}'"
    )


# ---------- Choix du mode (alternance) ----------
def choose_and_toggle_mode() -> str:
    """
    Lit la variable Airflow 'avito_scraper_last_mode' (par défaut: 'acheter')
    pour que la 1ère exécution parte en 'louer', puis alterne à chaque run.
    Retourne le task_id à exécuter.
    """
    last_mode = Variable.get("avito_scraper_last_mode", default_var="acheter")
    next_mode = "louer" if last_mode == "acheter" else "acheter"
    Variable.set("avito_scraper_last_mode", next_mode)
    return "scrape_louer" if next_mode == "louer" else "scrape_acheter"

with DAG(
    dag_id="avito_scraper",
    default_args=default_args,
    schedule="*/5 * * * *",          # Toutes les 5 minutes
    start_date=datetime(2025, 11, 2),
    catchup=False,
    max_active_runs=1,               # évite chevauchement
    tags=["avito", "scraper"],
) as dag:

    # Branche: décide quel task lancer (louer/acheter) et toggle la variable
    choose_mode = BranchPythonOperator(
        task_id="choose_mode",
        python_callable=choose_and_toggle_mode,
    )

    # Deux commandes identiques sauf le --mode
    scrape_louer = BashOperator(
        task_id="scrape_louer",
        bash_command=_make_cmd("louer"),
    )

    scrape_acheter = BashOperator(
        task_id="scrape_acheter",
        bash_command=_make_cmd("acheter"),
    )

    # Task "fin" pour que le DAG soit marqué success après la branche choisie
    done = EmptyOperator(
        task_id="done",
        trigger_rule="none_failed_min_one_success",
    )

    choose_mode >> [scrape_louer, scrape_acheter] >> done

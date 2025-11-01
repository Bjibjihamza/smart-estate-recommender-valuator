# avito_pipeline.py — minimal: ensure topic -> ensure sink -> scrape every 5 min
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "avito", "depends_on_past": False, "retries": 2, "retry_delay": timedelta(minutes=2)}

KAFKA_C   = "kafka"
SCRAPER_C = "avito-scraper"
SPARK_C   = "spark-iceberg"
KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC = "realestate.avito.raw"

ENSURE_TOPIC_CMD = (
    f'docker exec -i {KAFKA_C} bash -lc "'
    f'kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} '
    f'--create --if-not-exists --topic {KAFKA_TOPIC} --partitions 1 --replication-factor 1"'
)

ENSURE_SINK_CMD = f"""
docker exec -i {SPARK_C} bash -lc '
set -euo pipefail
mkdir -p /opt/work/checkpoints/avito_raw /opt/work/logs
cat > /opt/work/run_avito_sink.sh << "SH"
#!/usr/bin/env bash
set -euo pipefail
if pgrep -f "iceberg_kafka_sink.py .*--topic {KAFKA_TOPIC}" >/dev/null 2>&1; then
  echo "[sink] already running"; exit 0
fi
nohup /opt/spark/bin/spark-submit \
  /opt/work/src/Pipeline/load/iceberg_kafka_sink.py \
  --rest-uri http://iceberg-rest:8181 \
  --s3-endpoint http://minio:9000 \
  --s3-access-key admin \
  --s3-secret-key admin123 \
  --kafka-bootstrap {KAFKA_BOOTSTRAP} \
  --topic {KAFKA_TOPIC} \
  --table rest.raw.avito \
  --checkpoint file:///opt/work/checkpoints/avito_raw \
  --starting-offsets latest \
  --trigger "15 seconds" \
  > /opt/work/logs/avito_sink.log 2>&1 &
echo "[sink] started"
SH
chmod +x /opt/work/run_avito_sink.sh
/opt/work/run_avito_sink.sh
'
"""

# NEW: write a launcher inside avito-scraper that finds your producer script automatically
PREPARE_SCRAPER_LAUNCHER = f"""
docker exec -i {SCRAPER_C} bash -lc '
set -euo pipefail
cat > /app/run_avito_scraper.sh << "SH"
#!/usr/bin/env bash
set -euo pipefail

# candidate script paths (adjust/extend if you rename files)
CANDIDATES=(
  "/app/src/Pipeline/extract/avito_producer.py"
  "/app/src/avito_producer.py"
  "/app/src/Pipeline/extract/avito_scraper.py"
  "/app/src/avito_scraper.py"
)

SCRIPT=""
for p in "${{CANDIDATES[@]}}"; do
  if [ -f "$p" ]; then SCRIPT="$p"; break; fi
done

if [ -z "$SCRIPT" ]; then
  echo "[launcher] ERROR: could not find avito producer script under /app/src" >&2
  echo "[launcher] HINT: check volume mount and file name (case-sensitive)" >&2
  exit 2
fi

export PYTHONPATH=/app/src
python "$SCRIPT" "$@"
SH
chmod +x /app/run_avito_scraper.sh
'
"""

# Scrape every 5 minutes using the launcher (stable no matter your file layout)
RUN_SCRAPER_CMD = (
    f'docker exec -i {SCRAPER_C} bash -lc "'
    f'/app/run_avito_scraper.sh --mode louer --pages 1 --limit 10 '
    f'--bootstrap {KAFKA_BOOTSTRAP} --topic {KAFKA_TOPIC}"'
)

with DAG(
    dag_id="avito_minimal_scrape_to_iceberg",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2025, 10, 31),
    catchup=False,
    max_active_runs=1,
    tags=["avito", "kafka", "iceberg", "minimal"],
) as dag:
    ensure_topic = BashOperator(task_id="ensure_kafka_topic", bash_command=ENSURE_TOPIC_CMD)
    ensure_sink  = BashOperator(task_id="ensure_streaming_sink", bash_command=ENSURE_SINK_CMD)
    prepare_scraper = BashOperator(task_id="prepare_scraper_launcher", bash_command=PREPARE_SCRAPER_LAUNCHER)
    run_scraper = BashOperator(task_id="run_scraper", bash_command=RUN_SCRAPER_CMD)

    ensure_topic >> ensure_sink >> prepare_scraper >> run_scraper

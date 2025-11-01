# Smart Estate Recommender & Valuator — Minimal Streaming Pipeline

A minimal, reproducible real estate data pipeline that scrapes Avito listings and processes them through a modern streaming architecture.

## 🎯 Overview

**Pipeline Flow:**  
Avito Scraper → **Kafka** → **Spark Structured Streaming** → **Apache Iceberg** (on **MinIO**) → Orchestrated by **Airflow**

This repository provides a complete Docker Compose setup with:
- **Kafka** (KRaft mode, no ZooKeeper) + **Kafka UI**
- **MinIO** object storage (S3-compatible)
- **Iceberg REST** catalog server
- **Spark** worker with Iceberg + AWS SDK integration
- **Avito scraper** container (Python requests/BeautifulSoup4)
- **Airflow** (web server + scheduler + PostgreSQL) with automated DAG:
  - Ensures Kafka topic exists
  - Maintains streaming Spark job
  - Executes scraper every 5 minutes

---

## 📋 Prerequisites

- **Docker** & **Docker Compose** installed
- **~4 GB RAM** available
- **Required ports** free:
  - `8088` - Airflow web UI
  - `8090` - Kafka UI
  - `9000` - MinIO API
  - `9001` - MinIO console
  - `8181` - Iceberg REST server

---

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <YOUR_REPO_URL>
cd smart-estate-recommender-valuator
```

### 2. Configure Environment

Create a `.env` file in the repository root:

```env
# MinIO Configuration
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin123
LAKE_BUCKET=lake

# Kafka Configuration
KAFKA_OUTSIDE_PORT=9094
KAFKA_UI_PORT=8090

# Airflow Configuration
AIRFLOW_USER=admin
AIRFLOW_PASSWORD=admin

# Optional: MinIO Ports (defaults)
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
```

### 3. Build and Start Services

```bash
docker compose up -d --build
```

Wait for all services to become healthy:
- ✅ Kafka is healthy
- ✅ MinIO is healthy
- ✅ Airflow web & scheduler are running
- ✅ Spark-Iceberg worker is running

### 4. Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8088 | admin / admin |
| **Kafka UI** | http://localhost:8090 | - |
| **MinIO Console** | http://localhost:9001 | admin / admin123 |
| **Iceberg REST** | http://localhost:8181 | (service endpoint) |

---

## 📊 Pipeline DAG

**File:** `dags/avito_pipeline.py`  
**Schedule:** Every 5 minutes

### DAG Tasks:

1. **Create Kafka Topic**  
   Creates `realestate.avito.raw` topic if it doesn't exist

2. **Ensure Spark Streaming Sink**  
   - Submits `src/Pipeline/load/iceberg_kafka_sink.py`
   - Runs in background with file-based checkpointing
   - Writes to Iceberg table `rest.raw.avito`

3. **Run Scraper**  
   - Executes scraper in `avito-scraper` container
   - Scrapes listings with parameters:
     - Mode: `louer` (rent)
     - Pages: 1
     - Limit: 10 listings
     - Outputs to Kafka topic

### Manual Trigger

**Via Airflow UI:** Click the play button on the DAG

**Via CLI:**
```bash
docker exec -it airflow-web bash -lc "airflow dags trigger avito_minimal_scrape_to_iceberg"
```

Check task logs: **Airflow UI → Graph View → Click Task → Logs**

---

## ✅ Verification & Testing

### 1. Check Producer Logs

View Airflow task logs for output like:

```
[*] Mode: LOUER | Pages: 1 | Sink: kafka | Limit: 10
[*] Total URLs: 10
[1/10] https://www.avito.ma/...
[2/10] https://www.avito.ma/...
```

### 2. Verify Spark Streaming Job

Check if the sink is running:

```bash
docker exec -it spark-iceberg bash -lc 'pgrep -fal iceberg_kafka_sink.py || true'
```

View sink logs:

```bash
docker exec -it spark-iceberg bash -lc 'tail -n 200 /opt/work/logs/avito_sink.log'
```

### 3. Query Iceberg Data with Spark SQL

Create SQL query file:

```bash
docker exec -it spark-iceberg bash -lc "cat > /opt/work/check_avito.sql <<'SQL'
SELECT count(*) AS n FROM rest.raw.avito;
SELECT id, substr(payload,1,120) AS sample, ingest_ts
FROM rest.raw.avito
ORDER BY ingest_ts DESC
LIMIT 5;
SQL"
```

Execute query:

```bash
docker exec -it spark-iceberg bash -lc '/opt/spark/bin/spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.path-style-access=true \
  --conf spark.sql.catalog.rest.s3.access-key-id=${MINIO_ROOT_USER:-admin} \
  --conf spark.sql.catalog.rest.s3.secret-access-key=${MINIO_ROOT_PASSWORD:-admin123} \
  -S -f /opt/work/check_avito.sql'
```

**Expected output:** Row count and 5 most recent records

---

## 🛠️ Troubleshooting

### Issue: `can't open file '/app/src/Pipeline/extract/avito_producer.py'`

**Cause:** Producer script not found at expected location

**Solution:** The launcher checks these paths in order:
- `/app/src/Pipeline/extract/avito_producer.py`
- `/app/src/avito_producer.py`
- `/app/src/Pipeline/extract/avito_scraper.py`
- `/app/src/avito_scraper.py`

If your producer is at `src/Pipeline/producer/avito_producer.py`, create a wrapper:

```python
# /app/src/avito_producer.py
from Pipeline.producer.avito_producer import main

if __name__ == "__main__":
    main()
```

### Issue: S3 403 / Iceberg Read Problems

**Cause:** Missing or incorrect MinIO credentials

**Solution:** Ensure all Spark/Iceberg configurations include:

```bash
--conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000
--conf spark.sql.catalog.rest.s3.path-style-access=true
--conf spark.sql.catalog.rest.s3.access-key-id=admin
--conf spark.sql.catalog.rest.s3.secret-access-key=admin123
```

### Issue: Airflow Can't See DAG

**Check DAG file exists:**

```bash
docker exec -it airflow-web bash -lc "ls -la /opt/airflow/dags && airflow dags list"
```

**View logs:**

```bash
docker logs -f airflow-web
docker logs -f airflow-scheduler
```

### Issue: Windows PowerShell Quoting Problems

**Solution:** Use the SQL file pattern shown above to avoid command-line quoting issues

---

## 📁 Repository Structure

```
smart-estate-recommender-valuator/
├── dags/
│   └── avito_pipeline.py              # Airflow DAG definition
├── src/
│   └── Pipeline/
│       ├── extract/
│       │   └── avito_scraper.py       # HTTP scraper (SERP + details)
│       ├── producer/
│       │   └── avito_producer.py      # Kafka producer
│       └── load/
│           ├── iceberg_kafka_sink.py  # Spark streaming → Iceberg
│           └── init_iceberg_catalog.py
├── docker-compose.yml                 # Service orchestration
├── Dockerfile                         # Scraper image
├── Dockerfile.spark                   # Spark-Iceberg image
├── .env                               # Environment configuration
└── README.md                          # This file
```

---

## 🔧 Useful Commands

### Rebuild Scraper Only

```bash
docker compose up -d --build scraper
```

### Trigger DAG Immediately

```bash
docker exec -it airflow-web bash -lc "airflow dags trigger avito_minimal_scrape_to_iceberg"
```

### View Kafka Messages

Open Kafka UI: http://localhost:8090

### Inspect MinIO Objects

- **Console UI:** http://localhost:9001
- Navigate to `lake/warehouse` bucket to browse Iceberg files

### View Container Logs

```bash
docker logs -f <container_name>
# Examples:
docker logs -f airflow-scheduler
docker logs -f spark-iceberg
docker logs -f kafka
```

### Stop All Services

```bash
docker compose down
```

### Clean Restart (Remove Volumes)

```bash
docker compose down -v
docker compose up -d --build
```

---

## 📝 Notes

- The pipeline processes data in near real-time through Kafka streaming
- Iceberg provides ACID transactions and time travel capabilities
- MinIO serves as S3-compatible object storage for the data lake
- Airflow ensures automated orchestration and monitoring
- All configurations use Docker network internal hostnames (e.g., `kafka:9092`, `minio:9000`)

---

## 🤝 Contributing

Feel free to submit issues and enhancement requests!

## 📄 License

[Add your license information here]
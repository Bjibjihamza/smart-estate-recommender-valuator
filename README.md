# 🏙️ Smart Estate Recommender & Valuator

A production-ready real estate data pipeline that scrapes **Avito** and **Mubawab** listings and processes them through a modern streaming architecture.

## 🎯 Overview

**Pipeline Flow:**

```
Avito/Mubawab Scrapers → Kafka → Spark Streaming → Iceberg RAW → Transformation → Iceberg SILVER
```

This repository provides a complete Docker Compose setup with:

- **Kafka** (KRaft mode) + Kafka UI for monitoring
- **MinIO** object storage (S3-compatible)
- **Apache Iceberg** REST catalog server
- **Spark** with Iceberg integration
- **Dual Scrapers** (Avito & Mubawab) with BeautifulSoup4
- **Apache Airflow** for orchestration
- **JupyterLab** for data analysis

---

## 📋 Prerequisites

- Docker & Docker Compose installed
- ~4 GB RAM available
- Required ports free:
  - `8088` — Airflow web UI
  - `8090` — Kafka UI
  - `9000` — MinIO API
  - `9001` — MinIO console
  - `8181` — Iceberg REST server
  - `8888` — JupyterLab

---

## 🚀 Quick Start

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/Bjibjihamza/smart-estate-recommender-valuator.git
cd smart-estate-recommender-valuator
```

### 2️⃣ Configure Environment

Create a `.env` file in the repository root:

```bash
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

# Spark Notebook
JUPYTER_TOKEN=serv

# Optional Ports
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
```

### 3️⃣ Build and Start Services

**Build Spark base image:**

```bash
docker compose build spark-iceberg
```

**Start all services:**

```bash
docker compose up -d --build
```

Wait ~2–3 minutes for all services to become healthy.

**Verify running containers:**

```bash
docker ps
```

**Expected containers:**
✅ kafka  
✅ kafka-ui  
✅ minio  
✅ minio-setup  
✅ iceberg-rest  
✅ spark-iceberg  
✅ spark-notebook  
✅ airflow-db  
✅ airflow-init  
✅ airflow-webserver  
✅ airflow-scheduler  

### ⚙️ Initialize Airflow Admin User

Create admin user (first time only):

```bash
docker exec -it airflow-webserver bash -lc "airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin"
```

If you see "User already exists", skip this step.

---

## 🧊 Create Iceberg Tables & Namespaces

### 📁 Table Structure

```
rest/
├── raw/
│   ├── avito (id, payload, ingest_ts)    ← Raw JSON from Kafka
│   └── mubawab (id, payload, ingest_ts)  ← Raw JSON from Kafka
└── silver/
    └── avito (31 columns)                 ← Cleaned & structured data
```

### Step 1: Create RAW Layer

**Schema:**
- `id` (STRING) — Listing ID
- `payload` (STRING) — Raw JSON document
- `ingest_ts` (TIMESTAMP) — Ingestion timestamp
- **Partitioned by:** `days(ingest_ts)`

**Command:**

```bash
docker exec -it spark-iceberg bash -lc "export PYTHONPATH=/opt/work/src && /opt/spark/bin/spark-submit --master local[*] /opt/work/src/database/bronze.py --sources avito mubawab"
```

**Expected output:**

```
============================================================
Creating Raw Layer in Iceberg
============================================================
[INFO] Creating namespace 'raw' if not exists...
[INFO] Creating table 'raw.avito' with schema...
[SUCCESS] Raw namespace and table created successfully!

============================================================
Creating Raw Layer in Iceberg
============================================================
[INFO] Creating namespace 'raw' if not exists...
[INFO] Creating table 'raw.mubawab' with schema...
[SUCCESS] Raw namespace and table created successfully!
```

### Step 2: Create SILVER Layer

**Schema (31 columns):**
- **Core:** id, url, title, price, description
- **Seller:** seller_name, seller_type
- **Location:** city, neighborhood, site
- **Metadata:** offer, property_type, published_date, ingest_ts
- **Arrays:** image_urls, equipments
- **Property:** living_area, bedrooms, floor, etc.
- **Partitioned by:** `days(ingest_ts)`

**Command:**

```bash
docker exec -it spark-iceberg bash -lc "export PYTHONPATH=/opt/work/src && /opt/spark/bin/spark-submit --master local[*] /opt/work/src/database/silver.py"
```

**Expected output:**

```
============================================================
Creating Silver Layer in Iceberg
============================================================
[INFO] Creating namespace 'silver' if not exists...
[INFO] Creating table 'silver.avito' with schema...
[SUCCESS] Silver namespace and table created successfully!
```

### Step 3: Verify Tables

**Command:**

```bash
docker exec -it spark-iceberg bash -lc "/opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=rest \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e 'SHOW NAMESPACES; SHOW TABLES IN raw; SHOW TABLES IN silver;'"
```

or u can acces on notebooks*

**Expected output:**

```
namespace
---------
raw
silver

namespace  tableName
---------  ---------
raw        avito
raw        mubawab

namespace  tableName
---------  ---------
silver     avito
```

---

## 4️⃣ Launch the Streaming Sinks (Kafka → Iceberg RAW)

These Spark jobs consume Kafka topics and append to the RAW Iceberg tables.

### Choose your starting mode

- **Prod (recommended):** `--starting-offsets latest` → only new messages
- **Backfill/first run:** `--starting-offsets earliest`

⚠️ If you change starting offsets, delete the corresponding checkpoint first.

### A) Prepare folders (checkpoints, logs, Ivy cache)

```bash
docker exec -it spark-iceberg bash -lc '
  mkdir -p /opt/work/checkpoints/avito_raw /opt/work/checkpoints/mubawab_raw ;
  mkdir -p /opt/work/logs ;
  mkdir -p /tmp/ivy
'
```

### B) Start sinks (daemonized with logs + PID files)

**Avito → rest.raw.avito:**

```bash
docker exec -it spark-iceberg bash -lc '
  export PYTHONPATH=/opt/work/src ;
  nohup /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.jars.ivy=/tmp/ivy \
    --conf spark.sql.shuffle.partitions=2 \
    --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.kafka:kafka-clients:3.5.1,\
org.apache.commons:commons-pool2:2.11.1,\
org.lz4:lz4-java:1.8.0,\
org.xerial.snappy:snappy-java:1.1.10.5 \
    /opt/work/src/pipeline/load/iceberg_kafka_sink.py \
      --source avito \
      --rest-uri http://iceberg-rest:8181 \
      --s3-endpoint http://minio:9000 --s3-access-key admin --s3-secret-key admin123 \
      --kafka-bootstrap kafka:9092 \
      --topic realestate.avito.raw \
      --table rest.raw.avito \
      --checkpoint /opt/work/checkpoints/avito_raw \
      --starting-offsets latest \
      --trigger 10s \
    >> /opt/work/logs/avito_sink.log 2>&1 & echo $! > /opt/work/logs/avito_sink.pid
'
```

**Mubawab → rest.raw.mubawab:**

```bash
docker exec -it spark-iceberg bash -lc '
  export PYTHONPATH=/opt/work/src ;
  nohup /opt/spark/bin/spark-submit \
    --master local[*] \
    --conf spark.jars.ivy=/tmp/ivy \
    --conf spark.sql.shuffle.partitions=2 \
    --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
org.apache.kafka:kafka-clients:3.5.1,\
org.apache.commons:commons-pool2:2.11.1,\
org.lz4:lz4-java:1.8.0,\
org.xerial.snappy:snappy-java:1.1.10.5 \
    /opt/work/src/pipeline/load/iceberg_kafka_sink.py \
      --source mubawab \
      --rest-uri http://iceberg-rest:8181 \
      --s3-endpoint http://minio:9000 --s3-access-key admin --s3-secret-key admin123 \
      --kafka-bootstrap kafka:9092 \
      --topic realestate.mubawab.raw \
      --table rest.raw.mubawab \
      --checkpoint /opt/work/checkpoints/mubawab_raw \
      --starting-offsets latest \
      --trigger 10s \
    >> /opt/work/logs/mubawab_sink.log 2>&1 & echo $! > /opt/work/logs/mubawab_sink.pid
'
```

### C) Check they're running

**Processes:**

```bash
docker exec -it spark-iceberg bash -lc "ps -ef | grep 'iceberg_kafka_sink.py' | grep -v grep"
```

**Live logs:**

```bash
docker exec -it spark-iceberg bash -lc 'tail -n 120 /opt/work/logs/avito_sink.log || true'
docker exec -it spark-iceberg bash -lc 'tail -n 120 /opt/work/logs/mubawab_sink.log || true'
```

**Container logs (quick signal that streaming started):**

```bash
docker logs spark-iceberg --tail 50
```

You should see lines like:

```
Streaming started → rest.raw.avito (checkpoint: file:///opt/work/checkpoints/avito_raw)
Committed snapshot ... addedRecords=...
```

### D) Quick verification in Iceberg

```bash
docker exec -it spark-iceberg bash -lc "
/opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=rest \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3a://warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.path-style-access=true \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e '
    SELECT COUNT(*) AS avito_total   FROM rest.raw.avito;
    SELECT COUNT(*) AS mubawab_total FROM rest.raw.mubawab;
  '
"
```

---

## 5️⃣ Enable Airflow DAGs

**Restart Airflow to apply changes:**

```bash
docker restart airflow-scheduler airflow-webserver
```

---

## 🌐 Access the UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8088 | `admin / admin` |
| **Kafka UI** | http://localhost:8090 | - |
| **MinIO Console** | http://localhost:9001 | `admin / admin123` |
| **JupyterLab** | http://localhost:8888/lab?token=serv | `Token: serv` |

---

## ✅ Verification & Testing

### Check Kafka Messages

**Avito topic:**

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic realestate.avito.raw --from-beginning --max-messages 1
```

**Mubawab topic:**

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic realestate.mubawab.raw --from-beginning --max-messages 1
```

### Check RAW Table Row Counts

```bash
docker exec -it spark-iceberg bash -lc "/opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=rest \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e 'SELECT COUNT(*) as total FROM rest.raw.avito; SELECT COUNT(*) as total FROM rest.raw.mubawab;'"
```

### Check SILVER Table Row Count

```bash
docker exec -it spark-iceberg bash -lc "/opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=rest \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e 'SELECT COUNT(*) as total FROM rest.silver.avito;'"
```

### View Sample SILVER Data

```bash
docker exec -it spark-iceberg bash -lc "/opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=rest \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e 'SELECT id, title, price, city, offer FROM rest.silver.avito LIMIT 5;'"
```

---

## 🧠 Exploratory Data Analysis (EDA)

### Access JupyterLab

- **URL:** http://localhost:8888/lab?token=serv
- **Workspace:** `/opt/work/notebooks/`

### Connect to Iceberg from Notebook

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Iceberg Analysis")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    .config("spark.sql.catalog.rest.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.rest.warehouse", "s3://lake/warehouse")
    .config("spark.sql.catalog.rest.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.rest.s3.path-style-access", "true")
    .config("spark.sql.catalog.rest.s3.access-key-id", "admin")
    .config("spark.sql.catalog.rest.s3.secret-access-key", "admin123")
    .config("spark.sql.defaultCatalog", "rest")
    .getOrCreate()
)

# Load silver data
df = spark.table("rest.silver.avito")
df.printSchema()
df.show(5)

# Analysis examples
df.groupBy("city", "offer").count().show()
df.groupBy("offer").avg("price").show()
```

---

## 🛠️ Troubleshooting

### Restart Streaming Sink

If the streaming job stops or you need to restart it:

**Kill existing process:**

```bash
docker exec -it spark-iceberg pkill -f iceberg_kafka_sink
```

**Restart Avito sink:**

```bash
docker exec -d spark-iceberg bash -lc "export PYTHONPATH=/opt/work/src && /opt/spark/bin/spark-submit --master local[*] /opt/work/src/pipeline/load/iceberg_kafka_sink.py --source avito --starting-offsets latest"
```

**Restart Mubawab sink:**

```bash
docker exec -d spark-iceberg bash -lc "export PYTHONPATH=/opt/work/src && /opt/spark/bin/spark-submit --master local[*] /opt/work/src/pipeline/load/iceberg_kafka_sink.py --source mubawab --starting-offsets latest"
```

### Check Logs

**Streaming sink logs:**

```bash
docker logs spark-iceberg --tail 100 -f
```

**Airflow scheduler logs:**

```bash
docker logs airflow-scheduler --tail 100 -f
```

**Airflow webserver logs:**

```bash
docker logs airflow-webserver --tail 100 -f
```

### View Airflow Task Logs

1. Go to http://localhost:8088
2. Click on the **avito_pipeline** or **mubawab_pipeline** DAG
3. Click on a task instance
4. Click **Log** button

### Recreate Tables

If you need to drop and recreate tables:

```bash
docker exec -it spark-iceberg bash -lc "/opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=rest \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e 'DROP TABLE IF EXISTS rest.raw.avito; DROP TABLE IF EXISTS rest.raw.mubawab; DROP TABLE IF EXISTS rest.silver.avito;'"
```

Then run the creation scripts again (Step 1 and 2 from "Create Iceberg Tables" section).

### Check Service Health

**All services status:**

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

**Check specific service logs:**

```bash
docker logs <container-name>
```

---

## 📁 Repository Structure

```
smart-estate-recommender-valuator/
├── dags/
│   ├── avito_pipeline.py              # Avito Airflow DAG
│   └── mubawab_pipeline.py            # Mubawab Airflow DAG
├── src/
│   ├── database/
│   │   ├── bronze.py                  # RAW table creation (both sources)
│   │   └── silver.py                  # SILVER table creation
│   ├── notebooks/                     # Jupyter workspace
│   │   ├── EDA_to_silver.ipynb
│   │   └── init_iceberg.ipynb
│   └── pipeline/
│       ├── extract/
│       │   ├── avito_scraper.py       # Avito web scraper
│       │   └── mubawab_scraper.py     # Mubawab web scraper
│       ├── producer/
│       │   ├── avito_producer.py      # Avito Kafka producer
│       │   └── mubawab_producer.py    # Mubawab Kafka producer
│       ├── consumer/
│       │   ├── avito_consumer.py      # Avito Kafka consumer
│       │   └── mubawab_consumer.py    # Mubawab Kafka consumer
│       ├── load/
│       │   ├── iceberg_kafka_sink.py  # Streaming sink (multi-source)
│       │   └── silver_loader.py       # Silver writer
│       └── transform/
│           └── avito_raw_to_silver.py # Transformation logic
├── Dockerfile                         # Scraper image
├── Dockerfile.spark                   # Spark + Iceberg image
├── docker-compose.yml                 # Stack definition
├── .env                               # Environment variables
└── README.md                          # This file
```

---

## 🎓 Data Pipeline Architecture

```
┌──────────────────────────────────────────────────────────┐
│                  DATA PIPELINE FLOW                       │
└──────────────────────────────────────────────────────────┘

┌─────────────┐     ┌─────────────┐
│  Avito.ma   │     │ Mubawab.ma  │  ← Real estate marketplaces
└──────┬──────┘     └──────┬──────┘
       │                   │ HTTP Scraping
       ↓                   ↓
┌─────────────────┐ ┌─────────────────┐
│ Python Scraper  │ │ Python Scraper  │  ← BeautifulSoup4 + Requests
│ (Every 5 min)   │ │ (Every 5 min)   │  ← Orchestrated by Airflow
└──────┬──────────┘ └──────┬──────────┘
       │ JSON                │ JSON
       ↓                     ↓
┌─────────────────┐ ┌─────────────────┐
│ Kafka Topic     │ │ Kafka Topic     │  ← realestate.{source}.raw
│ (Avito)         │ │ (Mubawab)       │  ← Message brokers
└──────┬──────────┘ └──────┬──────────┘
       │                   │ Real-time Stream
       ↓                   ↓
┌─────────────────────────────────────┐
│       Spark Streaming               │  ← Micro-batches (10 sec)
│       (Continuous)                  │  ← Structured Streaming
└──────┬──────────────────────────────┘
       │ Write Parquet
       ↓
┌─────────────────────────────────────┐
│     Iceberg RAW (MinIO Storage)     │  ← rest.raw.{avito|mubawab}
│     Partitioned by day              │  ← Time-travel enabled
└──────┬──────────────────────────────┘
       │ Batch Transform (Every 5 min)
       ↓
┌─────────────────────────────────────┐
│     Spark Batch Transformation      │  ← Parse JSON, Clean, Enrich
│     (Deduplicate, Validate)         │  ← Quality checks
└──────┬──────────────────────────────┘
       │ Write Parquet
       ↓
┌─────────────────────────────────────┐
│     Iceberg SILVER                  │  ← rest.silver.avito
│     (Analytics-Ready)               │  ← 31 structured columns
└──────┬──────────────────────────────┘
       │
       ↓
┌─────────────────────────────────────┐
│     Analytics/ML Consumption        │  ← JupyterLab, SQL
│     (Recommendations, Valuations)   │  ← Business Intelligence
└─────────────────────────────────────┘
```

---

## 📊 Pipeline Metrics

### Latency
- **Scraping → Kafka:** < 1 second
- **Kafka → RAW:** ~10 seconds (micro-batch)
- **RAW → SILVER:** ~5 minutes (batch transform)

### Frequency
- **Scraping:** Every 5 minutes (Airflow)
- **Streaming:** Continuous (10-second triggers)
- **Transformation:** Every 5 minutes (Airflow)

### Data Volume
- **RAW:** JSON format (~2-5 KB/record)
- **SILVER:** Parquet format (~1 KB/record)

### Data Sources
- **Avito:** Primary real estate marketplace
- **Mubawab:** Secondary real estate marketplace
- **Combined:** Comprehensive market coverage

---

## 📄 License

MIT License - 2025 © Hamza Bjibji

---

## 🎯 Next Steps

After completing this setup, consider:

1. **Gold Layer** — Create aggregated tables for analytics
2. **Machine Learning** — Price prediction, recommendations
3. **API Layer** — Expose data via FastAPI
4. **Visualization** — Add Grafana/Superset dashboards
5. **Monitoring** — Implement Prometheus + Grafana
6. **Testing** — Add unit and integration tests
7. **CI/CD** — Automate deployment pipeline
8. **Mubawab Silver Layer** — Extend transformation to Mubawab data

---

**Happy Data Engineering! 🚀**
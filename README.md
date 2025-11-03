# 🏙️ Smart Estate Recommender & Valuator

A minimal, reproducible **real estate data pipeline** that scrapes Avito listings and processes them through a modern streaming architecture.

---

## 🎯 Overview

**Pipeline Flow:**
Avito Scraper → **Kafka** → **Spark Structured Streaming** → **Apache Iceberg** (on **MinIO**) → Orchestrated by **Airflow**

This repository provides a complete Docker Compose setup with:

* **Kafka (KRaft mode)** + **Kafka UI**
* **MinIO** object storage (S3-compatible)
* **Iceberg REST** catalog server
* **Spark** worker with Iceberg + AWS SDK integration
* **Avito Scraper** container (Python requests/BeautifulSoup4)
* **Airflow** (web server + scheduler + PostgreSQL) with automated DAG:
  * Ensures Kafka topic exists
  * Maintains streaming Spark job
  * Executes scraper every 5 minutes

---

## 📋 Prerequisites

* **Docker** & **Docker Compose** installed
* **~4 GB RAM** available
* **Required ports** free:
  * `8088` — Airflow web UI
  * `8090` — Kafka UI
  * `9000` — MinIO API
  * `9001` — MinIO console
  * `8181` — Iceberg REST server
  * `8888` — JupyterLab (EDA Silver)

---

## 🚀 Quick Start

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/Bjibjihamza/smart-estate-recommender-valuator.git
cd smart-estate-recommender-valuator
```

### 2️⃣ Configure Environment

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

# Spark Notebook
JUPYTER_TOKEN=serv

# Optional Ports
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
```

### 3️⃣ Build and Start Services

```bash
docker compose up -d --build
```

Wait ~2–3 minutes for all services to become healthy.

Verify:

```bash
docker ps
```

Expected running containers:
✅ kafka
✅ minio
✅ iceberg-rest
✅ spark-iceberg
✅ spark-notebook
✅ airflow-db
✅ airflow-web
✅ airflow-scheduler
✅ avito-scraper

---

## ⚙️ Initialize Airflow Admin User (First Time Only)

```bash
docker exec -it airflow-web airflow users create \
  --username admin --firstname Admin --lastname User \
  --role Admin --email admin@example.com --password admin
```

If "User already exists", skip this step.

---

## 🧊 Create Iceberg Tables & Namespaces

We use Python scripts to create the Iceberg namespaces and tables programmatically. This approach is cleaner and more maintainable than manual SQL commands.

### 📁 Table Structure

```
rest/
├── raw/
│   └── avito (id, payload, ingest_ts)  ← Raw JSON data from Kafka
└── silver/
    └── avito (31 columns)              ← Cleaned & structured data
```

### a. Create RAW Layer

The **raw** layer stores the original JSON payload from Kafka with minimal processing.

**Schema:**
- `id` (STRING) — Listing ID
- `payload` (STRING) — Raw JSON document
- `ingest_ts` (TIMESTAMP) — Ingestion timestamp
- **Partitioned by:** `days(ingest_ts)`

**Run:**

```bash
docker exec -it spark-iceberg bash -lc "
/opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/work/src/database/raw.py
"
```

**Expected output:**
```
============================================================
Creating Raw Layer in Iceberg
============================================================
[INFO] Creating namespace 'raw' if not exists...
[INFO] Creating table 'raw.avito' with schema...
[SUCCESS] Raw namespace and table created successfully!
```

### b. Create SILVER Layer

The **silver** layer contains cleaned, structured, and enriched data ready for analytics.

**Schema (31 columns):**
- Core fields: `id`, `url`, `title`, `price`, `description`
- Seller info: `seller_name`, `seller_type`
- Location: `city`, `neighborhood`, `site`
- Metadata: `offre`, `type`, `published_date`, `ingest_ts`
- Arrays: `image_urls`, `equipments`
- Property attributes: `Surface habitable`, `Chambres`, `Étage`, etc.
- **Partitioned by:** `days(ingest_ts)`

**Run:**

```bash
docker exec -it spark-iceberg bash -lc "
/opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/work/src/database/silver.py
"
```

**Expected output:**
```
============================================================
Creating Silver Layer in Iceberg
============================================================
[INFO] Creating namespace 'silver' if not exists...
[INFO] Creating table 'silver.avito' with schema...
[SUCCESS] Silver namespace and table created successfully!
+---------+---------+-----------+
|namespace|tableName|isTemporary|
+---------+---------+-----------+
|silver   |avito    |false      |
+---------+---------+-----------+
```

### c. Verify Tables Creation

```bash
docker exec -it spark-iceberg /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e "SHOW NAMESPACES IN rest; SHOW TABLES IN rest.raw; SHOW TABLES IN rest.silver;"
```

---

## 🔁 Start Kafka → Iceberg Streaming Sink

This streaming job continuously reads from Kafka and writes to the **raw.avito** table.

```bash
docker exec -d spark-iceberg bash -c "nohup /opt/spark/bin/spark-submit --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 --conf spark.sql.catalog.rest.s3.path-style-access=true --conf spark.sql.catalog.rest.s3.access-key-id=admin --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 --conf spark.sql.defaultCatalog=rest /opt/work/src/Pipeline/load/iceberg_kafka_sink.py --kafka-bootstrap kafka:9092 --topic realestate.avito.raw --table rest.raw.avito --checkpoint file:///opt/work/checkpoints/avito_raw --starting-offsets latest --trigger '15 seconds' > /opt/work/logs/avito_sink.log 2>&1 &"

```

Check it's running:

```bash
docker exec -it spark-iceberg ps aux | grep spark-submit
```

View logs:

```bash
docker exec -it spark-iceberg tail -f /opt/work/logs/avito_sink.log
```

---

## 🌐 Access the UIs

| Service              | URL                                                                          | Credentials        |
| -------------------- | ---------------------------------------------------------------------------- | ------------------ |
| **Airflow**          | [http://localhost:8088](http://localhost:8088)                               | `admin / admin`    |
| **Kafka UI**         | [http://localhost:8090](http://localhost:8090)                               | -                  |
| **MinIO Console**    | [http://localhost:9001](http://localhost:9001)                               | `admin / admin123` |
| **JupyterLab (EDA)** | [http://localhost:8888/lab?token=serv](http://localhost:8888/lab?token=serv) | `Token: serv`      |

---

## ✅ Verification & Testing

### Check Kafka Messages

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic realestate.avito.raw \
  --from-beginning --max-messages 1
```

### Check RAW Table Data

```bash
docker exec -it spark-iceberg /opt/spark/bin/spark-sql --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 --conf spark.sql.catalog.rest.s3.path-style-access=true --conf spark.sql.catalog.rest.s3.access-key-id=admin --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 --conf spark.sql.defaultCatalog=rest -S -e "SHOW NAMESPACES; SHOW TABLES IN rest.raw; SELECT COUNT(*) AS raw_rows FROM rest.raw.avito; SELECT COUNT(*) AS silver_rows FROM rest.silver.avito;"
```

### Check SILVER Table (After Transformation)

```bash
docker exec -it spark-iceberg /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e "SELECT id, title, price, city, offre FROM rest.silver.avito LIMIT 10;"
```

---

## 🧠 Exploratory Data Analysis (EDA — Silver Dataset)

A **JupyterLab environment** is integrated for interactive data exploration and preparation before building **Silver** tables.

### 📘 Notebook Environment

* **Container:** `spark-notebook`
* **URL:** [http://localhost:8888/lab?token=serv](http://localhost:8888/lab?token=serv)
* **Workspace:** `/opt/work/notebooks/`

### ⚙️ Configuration

The JupyterLab image includes:

* **Apache Spark 3.5.1**
* **Iceberg runtime 1.6.0**
* **Python 3.8 + PySpark, Pandas, Boto3**
* Predefined token via `.env`:

  ```env
  JUPYTER_TOKEN=serv
  ```

### 🧪 Example: Connect to Iceberg

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Iceberg via REST")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
    .config("spark.sql.catalog.local.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.local.warehouse", "s3://lake/warehouse")
    .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.local.s3.path-style-access", "true")
    .config("spark.sql.catalog.local.s3.access-key-id", "admin")
    .config("spark.sql.catalog.local.s3.secret-access-key", "admin123")
    .getOrCreate()
)

# Load raw data
raw_df = spark.table("local.raw.avito")
raw_df.show(5)

# Load silver data
silver_df = spark.table("local.silver.avito")
silver_df.printSchema()
```

Access shell if needed:

```bash
docker exec -it spark-notebook bash
```

---

## 📊 Pipeline DAG

**File:** `dags/avito_scraper.py`
**Schedule:** Every 5 minutes

### DAG Tasks

1. **Choose Mode** — Alternates between `louer` (rent) and `acheter` (buy)
2. **Run Scraper** — Extracts & pushes new Avito listings to Kafka
3. **Done** — Marks completion

The DAG automatically alternates between scraping rental and sale listings on each run.

---

## 🛠️ Troubleshooting

### Restart Streaming Sink

If the streaming job stops:

```bash
# Kill existing process
docker exec -it spark-iceberg pkill -f iceberg_kafka_sink

# Restart (use command from section above)
docker exec -d spark-iceberg bash -c "nohup /opt/spark/bin/spark-submit ..."
```

### Check Logs

```bash
# Streaming sink logs
docker exec -it spark-iceberg tail -f /opt/work/logs/avito_sink.log

# Airflow logs
docker logs airflow-scheduler -f

# Spark logs
docker logs spark-iceberg -f
```

### Recreate Tables

If you need to drop and recreate tables:

```bash
docker exec -it spark-iceberg /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  -e "DROP TABLE IF EXISTS rest.raw.avito; DROP TABLE IF EXISTS rest.silver.avito;"

# Then run the creation scripts again
```

---

## 📁 Repository Structure

```
smart-estate-recommender-valuator/
├── dags/
│   └── avito_scraper.py           # Airflow DAG for orchestration
├── src/
│   ├── database/                  # Table creation scripts
│   │   ├── raw.py                 # Creates rest.raw.avito
│   │   └── silver.py              # Creates rest.silver.avito
│   ├── notebooks/                 # Jupyter EDA workspace
│   └── Pipeline/
│       ├── extract/
│       │   └── avito_scraper.py   # Web scraper logic
│       ├── producer/
│       │   └── avito_producer.py  # Kafka producer
│       └── load/
│           └── iceberg_kafka_sink.py  # Streaming sink
├── Dockerfile                     # Scraper container
├── Dockerfile.spark               # Spark + Iceberg container
├── docker-compose.yml             # Full stack definition
├── .env                           # Environment variables
└── README.md
```

---

## 🎓 Data Pipeline Architecture

```
┌──────────────┐
│ Avito.ma     │
│ (Web Source) │
└──────┬───────┘
       │ HTTP Requests
       ↓
┌──────────────────┐
│ Python Scraper   │  ← Airflow scheduled (every 5 min)
│ (BeautifulSoup4) │
└──────┬───────────┘
       │ JSON Messages
       ↓
┌──────────────────┐
│ Kafka Topic      │  ← realestate.avito.raw
│ (KRaft Mode)     │
└──────┬───────────┘
       │ Streaming Read
       ↓
┌──────────────────┐
│ Spark Structured │  ← Continuous micro-batches (15s)
│ Streaming        │
└──────┬───────────┘
       │ Write
       ↓
┌──────────────────┐
│ Iceberg REST     │  ← rest.raw.avito (partitioned)
│ Catalog + MinIO  │
└──────┬───────────┘
       │ Transform
       ↓
┌──────────────────┐
│ Silver Layer     │  ← rest.silver.avito (cleaned)
│ (31 columns)     │
└──────────────────┘
```

---

## 🤝 Contributing

Contributions and enhancements are welcome!
Create feature branches (e.g., `feature/eda-silver`) and open pull requests.

---

## 📄 License

[MIT License] — 2025 © Hamza Bjibji

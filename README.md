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

---

## ⚙️ Initialize Airflow Admin User (First Time Only)

```bash
docker exec -it airflow-web airflow users create \
  --username admin --firstname Admin --lastname User \
  --role Admin --email admin@example.com --password admin
```

If “User already exists”, skip this step.

---

## 🧊 Create Iceberg Table

### a. Create directories

```bash
docker exec -it spark-iceberg mkdir -p /opt/work/logs /opt/work/checkpoints/avito_raw
```

### b. Create table

```bash
docker exec -it spark-iceberg /opt/spark/bin/spark-sql --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 --conf spark.sql.catalog.rest.s3.path-style-access=true --conf spark.sql.catalog.rest.s3.access-key-id=admin --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 --conf spark.sql.defaultCatalog=rest -e "CREATE NAMESPACE IF NOT EXISTS raw; CREATE TABLE IF NOT EXISTS raw.avito (id STRING, payload STRING, ingest_ts TIMESTAMP) USING iceberg PARTITIONED BY (days(ingest_ts)) TBLPROPERTIES ('write.distribution-mode'='none','format-version'='2');"
```

---

## 🔁 Start Kafka → Iceberg Streaming Sink

```bash
docker exec -d spark-iceberg bash -c "nohup /opt/spark/bin/spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.catalog-impl=org.apache.iceberg.rest.RESTCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.path-style-access=true \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  --conf spark.sql.defaultCatalog=rest \
  /opt/work/src/Pipeline/load/iceberg_kafka_sink.py \
  --kafka-bootstrap kafka:9092 \
  --topic realestate.avito.raw \
  --table rest.raw.avito \
  --checkpoint file:///opt/work/checkpoints/avito_raw \
  --starting-offsets latest \
  --trigger '15 seconds' \
  > /opt/work/logs/avito_sink.log 2>&1 &"
```

Check it’s running:

```bash
docker exec -it spark-iceberg ps aux | grep spark-submit
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

### Check Iceberg Table Data

```bash
docker exec -it spark-iceberg /opt/spark/bin/spark-sql \
  --conf spark.sql.catalog.rest=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.rest.uri=http://iceberg-rest:8181 \
  --conf spark.sql.catalog.rest.warehouse=s3://lake/warehouse \
  --conf spark.sql.catalog.rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.rest.s3.endpoint=http://minio:9000 \
  --conf spark.sql.catalog.rest.s3.access-key-id=admin \
  --conf spark.sql.catalog.rest.s3.secret-access-key=admin123 \
  -e "SELECT COUNT(*) FROM rest.raw.avito;"
```

---

## 🧠 Exploratory Data Analysis (EDA — Silver Dataset)

A new **JupyterLab environment** is integrated for interactive data exploration and preparation before building **Silver** tables.

### 📘 Notebook Environment

* **Container:** `spark-notebook`
* **URL:** [http://localhost:8888/lab?token=serv](http://localhost:8888/lab?token=serv)
* **Workspace:** `/opt/work/notebooks/init_iceberg.ipynb`

### ⚙️ Configuration

The JupyterLab image includes:

* **Apache Spark 3.5.1**
* **Iceberg runtime 1.6.0**
* **Python 3.8 + PySpark, Pandas, Boto3**
* Predefined token via `.env`:

  ```env
  JUPYTER_TOKEN=serv
  ```

### 🧩 Default Setup

* Dependencies installed automatically during build
* Notebook copied to `/opt/work/notebooks`
* Runtime & data directory at `/opt/work/.jupyter`

Access it through the browser or VSCode Remote Jupyter connection.

### 🧪 Example Initialization

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Iceberg via REST")
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

raw_df = spark.table("local.raw.avito")

# Initialize minimal Silver dataset
silver_df = raw_df.select("id").distinct()
silver_df.show(5)

print("✅ Silver dataset initialized with only 'id' column.")
print("Total IDs:", silver_df.count())
```

This initializes the **Silver dataset** (only unique IDs) for future enrichment, cleansing, and analytics.

Access shell if needed:

```bash
docker exec -it spark-notebook bash
```

---

## 📊 Pipeline DAG

**File:** `dags/avito_pipeline.py`
**Schedule:** Every 5 minutes

### DAG Tasks

1. **Create Kafka Topic** — ensures topic exists
2. **Ensure Spark Streaming Sink** — submits streaming job
3. **Run Scraper** — extracts & pushes new Avito listings

---

## 🛠️ Troubleshooting

Common issues and solutions are documented for:

* Kafka producer paths
* S3/Iceberg permissions
* Missing DAGs
* Restarting Spark streaming sinks

(See original Troubleshooting section for full commands.)

---

## 📁 Repository Structure

```
smart-estate-recommender-valuator/
├── dags/
│   └── avito_pipeline.py
├── src/
│   ├── notebooks/                 # Jupyter EDA workspace
│   └── Pipeline/
│       ├── extract/
│       ├── producer/
│       └── load/
├── Dockerfile
├── Dockerfile.spark
├── docker-compose.yml
├── .env
└── README.md
```

---

## 🤝 Contributing

Contributions and enhancements are welcome!
Create feature branches (e.g., `feature/eda-silver`) and open pull requests.

---

## 📄 License

[MIT License] — 2025 © Hamza Bjibji

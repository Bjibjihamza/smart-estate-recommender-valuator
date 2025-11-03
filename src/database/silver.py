#!/usr/bin/env python3
"""
Create Silver namespace and table in Iceberg REST catalog
Usage: python silver.py
"""

import argparse
from pyspark.sql import SparkSession


def build_spark(rest_uri: str, s3_endpoint: str, ak: str, sk: str) -> SparkSession:
    """Initialize Spark session with Iceberg REST catalog configuration"""
    return (
        SparkSession.builder
        .appName("create-silver-table")
        # Iceberg extensions
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.rest.uri", rest_uri)
        .config("spark.sql.defaultCatalog", "rest")
        # S3/MinIO configuration
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
        .config("spark.sql.catalog.rest.s3.access-key-id", ak)
        .config("spark.sql.catalog.rest.s3.secret-access-key", sk)
        .config("spark.sql.catalog.rest.warehouse", "s3://lake/warehouse")
        # Hadoop S3A config
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .getOrCreate()
    )


def create_silver_table(spark: SparkSession):
    """Create the silver namespace and avito table"""
    
    print("[INFO] Creating namespace 'silver' if not exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.silver")
    
    print("[INFO] Creating table 'silver.avito' with schema...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rest.silver.avito (
        id STRING,
        url STRING,
        title STRING,
        price DOUBLE,
        description STRING,
        seller_name STRING,
        seller_type STRING,
        image_urls ARRAY<STRING>,
        equipments ARRAY<STRING>,
        ingest_ts TIMESTAMP,
        offre STRING,
        type STRING,
        city STRING,
        neighborhood STRING,
        site STRING,
        `Surface habitable` STRING,
        `Caution` STRING,
        `Zoning` STRING,
        `Type d'appartement` STRING,
        `Surface totale` STRING,
        `Étage` STRING,
        `Âge du bien` STRING,
        `Salle de bain` STRING,
        `Nombre de pièces` STRING,
        `Chambres` STRING,
        `Frais de syndic / mois` STRING,
        `Condition` STRING,
        `Nombre d'étage` STRING,
        `Disponibilité` STRING,
        `Salons` STRING,
        `Standing` STRING,
        published_date TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(ingest_ts))
    TBLPROPERTIES (
        'write.distribution-mode'='none',
        'format-version'='2'
    )
    """
    
    spark.sql(create_table_sql)
    
    print("[SUCCESS] Silver namespace and table created successfully!")
    print("[INFO] Table: rest.silver.avito")
    print("[INFO] Partitioned by: days(ingest_ts)")
    
    # Verify creation
    print("\n[INFO] Verifying table creation...")
    spark.sql("SHOW TABLES IN rest.silver").show(truncate=False)
    
    print("\n[INFO] Table schema:")
    spark.sql("DESCRIBE rest.silver.avito").show(truncate=False)


def main():
    ap = argparse.ArgumentParser(description="Create Silver namespace and Avito table in Iceberg")
    ap.add_argument("--rest-uri", default="http://iceberg-rest:8181", help="Iceberg REST catalog URI")
    ap.add_argument("--s3-endpoint", default="http://minio:9000", help="S3/MinIO endpoint")
    ap.add_argument("--s3-access-key", default="admin", help="S3 access key")
    ap.add_argument("--s3-secret-key", default="admin123", help="S3 secret key")
    args = ap.parse_args()
    
    print("=" * 60)
    print("Creating Silver Layer in Iceberg")
    print("=" * 60)
    
    spark = build_spark(
        args.rest_uri,
        args.s3_endpoint,
        args.s3_access_key,
        args.s3_secret_key
    )
    
    try:
        create_silver_table(spark)
    finally:
        spark.stop()
        print("\n[INFO] Spark session closed.")


if __name__ == "__main__":
    main()


# how to run 
# docker exec -it spark-iceberg bash -lc "
# /opt/spark/bin/spark-submit \
#   --master local[*] \
#   /opt/work/src/database/silver.py
#  "
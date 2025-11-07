#!/usr/bin/env python3
"""
Create RAW (Bronze) namespace and tables in Iceberg REST catalog.
- rest.raw.avito
- rest.raw.mubawab

Schema (both):
  id STRING,
  payload STRING,
  ingest_ts TIMESTAMP
Partitioning:
  days(ingest_ts)
Iceberg:
  format-version = 2
  write.distribution-mode = none

Usage examples:
  # Create both tables
  spark-submit ... bronze.py --sources avito mubawab

  # Create only avito
  spark-submit ... bronze.py --sources avito
"""

import argparse
from typing import Iterable, List
from pyspark.sql import SparkSession


def build_spark(rest_uri: str, s3_endpoint: str, ak: str, sk: str) -> SparkSession:
    """Initialize Spark session with Iceberg REST catalog configuration"""
    return (
        SparkSession.builder
        .appName("create-bronze-raw-tables")
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
        # Hadoop S3A config (optional but handy)
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .getOrCreate()
    )


def ensure_namespace(spark: SparkSession, catalog: str = "rest", ns: str = "raw") -> None:
    print(f"[INFO] Ensuring namespace '{catalog}.{ns}' exists...")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{ns}")


def create_raw_table(spark: SparkSession, table_fqn: str) -> None:
    """
    Create a raw table (id, payload, ingest_ts) if it doesn't exist.
    Partitioned by days(ingest_ts). Iceberg v2.
    """
    print(f"[INFO] Creating table '{table_fqn}' if not exists...")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
            id STRING,
            payload STRING,
            ingest_ts TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingest_ts))
        TBLPROPERTIES (
            'write.distribution-mode'='none',
            'format-version'='2'
        )
        """
    )
    print(f"[SUCCESS] Table ensured: {table_fqn}")


def create_selected_sources(spark: SparkSession, sources: Iterable[str]) -> List[str]:
    created = []
    ensure_namespace(spark, "rest", "raw")

    for src in sources:
        src_norm = src.strip().lower()
        if src_norm not in {"avito", "mubawab"}:
            print(f"[WARN] Unknown source '{src}'. Skipping (supported: avito, mubawab).")
            continue

        fqn = f"rest.raw.{src_norm}"
        create_raw_table(spark, fqn)
        created.append(fqn)

    # Show summary
    print("\n[INFO] Verifying tables in rest.raw:")
    spark.sql("SHOW TABLES IN rest.raw").show(truncate=False)

    for fqn in created:
        print(f"\n[INFO] Schema for {fqn}:")
        spark.sql(f"DESCRIBE {fqn}").show(truncate=False)

        print(f"\n[INFO] Properties for {fqn}:")
        spark.sql(f"SHOW TBLPROPERTIES {fqn}").show(truncate=False)

    return created


def main():
    ap = argparse.ArgumentParser(description="Create RAW (Bronze) tables in Iceberg for Avito & Mubawab")
    ap.add_argument("--rest-uri", default="http://iceberg-rest:8181", help="Iceberg REST catalog URI")
    ap.add_argument("--s3-endpoint", default="http://minio:9000", help="S3/MinIO endpoint")
    ap.add_argument("--s3-access-key", default="admin", help="S3 access key")
    ap.add_argument("--s3-secret-key", default="admin123", help="S3 secret key")
    ap.add_argument(
        "--sources",
        nargs="+",
        required=True,
        help="One or more sources to create (choices: avito, mubawab). Example: --sources avito mubawab",
    )
    args = ap.parse_args()

    print("=" * 70)
    print("Creating RAW (Bronze) tables in Iceberg")
    print("=" * 70)

    spark = build_spark(
        args.rest_uri,
        args.s3_endpoint,
        args.s3_access_key,
        args.s3_secret_key
    )

    try:
        created = create_selected_sources(spark, args.sources)
        if not created:
            print("[WARN] No tables created. Check your --sources argument.")
        else:
            print("\n[✓] Done. Created/ensured tables:")
            for fqn in created:
                print(f"   - {fqn}")
    finally:
        spark.stop()
        print("\n[INFO] Spark session closed.")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession

def build(rest_uri, s3_endpoint, ak, sk):
    return (
        SparkSession.builder.appName("reset-to-raw")
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.catalog-impl","org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.rest.uri",rest_uri)
        .config("spark.sql.defaultCatalog","rest")
        .config("spark.sql.catalog.rest.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint",s3_endpoint)
        .config("spark.sql.catalog.rest.s3.path-style-access","true")
        .config("spark.sql.catalog.rest.s3.access-key-id",ak)
        .config("spark.sql.catalog.rest.s3.secret-access-key",sk)
        .config("spark.sql.catalog.rest.warehouse","s3://lake/warehouse")
        .getOrCreate()
    )

DDL_RAW = """
CREATE TABLE IF NOT EXISTS rest.raw.avito (
  id STRING,
  payload STRING,
  ingest_ts TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version'='2')
"""

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rest-uri", default="http://iceberg-rest:8181")
    ap.add_argument("--s3-endpoint", default="http://minio:9000")
    ap.add_argument("--s3-access-key", default="admin")
    ap.add_argument("--s3-secret-key", default="admin123")
    args = ap.parse_args()

    spark = build(args.rest_uri, args.s3_endpoint, args.s3_access_key, args.s3_secret_key)

    # drop old objects (idempotent)
    spark.sql("DROP TABLE IF EXISTS rest.lake.avito.bronze_raw_avito")
    spark.sql("DROP TABLE IF EXISTS rest.lake.avito.silver_listings")
    spark.sql("DROP TABLE IF EXISTS rest.lake.avito.gold_listings_daily")
    spark.sql("DROP NAMESPACE IF EXISTS rest.lake.avito CASCADE")
    spark.sql("DROP NAMESPACE IF EXISTS rest.lake RESTRICT")

    # create target namespace + table
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.raw")
    spark.sql(DDL_RAW)

    print("✓ Reset done. Namespace: rest.raw ; Table: rest.raw.avito")
    spark.stop()

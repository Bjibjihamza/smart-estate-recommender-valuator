#!/usr/bin/env python3
# Kafka -> Iceberg (REST) streaming sink (generic: Avito, Mubawab, ...)

import argparse
import re
from pyspark.sql import SparkSession, functions as F


def build_spark(rest_uri: str, s3_endpoint: str, ak: str, sk: str, app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.rest.uri", rest_uri)
        .config("spark.sql.defaultCatalog", "rest")
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
        .config("spark.sql.catalog.rest.s3.access-key-id", ak)
        .config("spark.sql.catalog.rest.s3.secret-access-key", sk)
        .config("spark.sql.catalog.rest.warehouse", "s3://lake/warehouse")
        .getOrCreate()
    )


def normalize_trigger(x: str) -> str:
    """
    Accepts: '10', '10s', '10 sec', '10 seconds', 'PT10S'
    Returns Spark-friendly string: '10 seconds'
    """
    if not x:
        return "15 seconds"
    s = str(x).strip().lower()

    # ISO-8601 PTxxS
    if s.startswith("pt") and s.endswith("s"):
        n = re.sub(r"[^0-9]", "", s)
        return f"{n} seconds" if n else "15 seconds"

    # Plain number -> seconds
    if re.fullmatch(r"[0-9]+", s):
        return f"{s} seconds"

    # 10s -> 10 seconds
    m = re.fullmatch(r"([0-9]+)\s*s", s)
    if m:
        return f"{m.group(1)} seconds"

    # 10 sec / 10 second / 10 seconds
    if re.fullmatch(r"[0-9]+\s*(sec|secs|second|seconds)", s):
        return re.sub(r"\bsecs?\b", "seconds", s)

    # Fallback
    return s


def main():
    ap = argparse.ArgumentParser()
    # purely for app name/logs clarity
    ap.add_argument("--source", default="avito", help="logical source name for appName/logs (e.g., avito, mubawab)")
    ap.add_argument("--rest-uri", default="http://iceberg-rest:8181")
    ap.add_argument("--s3-endpoint", default="http://minio:9000")
    ap.add_argument("--s3-access-key", default="admin")
    ap.add_argument("--s3-secret-key", default="admin123")
    ap.add_argument("--kafka-bootstrap", default="kafka:9092")
    # defaults kept for Avito (backward compatible)
    ap.add_argument("--topic", default="realestate.avito.raw")
    ap.add_argument("--table", default="rest.raw.avito")
    ap.add_argument("--checkpoint", default="s3://lake/checkpoints/avito_raw")
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--trigger", default="15 seconds")
    args = ap.parse_args()

    app_name = f"{args.source}-kafka-to-iceberg"
    trigger_str = normalize_trigger(args.trigger)

    spark = build_spark(args.rest_uri, args.s3_endpoint, args.s3_access_key, args.s3_secret_key, app_name)

    # namespace safety
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.raw")

    kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
        .select(
            F.col("key").cast("string").alias("k"),
            F.col("value").cast("string").alias("v"),
            F.current_timestamp().alias("ts")
        )
    )

    out = kafka.select(
        F.coalesce(F.get_json_object("v", "$.id"), F.col("k")).alias("id"),
        F.col("v").alias("payload"),
        F.col("ts").alias("ingest_ts")
    )

    q = (
        out.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", args.table)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=trigger_str)
        .start()
    )
    print(
        f"✅ [{app_name}] Streaming started → {args.table} "
        f"(topic={args.topic}, checkpoint={args.checkpoint}, trigger={trigger_str})"
    )
    q.awaitTermination()


if __name__ == "__main__":
    main()

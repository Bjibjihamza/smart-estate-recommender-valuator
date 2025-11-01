#!/usr/bin/env python3
# Kafka -> Iceberg (REST) streaming sink vers rest.raw.avito (id, payload, ingest_ts)

import argparse
from pyspark.sql import SparkSession, functions as F

def build_spark(rest_uri: str, s3_endpoint: str, ak: str, sk: str) -> SparkSession:
    return (
        SparkSession.builder.appName("avito-kafka-to-iceberg")
        # Iceberg REST catalog (catalog = rest)
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.catalog-impl","org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.rest.uri", rest_uri)
        .config("spark.sql.defaultCatalog","rest")
        # S3/MinIO pour Iceberg
        .config("spark.sql.catalog.rest.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.rest.s3.path-style-access","true")
        .config("spark.sql.catalog.rest.s3.access-key-id", ak)
        .config("spark.sql.catalog.rest.s3.secret-access-key", sk)
        .config("spark.sql.catalog.rest.warehouse","s3://lake/warehouse")
        # (facultatif mais utile si tu utilises s3a ailleurs)
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access","true")
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .getOrCreate()
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rest-uri", default="http://iceberg-rest:8181")
    ap.add_argument("--s3-endpoint", default="http://minio:9000")
    ap.add_argument("--s3-access-key", default="admin")
    ap.add_argument("--s3-secret-key", default="admin123")
    ap.add_argument("--kafka-bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="realestate.avito.raw")
    ap.add_argument("--table", default="rest.raw.avito")          # <- table actuelle
    ap.add_argument("--checkpoint", default="s3://lake/checkpoints/avito_raw")
    ap.add_argument("--starting-offsets", default="latest")       # "earliest" pour backfill initial
    ap.add_argument("--trigger", default="15 seconds")
    args = ap.parse_args()

    spark = build_spark(args.rest_uri, args.s3_endpoint, args.s3_access_key, args.s3_secret_key)

    # Source Kafka (stream)
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

    # Mapping vers le schéma simple: id, payload (JSON brut), ingest_ts
    out = kafka.select(
        F.coalesce(F.get_json_object("v", "$.id"), F.col("k")).alias("id"),
        F.col("v").alias("payload"),
        F.col("ts").alias("ingest_ts")
    )

    # S'assurer que le namespace existe (idempotent)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.raw")

    # Écriture streaming vers Iceberg
    q = (
        out.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", args.table)                 # chemin Iceberg: rest.raw.avito
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )
    print(f"Streaming started → {args.table} (checkpoint: {args.checkpoint})")
    q.awaitTermination()

if __name__ == "__main__":
    main()

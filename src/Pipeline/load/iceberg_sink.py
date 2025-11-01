#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
import argparse

def build_spark(rest_uri, s3_endpoint, ak, sk):
    return (
        SparkSession.builder.appName("avito-kafka-to-iceberg")
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.catalog-impl","org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.rest.uri",rest_uri)
        .config("spark.sql.defaultCatalog","rest")
        .config("spark.sql.catalog.rest.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.rest.s3.path-style-access","true")
        .config("spark.sql.catalog.rest.s3.access-key-id", ak)
        .config("spark.sql.catalog.rest.s3.secret-access-key", sk)
        .config("spark.sql.catalog.rest.warehouse","s3://lake/warehouse")
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
    ap.add_argument("--table", default="rest.raw.avito")
    ap.add_argument("--checkpoint", default="s3://lake/checkpoints/avito_raw")
    ap.add_argument("--starting-offsets", default="latest")
    ap.add_argument("--trigger", default="15 seconds")
    args = ap.parse_args()

    spark = build_spark(args.rest_uri, args.s3_endpoint, args.s3_access_key, args.s3_secret_key)

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

    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.raw")

    q = (
        out.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("path", args.table)
        .option("checkpointLocation", args.checkpoint)
        .trigger(processingTime=args.trigger)
        .start()
    )
    print(f"Streaming started → {args.table} (checkpoint: {args.checkpoint})")
    q.awaitTermination()

if __name__ == "__main__":
    main()

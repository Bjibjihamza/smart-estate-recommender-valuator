#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
from datetime import datetime
from unicodedata import normalize
from functools import reduce
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

from Pipeline.load.silver_loader import write_to_silver

# ----------------------------
# Spark / Iceberg config
# ----------------------------
def build_spark(rest_uri: str, s3_endpoint: str, ak: str, sk: str, app_name="avito-raw-to-silver-modular") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.defaultCatalog", "rest")
        .config("spark.sql.catalog.rest.uri", rest_uri)
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.warehouse", "s3://lake/warehouse")
        .config("spark.sql.catalog.rest.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
        .config("spark.sql.catalog.rest.s3.access-key-id", ak)
        .config("spark.sql.catalog.rest.s3.secret-access-key", sk)
        .getOrCreate()
    )

# ----------------------------
# Helpers
# ----------------------------
def _parse_iso8601(ts: str):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def to_snake_ascii(name: str) -> str:
    ascii_name = normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    ascii_name = ascii_name.lower()
    ascii_name = re.sub(r"[^a-z0-9]+", "_", ascii_name)
    ascii_name = re.sub(r"_+", "_", ascii_name).strip("_")
    return ascii_name

def clean_equipments(eqs):
    if eqs is None:
        return []
    out = []
    for item in eqs:
        s = "" if item is None else str(item)
        if re.match(r"^\d+$", s):
            continue
        if re.search(r"(mois|an)", s, flags=re.IGNORECASE):
            continue
        out.append(s)
    return out

# ----------------------------
# Transform
# ----------------------------
def transform_raw_to_silver(
    spark,
    catalog: str,
    since: Optional[str] = None,
    until: Optional[str] = None,
    fallback_window_mins: int = 30,
):
    raw_table = f"{catalog}.raw.avito"

    # 1) Window filter on ingest_ts
    df = spark.table(raw_table)
    conds = [F.col("payload").isNotNull(), F.col("ingest_ts").isNotNull()]
    if since:
        _ = _parse_iso8601(since)
        conds.append(F.col("ingest_ts") >= F.to_timestamp(F.lit(since)))
    if until:
        _ = _parse_iso8601(until)
        conds.append(F.col("ingest_ts") < F.to_timestamp(F.lit(until)))
    if not since and not until:
        conds.append(F.col("ingest_ts") >= F.expr(f"timestampadd(MINUTE, -{fallback_window_mins}, current_timestamp())"))

    condition = reduce(lambda a, b: a & b, conds)
    df = df.where(condition)

    # 2) Parse payload JSON
    payload_schema = T.StructType([
        T.StructField("id", T.StringType()),
        T.StructField("url", T.StringType()),
        T.StructField("error", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("price_text", T.StringType()),
        T.StructField("breadcrumbs", T.StringType()),
        T.StructField("category", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("attributes", T.StringType()),
        T.StructField("equipments", T.StringType()),
        T.StructField("seller_name", T.StringType()),
        T.StructField("seller_type", T.StringType()),
        T.StructField("published_date", T.StringType()),
        T.StructField("image_urls", T.StringType()),
    ])

    p = F.from_json(F.col("payload"), payload_schema).alias("p")
    parsed = df.select("ingest_ts", p).where(F.col("p").isNotNull())

    attrs_map = F.from_json(F.col("p.attributes"), T.MapType(T.StringType(), T.StringType()))
    price_value = F.regexp_replace(F.col("p.price_text"), r"[^0-9]", "").cast("double")
    image_urls_arr = F.transform(F.split(F.col("p.image_urls"), r"\s*\|\s*"), lambda x: F.trim(x))
    equipments_arr = F.transform(F.split(F.col("p.equipments"), r"\s*;\s*"), lambda x: F.trim(x))

    silver = parsed.select(
        F.col("p.id").alias("id"),
        F.col("p.url").alias("url"),
        F.col("p.title").alias("title"),
        price_value.alias("price"),
        F.col("p.description").alias("description"),
        F.col("p.seller_name").alias("seller_name"),
        F.lower(F.col("p.seller_type")).alias("seller_type"),
        image_urls_arr.alias("image_urls"),
        equipments_arr.alias("equipments"),
        F.col("ingest_ts").cast("timestamp").alias("ingest_ts"),
        F.col("p.category").alias("category"),
        F.col("p.breadcrumbs").alias("breadcrumbs"),
        F.col("p.published_date").alias("published_date_text"),
        attrs_map.alias("attributes_map"),
    )

    # Offer / property_type
    silver = (
        silver
        .withColumn(
            "offer",
            F.when(F.col("category").contains("à louer"), "rent")
             .when(F.col("category").contains("à vendre"), "sale")
             .otherwise(None)
        )
        .withColumn(
            "property_type",
            F.when(
                F.col("category").contains("à louer") | F.col("category").contains("à vendre"),
                F.split(F.col("category"), ",")[0]
            ).otherwise(None)
        )
        .drop("category")
    )

    # Breadcrumbs → city / neighborhood / site
    split_bc = F.split(F.col("breadcrumbs"), " > ")
    silver = (
        silver
        .withColumn("city", split_bc.getItem(2))
        .withColumn("neighborhood", split_bc.getItem(3))
        .withColumn("site", F.lit("avito"))
        .drop("breadcrumbs")
    )

    # published_date text → timestamp
    silver = silver.withColumn(
        "published_date",
        F.to_timestamp("published_date_text", "yyyy-MM-dd HH:mm:ss")
    ).drop("published_date_text")

    # Clean equipments
    clean_udf = F.udf(clean_equipments, T.ArrayType(T.StringType()))
    silver = silver.withColumn("equipments", clean_udf(F.col("equipments")))

    # Expand attributes_map → known columns
    attr_keys = [
        "Surface habitable", "Caution", "Zoning", "Standing", "Surface totale", "Étage",
        "Âge du bien", "Salle de bain", "Nombre de pièces", "Chambres",
        "Frais de syndic / mois", "Condition", "Nombre d'étage", "Disponibilité",
        "Salons", "Type d'appartement"
    ]
    for k in attr_keys:
        silver = silver.withColumn(k, F.col("attributes_map")[F.lit(k)])
    silver = silver.drop("attributes_map")

    # NULL→0.0 for price
    silver = silver.fillna({"price": 0.0})

    # Rename FR → EN
    rename_map = {
        "Surface habitable": "living_area",
        "Surface totale": "total_area",
        "Étage": "floor",
        "Âge du bien": "property_age",
        "Salle de bain": "bathrooms",
        "Nombre de pièces": "rooms",
        "Chambres": "bedrooms",
        "Frais de syndic / mois": "hoa_fee_per_month",
        "Condition": "condition",
        "Nombre d'étage": "floors",
        "Disponibilité": "availability",
        "Salons": "living_rooms",
        "Type d'appartement": "apartment_type",
        "Zoning": "zoning",
        "Standing": "standing",
        "Caution": "deposit",
    }
    for old, new in rename_map.items():
        if old in silver.columns:
            silver = silver.withColumnRenamed(old, new)

    # Global sanitize
    cols_before = silver.columns
    sanitized = [to_snake_ascii(c) for c in cols_before]
    seen = {}
    uniq = []
    for s in sanitized:
        if s not in seen:
            seen[s] = 1
            uniq.append(s)
        else:
            seen[s] += 1
            uniq.append(f"{s}_{seen[s]}")
    for old, new in zip(cols_before, uniq):
        if old != new:
            silver = silver.withColumnRenamed(old, new)

    # ---- Dedup INSIDE THE BATCH by (site,id): keep newest ingest_ts ----
    w = Window.partitionBy("site", "id").orderBy(F.col("ingest_ts").desc_nulls_last())
    silver = silver.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1).drop("rn")

    return silver

def align_to_table_schema(spark, df, target_fqn: str):
    target_schema = spark.table(target_fqn).schema
    df_cols = set(df.columns)
    for field in target_schema:
        if field.name not in df_cols:
            df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
    df = df.select(*[f.name for f in target_schema])
    return df

def main():
    ap = argparse.ArgumentParser(description="Transform Avito RAW → SILVER (last 30 min + dedup + anti-join)")
    ap.add_argument("--catalog", default="rest")
    ap.add_argument("--since", default=None, help="ISO8601 start (inclusive)")
    ap.add_argument("--until", default=None, help="ISO8601 end (exclusive)")
    ap.add_argument("--mode", default="append", help="append | overwrite | dynamic_overwrite")
    ap.add_argument("--fallback-window-mins", type=int, default=30)
    ap.add_argument("--rest-uri", default="http://iceberg-rest:8181")
    ap.add_argument("--s3-endpoint", default="http://minio:9000")
    ap.add_argument("--s3-access-key", default="admin")
    ap.add_argument("--s3-secret-key", default="admin123")
    args = ap.parse_args()

    spark = build_spark(args.rest_uri, args.s3_endpoint, args.s3_access_key, args.s3_secret_key)
    try:
        batch_df = transform_raw_to_silver(
            spark,
            catalog=args.catalog,
            since=args.since,
            until=args.until,
            fallback_window_mins=args.fallback_window_mins,
        )

        target = f"{args.catalog}.silver.avito"

        # ---- ANTI-JOIN vs existing Silver: only keep (site,id) not present ----
        try:
            silver_ids = spark.table(target).select("site", "id").distinct()
            # Left-anti on (site,id)
            batch_df = (
                batch_df.alias("b")
                .join(silver_ids.alias("t"), on=(F.col("b.site")==F.col("t.site")) & (F.col("b.id")==F.col("t.id")), how="left_anti")
            )
        except Exception:
            # table might not exist yet -> skip anti-join
            pass

        # Align schema and write
        batch_df = align_to_table_schema(spark, batch_df, target)
        write_to_silver(spark, batch_df, catalog=args.catalog, table="silver.avito", mode=args.mode)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()

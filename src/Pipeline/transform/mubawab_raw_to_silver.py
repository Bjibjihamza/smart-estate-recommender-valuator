#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, lit, when, split, from_json, row_number, lower
)
from pyspark.sql.types import *

UNIFIED_COLS = [
    "id","url","error","ingest_ts","site","offre","price","title","seller",
    "published_date","city","neighborhood","property_type","images","equipments",
    "description_text","offre_match","surface_habitable","caution","zoning",
    "type_d_appartement","standing","surface_totale","etage","age_du_bien",
    "nombre_de_pieces","chambres","salle_de_bain","frais_de_syndic_mois",
    "condition","nombre_d_etage","disponibilite","salons",
    "features_amenities_json","type_de_bien","surface_de_la_parcelle",
    "type_du_sol","etage_du_bien","annees","orientation","etat"
]

def build_spark():
    return (
        SparkSession.builder.appName("mubawab_raw_to_silver")
        .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest","org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.type","rest")
        .config("spark.sql.catalog.rest.uri","http://iceberg-rest:8181")
        .config("spark.sql.catalog.rest.warehouse","s3://lake/warehouse")
        .config("spark.sql.catalog.rest.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint","http://minio:9000")
        .config("spark.sql.catalog.rest.s3.path-style-access","true")
        .config("spark.sql.catalog.rest.s3.access-key-id","admin")
        .config("spark.sql.catalog.rest.s3.secret-access-key","admin123")
        .config("spark.sql.catalog.rest.s3.region","us-east-1")
        .getOrCreate()
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="rest")
    ap.add_argument("--mode", choices=["append"], default="append")
    ap.add_argument("--fallback-window-mins", type=int, default=35)
    args = ap.parse_args()

    spark = build_spark()

    raw = spark.table(f"{args.catalog}.raw.mubawab")

    payload_schema = StructType([
        StructField("id", StringType()),
        StructField("url", StringType()),
        StructField("error", StringType()),
        StructField("listing_type", StringType()),
        StructField("title", StringType()),
        StructField("price", DoubleType()),
        StructField("location_text", StringType()),
        StructField("features_amenities_json", StringType()),
        StructField("description_text", StringType()),
        StructField("features_main_json", StringType()),
        StructField("gallery_urls", StringType()),
        StructField("agency_name", StringType()),
        StructField("agency_url", StringType()),
        StructField("published_date", StringType()),
    ])

    df = (
        raw
        .withColumn("j", from_json(col("payload"), payload_schema))
        .select(col("ingest_ts"), col("j.*"))
    )

    # Dedup latest per id
    w = Window.partitionBy("id").orderBy(col("ingest_ts").desc())
    df = df.withColumn("rn", row_number().over(w)).filter(col("rn")==1).drop("rn")

    # URL valid
    df = df.filter((col("url").isNotNull()) & (trim(col("url"))!=""))

    # Price: keep >0 else NULL
    df = df.withColumn("price", when((col("price")<=0) | col("price").isNull(), lit(None)).otherwise(col("price")))

    # seller from agency_name (lower/trim), drop agency fields (you asked to drop agency_url)
    df = (
        df.withColumn(
            "seller",
            when(
                (col("agency_name").isNull()) | (trim(col("agency_name"))=="") |
                (lower(trim(col("agency_name"))).isin("nan","null","unknown")),
                "unknown"
            ).otherwise(lower(trim(col("agency_name"))))
        )
        .drop("agency_name","agency_url")
    )

    # images from gallery_urls JSON array
    df = df.withColumn("images", from_json(col("gallery_urls"), ArrayType(StringType()))).drop("gallery_urls")

    # equipments: parse, keep as-is (you already normalized earlier in notebooks; here we keep extraction only)
    # Just project features_amenities_json through; you can transform later if needed
    df = df.withColumn("equipments", lit(None).cast("array<string>"))  # keep simple (extraction-only step)
    # If you want the array now: from_json(features_amenities_json, array<string>)

    # offre
    df = df.withColumnRenamed("listing_type","offre")

    # city / neighborhood from "A à B"
    parts = split(col("location_text"), " à ")
    df = (
        df.withColumn("neighborhood", trim(parts.getItem(0)))
          .withColumn("city", trim(parts.getItem(1)))
    )
    df = df.withColumn("city", when(col("city").isNull(), col("neighborhood")).otherwise(col("city"))).drop("location_text")

    # site
    df = df.withColumn("site", lit("mubawab"))

    # property_type from features_main_json["Type de bien"] and explode ALL keys into columns
    feat_map = from_json(col("features_main_json"), MapType(StringType(), StringType()))
    df = df.withColumn("features_map", feat_map).withColumn("property_type", col("features_map")["Type de bien"])

    keys = [r["k"] for r in df.selectExpr("explode(map_keys(features_map)) as k").distinct().collect() if r["k"]]
    import unicodedata, re
    def sanitize(name:str)->str:
        name = unicodedata.normalize("NFKD", name).encode("ascii","ignore").decode("ascii")
        name = re.sub(r"[^0-9a-zA-Z]+","_",name).strip("_")
        return name.lower()
    for k in keys:
        df = df.withColumn(sanitize(k), trim(col("features_map")[k]))
    df = df.drop("features_map","features_main_json")  # drop raw JSON as requested

    # Final projection to unified schema (no breadcrumbs/attributes, description already description_text)
    final = df.select(
        "id","url","error","ingest_ts","site","offre","price","title","seller",
        "published_date","city","neighborhood","property_type","images",
        "equipments","description_text",
        # Avito-only fields (NULLs here)
        lit(None).cast("boolean").alias("offre_match"),
        lit(None).cast("string").alias("surface_habitable"),
        lit(None).cast("string").alias("caution"),
        lit(None).cast("string").alias("zoning"),
        lit(None).cast("string").alias("type_d_appartement"),
        lit(None).cast("string").alias("standing"),
        lit(None).cast("string").alias("surface_totale"),
        lit(None).cast("string").alias("etage"),
        lit(None).cast("string").alias("age_du_bien"),
        lit(None).cast("string").alias("nombre_de_pieces"),
        lit(None).cast("string").alias("chambres"),
        lit(None).cast("string").alias("salle_de_bain"),
        lit(None).cast("string").alias("frais_de_syndic_mois"),
        lit(None).cast("string").alias("condition"),
        lit(None).cast("string").alias("nombre_d_etage"),
        lit(None).cast("string").alias("disponibilite"),
        lit(None).cast("string").alias("salons"),
        # Mubawab specifics (keep these if present)
        col("features_amenities_json"),
        col(sanitize("Type de bien")).alias("type_de_bien") if "type_de_bien" in df.columns else lit(None).cast("string").alias("type_de_bien"),
        col(sanitize("Surface de la parcelle")).alias("surface_de_la_parcelle") if "surface_de_la_parcelle" in df.columns else lit(None).cast("string").alias("surface_de_la_parcelle"),
        col(sanitize("Type du sol")).alias("type_du_sol") if "type_du_sol" in df.columns else lit(None).cast("string").alias("type_du_sol"),
        col(sanitize("Étage du bien")).alias("etage_du_bien") if "etage_du_bien" in df.columns else lit(None).cast("string").alias("etage_du_bien"),
        col(sanitize("Années")).alias("annees") if "annees" in df.columns else lit(None).cast("string").alias("annees"),
        col(sanitize("Orientation")).alias("orientation") if "orientation" in df.columns else lit(None).cast("string").alias("orientation"),
        col(sanitize("Etat")).alias("etat") if "etat" in df.columns else lit(None).cast("string").alias("etat"),
    )

    if args.fallback_window_mins and args.fallback_window_mins > 0:
        final = final.where(col("ingest_ts") >= expr(f"timestampadd(MINUTE, -{args.fallback_window_mins}, current_timestamp())"))

    final.select(*UNIFIED_COLS).writeTo(f"{args.catalog}.silver.listings_all").append()

    spark.stop()

if __name__ == "__main__":
    main()

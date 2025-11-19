#!/usr/bin/env python3
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, lit, when, length, regexp_replace, from_json, split, size,
    expr, array_position, element_at, coalesce, lower, row_number
)
from pyspark.sql.types import *


UNIFIED_COLS = [
    "id","url","error","ingest_ts","site",
    "offre","price","title","seller","published_date",
    "city","neighborhood","property_type",
    "images","equipments","description_text",

    # AVITO-specific
    "offre_match",
    "surface_habitable",
    "caution",
    "zoning",
    "type_d_appartement",
    "standing",
    "surface_totale",
    "etage",
    "age_du_bien",
    "nombre_de_pieces",
    "chambres",
    "salle_de_bain",
    "frais_de_syndic_mois",
    "condition",
    "nombre_d_etage",
    "disponibilite",
    "salons",

    # MUBAWAB-specific (NULL for Avito)
    "features_amenities_json",
    "type_de_terrain",
    "type_de_bien",
    "surface",
    "statut_du_terrain",
    "surface_de_la_parcelle",
    "type_du_sol",
    "etage_du_bien",
    "detail_1",
    "annees",
    "constructibilite",
    "salles_de_bain",
    "livraison",
    "pieces",
    "orientation",
    "etat",
    "nombre_d_etages",
]




def build_spark():
    return (
        SparkSession.builder.appName("avito_raw_to_silver")
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

    # ---- Read RAW
    raw = spark.table(f"{args.catalog}.raw.avito")

    # ---- Parse payload
    payload_schema = StructType([
        StructField("id", StringType()),
        StructField("url", StringType()),
        StructField("error", StringType()),
        StructField("title", StringType()),
        StructField("price_text", StringType()),
        StructField("breadcrumbs", StringType()),
        StructField("category", StringType()),
        StructField("description", StringType()),
        StructField("attributes", StringType()),
        StructField("equipments", StringType()),
        StructField("seller_name", StringType()),
        StructField("seller_type", StringType()),
        StructField("published_date", StringType()),
        StructField("image_urls", StringType()),
        StructField("listing_type", StringType()),
    ])

    df = (
        raw
        .withColumn("json", from_json(col("payload"), payload_schema))
        .select(col("ingest_ts"), col("json.*"))
    )

    # ---- Dedup by latest ingest_ts per id
    w = Window.partitionBy("id").orderBy(col("ingest_ts").desc())
    df = df.withColumn("rn", row_number().over(w)).filter(col("rn")==1).drop("rn")

    # ---- Keep valid URL
    df = df.filter((col("url").isNotNull()) & (trim(col("url"))!=""))

    # ---- price
    df = df.withColumn(
        "price",
        when((col("price_text").isNull()) | (length(col("price_text"))==0), lit(None).cast("double"))
        .otherwise(
            regexp_replace(
              regexp_replace(
                regexp_replace(col("price_text"), u"\u00A0",""),
                r"[ ,]",""
              ),
              r"[^0-9.]", ""
            ).cast("double")
        )
    ).drop("price_text")

    # ---- seller
    df = (
        df.withColumn(
            "seller",
            when(
                (col("seller_name").isNull()) | (trim(col("seller_name"))=="") |
                (lower(trim(col("seller_name"))).isin("null","none","unknown")),
                "unknown"
            ).otherwise(lower(trim(col("seller_name"))))
        ).drop("seller_name","seller_type")
    )

    # ---- images
    df = df.withColumn(
        "images",
        expr("FILTER(TRANSFORM(SPLIT(image_urls, '\\\\s*\\\\|\\\\s*'), x -> TRIM(x)), x -> x <> '')")
    ).drop("image_urls")

    # ---- equipments (array)
    df = df.withColumn(
        "equipments",
        expr("""
          CASE WHEN equipments IS NULL THEN array()
          ELSE array_distinct(
            FILTER(
              TRANSFORM(SPLIT(equipments, '\\s*;\\s*'), x -> trim(x)),
              x -> x <> '' AND x RLIKE '.*[A-Za-zÀ-ÿ].*'
                   AND NOT (x RLIKE '.*[0-9].*')
                   AND NOT (lower(x) RLIKE '.*moi.*')
                   AND NOT (lower(x) RLIKE '^(aucune|studio)$')
            )
          )
          END
        """)
    )

    # ---- offre
    df = df.withColumnRenamed("listing_type","offre")

    # ---- city/neighborhood from breadcrumbs
    parts = split(coalesce(col("breadcrumbs"), lit("")), " > ")
    idx = array_position(parts, "Tout le Maroc")
    df = (
        df
        .withColumn(
            "city",
            when((idx>lit(0)) & (size(parts) >= (idx+lit(1))),
                 trim(element_at(parts, (idx+lit(1)).cast("int"))))
        )
        .withColumn(
            "neighborhood_raw",
            when((idx>lit(0)) & (size(parts) >= (idx+lit(2))),
                 trim(element_at(parts, (idx+lit(2)).cast("int"))))
        )
    )
    bad_neigh = ["Avito Immobilier","أفيتو للعقار","Toute la ville","Autre secteur"]
    df = df.withColumn("neighborhood",
                       when(col("neighborhood_raw").isin(bad_neigh), None)
                       .otherwise(col("neighborhood_raw"))).drop("neighborhood_raw")

    # ---- site
    df = df.withColumn("site", lit("avito"))

    # ---- property_type + listing phrase from category
    parts = split(coalesce(col("category"), lit("")), r"\s*,\s*")
    df = (
        df.withColumn("property_type", when(size(parts)>=1, trim(parts.getItem(0))))
          .withColumn("listing_phrase", when(size(parts)>=2, trim(parts.getItem(1))))
    )

    # expected listing
    df = (
        df.withColumn(
            "listing_expected",
            when(col("listing_phrase").isin("à louer","a louer"), lit("location"))
            .when(col("listing_phrase").isin("à vendre","a vendre"), lit("vente"))
        )
        .withColumn("offre_match", when(col("listing_expected")==col("offre"), lit(True)).otherwise(lit(False)))
        .drop("category","listing_phrase","listing_expected")
    )

    # ---- attributes → dynamic columns (keep exact values)
    attr_map = from_json(col("attributes"), MapType(StringType(), StringType()))
    df = df.withColumn("attr_map", attr_map)
    keys = [r["k"] for r in df.selectExpr("explode(map_keys(attr_map)) as k").distinct().collect() if r["k"]]

    import unicodedata, re
    def sanitize(name:str)->str:
        name = unicodedata.normalize("NFKD", name).encode("ascii","ignore").decode("ascii")
        name = re.sub(r"[^0-9a-zA-Z]+","_",name).strip("_")
        return name.lower()

    for k in keys:
        df = df.withColumn(sanitize(k), trim(col("attr_map")[k]))
    df = df.drop("attr_map")  # keep raw 'attributes' to drop later per your rule

    final = df.select(
        col("id"), col("url"), col("error"), col("ingest_ts"), col("site"),
        col("offre"), col("price"), col("title"), col("seller"),
        col("published_date"), col("city"), col("neighborhood"),
        col("property_type"), col("images"), col("equipments"),
        col("description").alias("description_text"),

        # AVITO-specific stuff
        col("offre_match"),
        col("surface_habitable") if "surface_habitable" in df.columns else lit(None).cast("string").alias("surface_habitable"),
        col("caution") if "caution" in df.columns else lit(None).cast("string").alias("caution"),
        col("zoning") if "zoning" in df.columns else lit(None).cast("string").alias("zoning"),
        col("type_d_appartement") if "type_d_appartement" in df.columns else lit(None).cast("string").alias("type_d_appartement"),
        col("standing") if "standing" in df.columns else lit(None).cast("string").alias("standing"),
        col("surface_totale") if "surface_totale" in df.columns else lit(None).cast("string").alias("surface_totale"),
        col("etage") if "etage" in df.columns else lit(None).cast("string").alias("etage"),
        col("age_du_bien") if "age_du_bien" in df.columns else lit(None).cast("string").alias("age_du_bien"),
        col("nombre_de_pieces") if "nombre_de_pieces" in df.columns else lit(None).cast("string").alias("nombre_de_pieces"),
        col("chambres") if "chambres" in df.columns else lit(None).cast("string").alias("chambres"),
        col("salle_de_bain") if "salle_de_bain" in df.columns else lit(None).cast("string").alias("salle_de_bain"),
        col("frais_de_syndic_mois") if "frais_de_syndic_mois" in df.columns else lit(None).cast("string").alias("frais_de_syndic_mois"),
        col("condition") if "condition" in df.columns else lit(None).cast("string").alias("condition"),
        col("nombre_d_etage") if "nombre_d_etage" in df.columns else lit(None).cast("string").alias("nombre_d_etage"),
        col("disponibilite") if "disponibilite" in df.columns else lit(None).cast("string").alias("disponibilite"),
        col("salons") if "salons" in df.columns else lit(None).cast("string").alias("salons"),

        # MUBAWAB-specific – always NULL for Avito
        lit(None).cast("string").alias("features_amenities_json"),
        lit(None).cast("string").alias("type_de_terrain"),
        lit(None).cast("string").alias("type_de_bien"),
        lit(None).cast("string").alias("surface"),          # string to match Mubawab
        lit(None).cast("string").alias("statut_du_terrain"),
        lit(None).cast("string").alias("surface_de_la_parcelle"),
        lit(None).cast("string").alias("type_du_sol"),
        lit(None).cast("string").alias("etage_du_bien"),
        lit(None).cast("string").alias("detail_1"),
        lit(None).cast("string").alias("annees"),
        lit(None).cast("string").alias("constructibilite"),
        lit(None).cast("string").alias("salles_de_bain"),   # <<< IMPORTANT: exists in table
        lit(None).cast("string").alias("livraison"),
        lit(None).cast("string").alias("pieces"),           # <<< IMPORTANT: exists in table
        lit(None).cast("string").alias("orientation"),
        lit(None).cast("string").alias("etat"),
        lit(None).cast("string").alias("nombre_d_etages"),
    )

    # Optional: window filter (if table large)
    if args.fallback_window_mins and args.fallback_window_mins > 0:
        final = final.where(col("ingest_ts") >= expr(f"timestampadd(MINUTE, -{args.fallback_window_mins}, current_timestamp())"))

    # ---- Append into unified table
    final.select(*UNIFIED_COLS).writeTo(f"{args.catalog}.silver.listings_all").append()

    spark.stop()

if __name__ == "__main__":
    main()

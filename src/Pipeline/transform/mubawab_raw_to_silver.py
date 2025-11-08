#!/usr/bin/env python3
import argparse
import unicodedata
import re

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, lit, when, split, from_json, row_number, lower, expr
)
from pyspark.sql.types import *

# Doit matcher exactement la table rest.silver.listings_all
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

    # MUBAWAB-specific
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


def sanitize(name: str) -> str:
    """
    Transforme les labels Mubawab en noms de colonnes Spark :
      "Surface de la parcelle" -> "surface_de_la_parcelle"
      "Étage du bien"          -> "etage_du_bien"
      "Années"                 -> "annees"
      "Salles de bain"         -> "salles_de_bain"
      etc.
    """
    if name is None:
        return ""
    name = unicodedata.normalize("NFKD", name).encode("ascii","ignore").decode("ascii")
    name = re.sub(r"[^0-9a-zA-Z]+","_", name).strip("_")
    return name.lower()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="rest")
    ap.add_argument("--mode", choices=["append"], default="append")
    ap.add_argument("--fallback-window-mins", type=int, default=35)
    args = ap.parse_args()

    spark = build_spark()

    # Bronze/raw table (payload JSON)
    raw = spark.table(f"{args.catalog}.raw.mubawab")

    # Schéma du payload
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

    # Dedup: garder la dernière version par id
    w = Window.partitionBy("id").orderBy(col("ingest_ts").desc())
    df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

    # URL valide
    df = df.filter((col("url").isNotNull()) & (trim(col("url")) != ""))

    # Price > 0 sinon NULL
    df = df.withColumn(
        "price",
        when((col("price") <= 0) | col("price").isNull(), lit(None)).otherwise(col("price"))
    )

    # seller = agency_name normalisé (sinon "unknown")
    df = (
        df.withColumn(
            "seller",
            when(
                (col("agency_name").isNull()) | (trim(col("agency_name"))=="") |
                (lower(trim(col("agency_name"))).isin("nan","null","unknown")),
                "unknown"
            ).otherwise(lower(trim(col("agency_name"))))
        )
        .drop("agency_name", "agency_url")
    )

    # images depuis gallery_urls (JSON array)
    df = df.withColumn("images", from_json(col("gallery_urls"), ArrayType(StringType()))).drop("gallery_urls")

    # equipments : pour l’instant NULL
    df = df.withColumn("equipments", lit(None).cast("array<string>"))

    # offre depuis listing_type
    df = df.withColumnRenamed("listing_type", "offre")

    # city / neighborhood depuis "Quartier à Ville"
    parts = split(col("location_text"), " à ")
    df = (
        df.withColumn("neighborhood", trim(parts.getItem(0)))
          .withColumn("city", trim(parts.getItem(1)))
    )
    df = df.withColumn(
        "city",
        when(col("city").isNull(), col("neighborhood")).otherwise(col("city"))
    ).drop("location_text")

    # site
    df = df.withColumn("site", lit("mubawab"))

    # features_main_json -> map, puis colonnes individuelles
    feat_map = from_json(col("features_main_json"), MapType(StringType(), StringType()))
    df = df.withColumn("features_map", feat_map)

    # property_type (type de bien)
    df = df.withColumn("property_type", col("features_map")["Type de bien"])

    # Générer toutes les colonnes à partir des clés du map
    keys = [
        r["k"]
        for r in df.selectExpr("explode(map_keys(features_map)) as k").distinct().collect()
        if r["k"]
    ]

    for k in keys:
        safe = sanitize(k)
        if safe:  # évite les vides
            df = df.withColumn(safe, trim(col("features_map")[k]))

    # Plus besoin de la map brute
    df = df.drop("features_map", "features_main_json")

    # Helper pour sélectionner une colonne dérivée de features_main_json si elle existe
    def col_or_null(label: str, alias: str):
        safe = sanitize(label)
        return (
            col(safe).alias(alias)
            if safe in df.columns
            else lit(None).cast("string").alias(alias)
        )

    # Construction du DataFrame final aligné au schéma Silver
    final = df.select(
        # Commun
        "id","url","error","ingest_ts","site",
        "offre","price","title","seller","published_date",
        "city","neighborhood","property_type",
        "images","equipments","description_text",

        # AVITO fields -> NULL ici (sauf ceux qu’on choisit d’alimenter plus tard)
        lit(None).cast("boolean").alias("offre_match"),
        lit(None).cast("string").alias("surface_habitable"),
        lit(None).cast("string").alias("caution"),
        lit(None).cast("string").alias("zoning"),
        lit(None).cast("string").alias("type_d_appartement"),
        lit(None).cast("string").alias("standing"),
        lit(None).cast("string").alias("surface_totale"),
        lit(None).cast("string").alias("etage"),
        lit(None).cast("string").alias("age_du_bien"),
        # Ces 3 suivants peuvent aussi venir de features_main_json si tu veux,
        # pour l’instant on les laisse NULL pour Avito, mais:
        # - "Pièces"   -> pieces
        # - "Chambres" -> chambres
        # - "Salles de bain" -> salles_de_bain
        lit(None).cast("string").alias("nombre_de_pieces"),
        lit(None).cast("string").alias("chambres"),
        lit(None).cast("string").alias("salle_de_bain"),
        lit(None).cast("string").alias("frais_de_syndic_mois"),
        lit(None).cast("string").alias("condition"),
        lit(None).cast("string").alias("nombre_d_etage"),
        lit(None).cast("string").alias("disponibilite"),
        lit(None).cast("string").alias("salons"),

        # MUBAWAB-specific (dérivés de features_main_json)
        col("features_amenities_json"),
        col_or_null("Type de terrain", "type_de_terrain"),
        col_or_null("Type de bien", "type_de_bien"),
        col_or_null("Surface", "surface"),
        col_or_null("Statut du terrain", "statut_du_terrain"),
        col_or_null("Surface de la parcelle", "surface_de_la_parcelle"),
        col_or_null("Type du sol", "type_du_sol"),
        col_or_null("Étage du bien", "etage_du_bien"),
        # detail_1 : pour l'instant NULL (réservé si tu veux mapper autre chose après)
        lit(None).cast("string").alias("detail_1"),
        col_or_null("Années", "annees"),
        col_or_null("Constructibilité", "constructibilite"),
        col_or_null("Salles de bain", "salles_de_bain"),
        col_or_null("Livraison", "livraison"),
        col_or_null("Pièces", "pieces"),
        col_or_null("Orientation", "orientation"),
        col_or_null("Etat", "etat"),
        col_or_null("Nombre d'étages", "nombre_d_etages"),
    )

    # Fallback window (optionnel, garde juste les X dernières minutes)
    if args.fallback_window_mins and args.fallback_window_mins > 0:
        final = final.where(
            col("ingest_ts") >= expr(f"timestampadd(MINUTE, -{args.fallback_window_mins}, current_timestamp())")
        )

    # Écriture dans la Silver unifiée
    (
        final.select(*UNIFIED_COLS)
             .writeTo(f"{args.catalog}.silver.listings_all")
             .append()
    )

    spark.stop()


if __name__ == "__main__":
    main()

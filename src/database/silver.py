#!/usr/bin/env python3
"""
Create `rest.silver.listings_all` in Iceberg REST catalog (unified schema).

- description_text only (map Avito.description -> description_text when inserting)
- EXCLUDE breadcrumbs, attributes
- EXCLUDE agency_url
"""

import argparse
from pyspark.sql import SparkSession


def build_spark(rest_uri: str, s3_endpoint: str, ak: str, sk: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("create-silver-listings-all")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "rest")
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.rest.uri", rest_uri)
        .config("spark.sql.catalog.rest.warehouse", "s3://lake/warehouse")
        # IO (MinIO/S3)
        .config("spark.sql.catalog.rest.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.rest.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.rest.s3.path-style-access", "true")
        .config("spark.sql.catalog.rest.s3.access-key-id", ak)
        .config("spark.sql.catalog.rest.s3.secret-access-key", sk)
        .config("spark.sql.catalog.rest.s3.region", "us-east-1")
        # Optional S3A passthrough
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.access.key", ak)
        .config("spark.hadoop.fs.s3a.secret.key", sk)
        .getOrCreate()
    )


def create_namespace_and_table(spark: SparkSession):
    print("[INFO] Ensuring namespace 'rest.silver' exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.silver")

    print("[INFO] Creating unified table 'rest.silver.listings_all'...")
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS rest.silver.listings_all (
        -- Identifiers & meta
        id STRING,
        url STRING,
        error STRING,
        ingest_ts TIMESTAMP,
        site STRING NOT NULL,

        -- Commercial
        offre STRING,
        price DOUBLE,
        title STRING,
        seller STRING,
        published_date STRING,

        -- Geo
        city STRING,
        neighborhood STRING,

        -- Taxonomy
        property_type STRING,

        -- Media & features
        images ARRAY<STRING>,
        equipments ARRAY<STRING>,

        -- Single description column for both sources
        description_text STRING,

        -- ============ AVITO-specific (kept as-is, minus breadcrumbs/attributes) ============
        offre_match BOOLEAN,
        surface_habitable STRING,
        caution STRING,
        zoning STRING,
        type_d_appartement STRING,
        standing STRING,
        surface_totale STRING,
        etage STRING,
        age_du_bien STRING,
        nombre_de_pieces STRING,
        chambres STRING,
        salle_de_bain STRING,
        frais_de_syndic_mois STRING,
        condition STRING,
        nombre_d_etage STRING,
        disponibilite STRING,
        salons STRING,

        -- ============ MUBAWAB-specific (covers full updated schema) ============
        features_amenities_json STRING,
        type_de_terrain STRING,
        type_de_bien STRING,
        surface STRING,
        statut_du_terrain STRING,
        surface_de_la_parcelle STRING,
        type_du_sol STRING,
        etage_du_bien STRING,
        detail_1 STRING,
        annees STRING,
        constructibilite STRING,
        salles_de_bain STRING,
        livraison STRING,
        pieces STRING,
        orientation STRING,
        etat STRING,
        nombre_d_etages STRING
    )
    USING iceberg
    PARTITIONED BY (days(ingest_ts))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.distribution-mode' = 'none'
    )
    """
    spark.sql(create_table_sql)

    print("[SUCCESS] Table ready: rest.silver.listings_all")
    print("\n[INFO] SHOW TABLES in rest.silver")
    spark.sql("SHOW TABLES IN rest.silver").show(truncate=False)

    print("\n[INFO] DESCRIBE rest.silver.listings_all")
    spark.sql("DESCRIBE rest.silver.listings_all").show(truncate=False)


def main():
    ap = argparse.ArgumentParser(description="Create unified Silver listings_all table in Iceberg")
    ap.add_argument("--rest-uri", default="http://iceberg-rest:8181", help="Iceberg REST catalog URI")
    ap.add_argument("--s3-endpoint", default="http://minio:9000", help="S3/MinIO endpoint")
    ap.add_argument("--s3-access-key", default="admin", help="S3 access key")
    ap.add_argument("--s3-secret-key", default="admin123", help="S3 secret key")
    args = ap.parse_args()

    print("=" * 60)
    print("Creating Silver Table: rest.silver.listings_all")
    print("=" * 60)

    spark = build_spark(args.rest_uri, args.s3_endpoint, args.s3_access_key, args.s3_secret_key)
    try:
        create_namespace_and_table(spark)
    finally:
        spark.stop()
        print("\n[INFO] Spark session closed.")


if __name__ == "__main__":
    main()

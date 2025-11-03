# %%
try:
    spark.stop()
except Exception:
    pass

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Iceberg via REST")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "rest")
    .config("spark.sql.catalog.local.uri", "http://iceberg-rest:8181")
    .config("spark.sql.catalog.local.warehouse", "s3://lake/warehouse")
    .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.local.s3.path-style-access", "true")
    .config("spark.sql.catalog.local.s3.access-key-id", "admin")
    .config("spark.sql.catalog.local.s3.secret-access-key", "admin123")
    .config("spark.sql.catalog.local.s3.region", "us-east-1")
    .getOrCreate()
)

spark

# %%
spark.sql("SHOW NAMESPACES IN local").show(truncate=False)

# %%
spark.sql("SHOW TABLES IN local.raw").show(truncate=False)

# %%
tbl = "local.raw.avito"
spark.sql(f"SELECT COUNT(*) AS rows FROM {tbl}").show()

# %%
import os

# Define the export path relative to the current directory of the Jupyter notebook
export_path = os.path.join(os.getcwd(), "exported_avito_table")

# Export the Iceberg table to Parquet format in the current directory
spark.read.format("iceberg").load("local.raw.avito") \
    .write.format("parquet").save(export_path)

# Confirm the files were saved by checking the directory content
print(f"Table exported to: {export_path}")

# %%
# Load RAW table
raw_df = spark.table("local.raw.avito")   # or local.raw.sarouty

# %%
# Créer un DataFrame "Silver" minimal
silver_df = raw_df.select("id").distinct()

# Afficher quelques lignes
silver_df.show(5, truncate=False)

print("✅ Silver dataset initialized with only 'id' column.")
print("Total IDs:", silver_df.count())

# %%
from pyspark.sql import functions as F, types as T

# 1) Définir le schéma du JSON dans "payload"
payload_schema = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("url", T.StringType()),
    T.StructField("error", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("price_text", T.StringType()),
    T.StructField("breadcrumbs", T.StringType()),
    T.StructField("category", T.StringType()),
    T.StructField("description", T.StringType()),
    T.StructField("attributes", T.StringType()),  # JSON imbriqué sous forme de string
    T.StructField("equipments", T.StringType()),
    T.StructField("seller_name", T.StringType()),
    T.StructField("seller_type", T.StringType()),
    T.StructField("published_date", T.StringType()),
    T.StructField("image_urls", T.StringType()),
])

# 2) Parser le JSON depuis la colonne string "payload"
parsed = (raw_df
    .select(
        *[c for c in raw_df.columns if c != "payload"],  # ex: garder ingest_ts s'il existe
        F.from_json(F.col("payload"), payload_schema).alias("p")
    )
    .filter(F.col("p").isNotNull())  # ignorer les lignes avec JSON invalide
)

# 3) Parser le JSON imbriqué "attributes" -> Map<String,String>
attrs_map = F.from_json(F.col("p.attributes"), T.MapType(T.StringType(), T.StringType()))

# 4) Nettoyages utiles:
# - price_value (MAD) à partir de "price_text" (ex: "6 000 DH" -> 6000.0)
price_value = F.regexp_replace(F.col("p.price_text"), r"[^0-9]", "").cast("double")

# - image_urls -> array<string> en splittant sur " | " et trim de chaque url
image_urls_arr = F.transform(
    F.split(F.col("p.image_urls"), r"\s*\|\s*"),
    lambda x: F.trim(x)
)

# - equipments -> array<string> en splittant sur ";"
equipments_arr = F.transform(
    F.split(F.col("p.equipments"), r"\s*;\s*"),
    lambda x: F.trim(x)
)

# 5) Construire le DataFrame silver (colonnes à plat)
silver_df = parsed.select(
    F.col("p.id").alias("id"),
    F.col("p.url").alias("url"),
    F.col("p.title").alias("title"),
    F.col("p.price_text").alias("price_text"),
    price_value.alias("price_value_mad"),
    F.col("p.category").alias("category"),
    F.col("p.breadcrumbs").alias("breadcrumbs"),
    F.col("p.description").alias("description"),
    F.col("p.seller_name").alias("seller_name"),
    F.col("p.seller_type").alias("seller_type"),
    F.col("p.published_date").alias("published_date_text"),
    image_urls_arr.alias("image_urls"),
    equipments_arr.alias("equipments"),
    attrs_map.alias("attributes_map"),
    # garder le timestamp d'ingestion s'il est présent dans ton raw_df
    *([F.col("ingest_ts")] if "ingest_ts" in raw_df.columns else [])
)

# %%
import pandas as pd
from IPython.display import display

# Keep things compact
pd.set_option("display.max_columns", 20)   # don't try to show hundreds
pd.set_option("display.max_colwidth", 80)  # clamp long cells to ~80 chars

# pick a small sample and flatten newlines so rows stay short
pdf = (silver_df.limit(10)
       .toPandas()
       .replace({r'[\r\n\t]+': ' '}, regex=True))

# simple, compact table with ellipsis in long cells
display(
    pdf.style
      .set_table_styles([
          {'selector': 'table', 'props': [('table-layout','fixed'), ('width','100%')]},
          {'selector': 'th, td', 'props': [
              ('max-width','280px'),
              ('white-space','nowrap'),
              ('overflow','hidden'),
              ('text-overflow','ellipsis')
          ]}
      ])
      .hide(axis='index')  # remove row numbers
)

# %%
silver_df.printSchema()

# %%
# Drop the 'price_text' column
silver_df = silver_df.drop('price_text')

# Rename 'price_value_mad' to 'price'
silver_df = silver_df.withColumnRenamed('price_value_mad', 'price')

# Replace NULL 'price' values with 0.0
silver_df = silver_df.fillna({'price': 0.0})

silver_df.select('price').show(10, truncate=False)

# %%
from pyspark.sql import functions as F

# Transform 'category' into 'offre' and 'category_type'
silver_df = (
    silver_df
    # Create 'offre' column for 'rent' or 'sale'
    .withColumn(
        "offre",
        F.when(F.col("category").contains("à louer"), "rent")
         .when(F.col("category").contains("à vendre"), "sale")
         .otherwise(None)
    )
    # Create 'category_type' column with only the property type (e.g., Maisons, Appartements)
    .withColumn(
        "type",
        F.when(
            F.col("category").contains("à louer") | F.col("category").contains("à vendre"),
            F.split(F.col("category"), ",")[0]
        ).otherwise(None)
    )
    # Drop the original 'category' column
    .drop("category")
)

# Check the result
silver_df.select("offre", "type").show(30, truncate=False)


# %%
from pyspark.sql import functions as F

# Split 'breadcrumbs' based on '>'
split_breadcrumbs = F.split(F.col("breadcrumbs"), " > ")

# Create new columns for each segment
silver_df = (
    silver_df
    .withColumn("city", split_breadcrumbs.getItem(2))  # e.g., "Casablanca"
    .withColumn("neighborhood", split_breadcrumbs.getItem(3))  # e.g., "Maarif"
    .withColumn("site", split_breadcrumbs.getItem(4))  # e.g., "Avito Immobilier"
    .drop("breadcrumbs")  # Drop the original column if not needed
)

# Check the result
silver_df.select(
     "city", "neighborhood", "site", 
).show(30, truncate=False)


# %%
from pyspark.sql import functions as F

# Change all values in the 'site' column to 'avito'
silver_df = silver_df.withColumn('site', F.lit('avito'))

# Show the updated data
silver_df.select('site').show(10, truncate=False)

# %%
# Group by 'seller_name' and 'seller_type', and count the occurrences
silver_df.groupBy('seller_type').count().orderBy('count', ascending=False).show(10, truncate=False)

# %%
from pyspark.sql import functions as F

# Convert 'seller_type' to lowercase
silver_df = silver_df.withColumn("seller_type", F.lower(F.col("seller_type")))

# Show the distinct values of 'seller_type' with counts, including NULLs
silver_df.groupBy('seller_type').count().orderBy('count', ascending=False).show(30, truncate=False)

# Show rows where 'seller_type' is NULL
silver_df.filter(F.col('seller_type').isNull()).show(5, truncate=False)

# %%
# Drop rows where 'seller_type' is NULL
silver_df = silver_df.filter(F.col('seller_type').isNotNull())

# Show the updated result to confirm the rows are dropped
silver_df.select('seller_type').distinct().show(10, truncate=False)

# %%
# Group by 'seller_name' and 'seller_type', and count the occurrences
silver_df.groupBy('seller_name').count().orderBy('count', ascending=False).show(5, truncate=False)

# %%
from pyspark.sql.functions import to_timestamp

# Convert 'published_date_text' to timestamp and rename it to 'published_date'
silver_df = silver_df.withColumn(
    "published_date", 
    to_timestamp("published_date_text", "yyyy-MM-dd HH:mm:ss")  # Adjust format if needed
).drop("published_date_text")  # Drop the original 'published_date_text' column

# Show the result
silver_df.select("id", "published_date").show(5, truncate=False)

# %%
from pyspark.sql.functions import col, udf
from pyspark.sql.types import ArrayType, StringType
import re

# Define a UDF to clean the 'equipments' list, handling NoneType
def clean_equipments(equipments):
    # Handle NoneType case
    if equipments is None:
        return []
    
    # Filter out unwanted entries (numbers and time-related strings)
    cleaned = [item for item in equipments if not re.match(r'^\d+$', str(item)) and not re.match(r'.*(mois|an).*', str(item))]
    return cleaned

# Register the UDF
clean_equipments_udf = udf(clean_equipments, ArrayType(StringType()))

# Apply the UDF to clean 'equipments' directly (in place)
silver_df = silver_df.withColumn("equipments", clean_equipments_udf(col("equipments")))

# Show the cleaned 'equipments' column
silver_df.select("equipments").show(5, truncate=False)

# %%
from pyspark.sql import functions as F

# Extract the keys from 'attributes_map'
keys = silver_df.select(F.explode(F.map_keys(F.col("attributes_map"))).alias("attribute")).distinct().rdd.flatMap(lambda x: x).collect()

# For each key, create a new column with its corresponding value from 'attributes_map'
for key in keys:
    silver_df = silver_df.withColumn(
        key, 
        F.when(F.col("attributes_map").getItem(key).isNotNull(), 
               F.col("attributes_map").getItem(key)).otherwise(None)
    )

# Drop the 'attributes_map' column after extracting the keys and values
silver_df = silver_df.drop("attributes_map")

# Show the result with the new columns and drop the 'attributes_map' column
silver_df.select("id", *keys).show(5, truncate=False)

# %%
import pandas as pd
from IPython.display import display

# Keep things compact
pd.set_option("display.max_columns", 20)   # don't try to show hundreds
pd.set_option("display.max_colwidth", 80)  # clamp long cells to ~80 chars

# pick a small sample and flatten newlines so rows stay short
pdf = (silver_df.limit(10)
       .toPandas()
       .replace({r'[\r\n\t]+': ' '}, regex=True))

# simple, compact table with ellipsis in long cells
display(
    pdf.style
      .set_table_styles([
          {'selector': 'table', 'props': [('table-layout','fixed'), ('width','100%')]},
          {'selector': 'th, td', 'props': [
              ('max-width','280px'),
              ('white-space','nowrap'),
              ('overflow','hidden'),
              ('text-overflow','ellipsis')
          ]}
      ])
      .hide(axis='index')  # remove row numbers
)

# %%
silver_df.printSchema()

# %%
# %%  Rename French columns to English (ASCII, snake_case) and sanitize all names

from unicodedata import normalize
import re

# 1) Explicit mapping for known French fields -> English
rename_map = {
    "offre": "offer",
    "type": "property_type",

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

# Apply the explicit renames only if the column exists
for old, new in rename_map.items():
    if old in silver_df.columns:
        silver_df = silver_df.withColumnRenamed(old, new)

# 2) Global sanitizer to ensure ASCII + snake_case and remove any lingering accents/spaces
def to_snake_ascii(name: str) -> str:
    # remove accents → ASCII
    ascii_name = normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    # lowercase
    ascii_name = ascii_name.lower()
    # replace non-alphanumeric with underscores
    ascii_name = re.sub(r"[^a-z0-9]+", "_", ascii_name)
    # collapse multiple underscores and trim edges
    ascii_name = re.sub(r"_+", "_", ascii_name).strip("_")
    return ascii_name

# Build a stable rename plan to avoid collisions
current_cols = silver_df.columns
sanitized = [to_snake_ascii(c) for c in current_cols]

# If any collisions after sanitization, make them unique by suffixing _2, _3, ...
seen = {}
unique_sanitized = []
for s in sanitized:
    if s not in seen:
        seen[s] = 1
        unique_sanitized.append(s)
    else:
        seen[s] += 1
        unique_sanitized.append(f"{s}_{seen[s]}")

# Apply sanitized names
for old, new in zip(current_cols, unique_sanitized):
    if old != new:
        silver_df = silver_df.withColumnRenamed(old, new)

# Quick check
silver_df.printSchema()

# %%




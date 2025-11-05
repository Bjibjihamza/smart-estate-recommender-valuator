# src/pipeline/load/silver_loader.py
from pyspark.sql import DataFrame, SparkSession

def write_to_silver(
    spark: SparkSession,
    df: DataFrame,
    catalog: str = "rest",
    table: str = "silver.avito",
    mode: str = "append",  # append | overwrite | dynamic_overwrite
):
    """
    DataFrameWriterV2 to Iceberg table.
    """
    fqn = f"{catalog}.{table}"

    writer = (
        df.writeTo(fqn)
          .option("check-nullability", "false")
          .option("check-ordering", "false")
    )

    mode = (mode or "append").lower()
    if mode == "append":
        writer.append()
    elif mode == "overwrite":
        writer.overwritePartitions()
    elif mode in ("dynamic_overwrite", "dynamic-overwrite"):
        writer.overwritePartitions()
    else:
        raise ValueError(f"Unsupported write mode: {mode}")

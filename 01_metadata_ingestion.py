"""Reusable entry notebook for the Metadata Ingestion Pipeline."""
from pyspark.sql import functions as F
from delta.tables import DeltaTable

def stage_path(config: dict, run_id: str) -> str:
    return config["staging_path"].format(run_id=run_id)

def apply_watermark(df, watermark: dict):
    """Apply the prior committed watermark; do not mutate state here."""
    if not watermark or not watermark.get("column"):
        return df
    return df.filter(F.col(watermark["column"]) > F.lit(watermark["value"]))

def write_delta(df, target_path: str, mode: str, keys: list[str]):
    """Append or idempotently merge a processed entity into Delta."""
    if mode == "merge" and DeltaTable.isDeltaTable(spark, target_path):
        target = DeltaTable.forPath(spark, target_path)
        predicate = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        target.alias("t").merge(df.alias("s"), predicate).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        df.write.format("delta").mode("append").save(target_path)

def run(config: dict, run_id: str):
    staged = spark.read.format(config.get("format", "csv")).option("header", True).load(stage_path(config, run_id))
    filtered = apply_watermark(staged, config.get("watermark"))
    enriched = filtered.withColumn("_run_id", F.lit(run_id)).withColumn("_ingested_at", F.current_timestamp())
    write_delta(enriched, config["target_path"], config["write_mode"], config.get("merge_keys", []))
    return enriched.count()

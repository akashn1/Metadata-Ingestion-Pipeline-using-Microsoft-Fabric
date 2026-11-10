"""Shared quality-rule evaluator for the Metadata Ingestion Pipeline."""
from functools import reduce
from pyspark.sql import functions as F

def evaluate_rules(df, rules: list[dict]):
    failures = []
    for rule in rules:
        col = F.col(rule["column"])
        if rule["type"] == "not_null": failures.append(col.isNull())
        elif rule["type"] == "accepted_values": failures.append(~col.isin(rule["values"]))
        elif rule["type"] == "unique":
            duplicates = df.groupBy(rule["column"]).count().filter("count > 1").select(rule["column"])
            failures.append(col.isin([r[0] for r in duplicates.collect()]))
    invalid = reduce(lambda a, b: a | b, failures, F.lit(False))
    return df.filter(~invalid), df.filter(invalid).withColumn("_reject_reason", F.lit("quality_rule_failed"))

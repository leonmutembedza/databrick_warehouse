# Databricks notebook source
# Parse the small BIDMC CSV sample into replay-safe Bronze Delta row tables.
# Run after download_bidmc_sample and ingest_files.

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window


dbutils.widgets.text("manifest_table", "thesis_biomed.bronze.file_manifest", "Bronze file manifest")
dbutils.widgets.text("run_id", "", "Task run ID; blank creates a unique ID")

manifest_table = dbutils.widgets.get("manifest_table").strip()
run_id = dbutils.widgets.get("run_id").strip() or f"load-bidmc-{uuid.uuid4().hex}"
catalog = manifest_table.split(".")[0]
pipeline_version = "0.2.0"
schema_version = "1.0.0"
rule_version = "0.1.0"
dataset = "bidmc"
dataset_version = "1.0.0"
expected_files = {
    "bidmc_01_Signals.csv": "signals",
    "bidmc_01_Numerics.csv": "numerics",
}
started_at = datetime.now(timezone.utc)

if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", manifest_table):
    raise ValueError("manifest_table must be a three-part catalog.schema.table identifier.")

manifest_rows = (
    spark.table(manifest_table)
    .where(
        (F.col("source_dataset") == dataset)
        & (F.col("source_version") == dataset_version)
        & F.col("source_record_key").isin(list(expected_files))
    )
    .select(
        "record_id",
        "source_record_key",
        "raw_uri",
        "file_checksum",
    )
    .collect()
)
manifest_by_name = {row.source_record_key: row.asDict() for row in manifest_rows}
missing_files = sorted(set(expected_files) - set(manifest_by_name))
if missing_files:
    raise RuntimeError(f"Expected files are missing from {manifest_table}: {missing_files}")


def normalize_column(name: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", name.strip()).strip("_").lower()
    if not normalized:
        raise ValueError(f"Cannot normalize empty CSV column name: {name!r}")
    return normalized


def make_rule_result(record_id: str, modality: str, rule_id: str, severity: str,
                     passed: bool, observed: object, threshold: str | None,
                     reason: str | None) -> dict:
    return {
        "run_id": run_id,
        "record_id": record_id,
        "modality": modality,
        "rule_id": rule_id,
        "rule_version": rule_version,
        "severity": severity,
        "passed": passed,
        "observed_value": str(observed),
        "threshold_value": threshold,
        "failure_reason": reason,
        "pipeline_version": pipeline_version,
        "evaluated_at": datetime.now(timezone.utc),
    }


def build_quality_results(filename: str, record_id: str, frame, row_count: int) -> list[dict]:
    columns = frame.columns
    column_set = set(columns)
    is_signals = expected_files[filename] == "signals"
    modality = "physiological_signals"
    required = {"time_s", "pleth", "resp"} if is_signals else {
        "time_s", "hr", "pulse", "resp", "spo2"
    }
    if is_signals and not (column_set & {"ii", "v", "ecg"}):
        required.add("ii_or_v_or_ecg")

    missing_columns = sorted(required - column_set)
    normalized = frame.withColumn("time_seconds", F.col("time_s").cast("double") if "time_s" in column_set else F.lit(None).cast("double"))
    time_errors = normalized.where(
        F.col("time_seconds").isNull() | F.isnan("time_seconds")
    ).count()

    value_columns = [name for name in columns if name != "time_s"]
    invalid_value_count = 0
    missing_value_count = 0
    for name in value_columns:
        raw_value = F.col(name)
        text_value = F.upper(F.trim(raw_value))
        numeric_value = raw_value.cast("double")
        is_missing = raw_value.isNull() | (F.trim(raw_value) == "") | text_value.isin("NAN", "NA", "N/A", "NULL")
        invalid = (~is_missing) & (numeric_value.isNull() | F.isnan(numeric_value) | (F.abs(numeric_value) == F.lit(float("inf"))))
        missing_value_count += normalized.where(is_missing).count()
        invalid_value_count += normalized.where(invalid).count()

    distinct_times = normalized.select("time_seconds").distinct().count()
    duplicate_times = max(0, row_count - distinct_times)

    results = [
        make_rule_result(
            record_id, modality, "required_columns_present", "FAIL",
            not missing_columns, json.dumps(columns), json.dumps(sorted(required)),
            f"Missing required CSV columns: {missing_columns}" if missing_columns else None,
        ),
        make_rule_result(
            record_id, modality, "rows_non_empty", "FAIL", row_count > 0,
            row_count, "> 0", "CSV contains no data rows" if row_count == 0 else None,
        ),
        make_rule_result(
            record_id, modality, "time_numeric_parseability", "FAIL", time_errors == 0,
            time_errors, "0 unparseable or missing time values",
            f"{time_errors} rows have missing or non-numeric time" if time_errors else None,
        ),
        make_rule_result(
            record_id, modality, "channel_numeric_parseability", "FAIL", invalid_value_count == 0,
            invalid_value_count, "0 invalid non-missing channel values",
            f"{invalid_value_count} channel values are not numeric" if invalid_value_count else None,
        ),
        make_rule_result(
            record_id, modality, "channel_missingness", "WARN", missing_value_count == 0,
            missing_value_count, "0 missing channel values (profile before setting threshold)",
            f"{missing_value_count} channel values are null/blank/NaN" if missing_value_count else None,
        ),
        make_rule_result(
            record_id, modality, "duplicate_time_values", "WARN", duplicate_times == 0,
            duplicate_times, "0 duplicate time values",
            f"{duplicate_times} rows share a timestamp" if duplicate_times else None,
        ),
    ]
    return results


def load_one_file(filename: str, manifest_row: dict) -> tuple[dict, list[dict], int, int]:
    kind = expected_files[filename]
    target_table = f"{catalog}.bronze.bidmc_{kind}"
    raw_frame = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .load(manifest_row["raw_uri"])
    )
    original_columns = raw_frame.columns
    normalized_columns = [normalize_column(name) for name in original_columns]
    if len(normalized_columns) != len(set(normalized_columns)):
        raise ValueError(f"CSV column names collide after normalization: {original_columns}")
    frame = raw_frame.toDF(*normalized_columns)
    source_rows = frame.count()
    quality_results = build_quality_results(filename, manifest_row["record_id"], frame, source_rows)

    time_column = F.col("time_s").cast("double") if "time_s" in normalized_columns else F.lit(None).cast("double")
    value_columns = [name for name in normalized_columns if name != "time_s"]
    if value_columns:
        map_entries = []
        for name in value_columns:
            map_entries.extend([F.lit(name), F.col(name)])
        source_values = F.create_map(*map_entries)
    else:
        source_values = F.create_map(F.lit("_no_channels"), F.lit(None).cast("string"))

    row_payload = F.to_json(F.struct(*[F.col(name) for name in normalized_columns]), {"ignoreNullFields": "false"})
    enriched = (
        frame
        .withColumn("source_time_seconds", time_column)
        .withColumn("source_values", source_values)
        .withColumn("source_row_hash", F.sha2(row_payload, 256))
        .withColumn(
            "source_row_number",
            F.row_number().over(
                Window.orderBy(
                    F.col("source_time_seconds").asc_nulls_first(),
                    F.col("source_row_hash").asc(),
                )
            ).cast("long"),
        )
        .select(
            F.lit(dataset).alias("source_dataset"),
            F.lit(dataset_version).alias("source_version"),
            F.lit(manifest_row["source_record_key"]).alias("source_record_key"),
            F.lit(filename).alias("source_file_name"),
            F.lit(manifest_row["record_id"]).alias("record_id"),
            F.lit(manifest_row["raw_uri"]).alias("raw_uri"),
            F.lit(manifest_row["file_checksum"]).alias("file_checksum"),
            "source_row_number",
            "source_time_seconds",
            "source_values",
            "source_row_hash",
            F.lit(run_id).alias("ingestion_run_id"),
            F.current_timestamp().alias("loaded_at"),
            F.lit(schema_version).alias("schema_version"),
            F.lit(pipeline_version).alias("pipeline_version"),
        )
    )

    if not spark.catalog.tableExists(target_table):
        enriched.limit(0).write.format("delta").saveAsTable(target_table)
    target = spark.table(target_table)
    identity = ["source_dataset", "source_version", "source_record_key", "source_row_number"]
    changed_rows = (
        enriched.alias("new")
        .join(target.alias("old"), on=identity, how="inner")
        .where(
            (F.col("new.file_checksum") != F.col("old.file_checksum"))
            | (F.col("new.source_row_hash") != F.col("old.source_row_hash"))
        )
    )
    if changed_rows.limit(1).count():
        raise RuntimeError(f"Changed content detected for existing {filename} rows in {target_table}.")

    exact_key = identity + ["file_checksum", "source_row_hash"]
    replay_count = enriched.alias("new").join(target.alias("old"), on=exact_key, how="inner").count()
    new_rows = enriched.alias("new").join(target.alias("old"), on=exact_key, how="left_anti")
    new_count = new_rows.count()
    if source_rows != replay_count + new_count:
        raise RuntimeError(
            f"Row reconciliation failed for {filename}: source={source_rows}, "
            f"replayed={replay_count}, new={new_count}."
        )

    view_name = f"bidmc_{kind}_incoming"
    enriched.createOrReplaceTempView(view_name)
    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING {view_name} AS source
        ON target.source_dataset = source.source_dataset
          AND target.source_version = source.source_version
          AND target.source_record_key = source.source_record_key
          AND target.source_row_number = source.source_row_number
          AND target.file_checksum = source.file_checksum
          AND target.source_row_hash = source.source_row_hash
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    return {
        "file_name": filename,
        "table": target_table,
        "source_rows": source_rows,
        "new_rows_inserted": new_count,
        "exact_replay_rows_skipped": replay_count,
        "dq_failed_rules": [result["rule_id"] for result in quality_results if not result["passed"] and result["severity"] == "FAIL"],
    }, quality_results, new_count, replay_count


file_summaries = []
all_quality_results = []
total_new_rows = 0
total_replay_rows = 0
for filename in expected_files:
    summary, quality_results, new_count, replay_count = load_one_file(filename, manifest_by_name[filename])
    file_summaries.append(summary)
    all_quality_results.extend(quality_results)
    total_new_rows += new_count
    total_replay_rows += replay_count

dq_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("record_id", StringType(), False),
    StructField("modality", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("rule_version", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("passed", BooleanType(), False),
    StructField("observed_value", StringType(), True),
    StructField("threshold_value", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("pipeline_version", StringType(), False),
    StructField("evaluated_at", TimestampType(), False),
])
dq_table = f"{catalog}.ops.dq_rule_results"
spark.createDataFrame(all_quality_results, schema=dq_schema).write.format("delta").mode("append").saveAsTable(dq_table)

hard_failures = [
    {"file": item["file_name"], "rules": item["dq_failed_rules"]}
    for item in file_summaries if item["dq_failed_rules"]
]
run_finished_at = datetime.now(timezone.utc)
run_summary = {
    "run_id": run_id,
    "files_processed": len(file_summaries),
    "rows_inserted": total_new_rows,
    "exact_replay_rows_skipped": total_replay_rows,
    "source_rows": total_new_rows + total_replay_rows,
    "file_results": file_summaries,
    "hard_dq_failures": hard_failures,
}

run_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("parent_job_run_id", StringType(), True),
    StructField("task_key", StringType(), True),
    StructField("source_dataset", StringType(), True),
    StructField("source_version", StringType(), True),
    StructField("run_date", DateType(), True),
    StructField("pipeline_version", StringType(), False),
    StructField("schema_version", StringType(), False),
    StructField("rule_version", StringType(), True),
    StructField("run_status", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("finished_at", TimestampType(), True),
    StructField("source_files_seen", LongType(), True),
    StructField("manifest_rows_inserted", LongType(), True),
    StructField("exact_replays_skipped", LongType(), True),
    StructField("changed_payload_conflicts", LongType(), True),
    StructField("accepted_rows", LongType(), True),
    StructField("quarantined_rows", LongType(), True),
    StructField("error_summary", StringType(), True),
    StructField("metrics_json", StringType(), True),
])
failed = bool(hard_failures)
audit_row = {
    "run_id": run_id,
    "parent_job_run_id": None,
    "task_key": "load_bidmc_bronze",
    "source_dataset": dataset,
    "source_version": dataset_version,
    "run_date": started_at.date(),
    "pipeline_version": pipeline_version,
    "schema_version": schema_version,
    "rule_version": rule_version,
    "run_status": "FAILED" if failed else "SUCCEEDED",
    "started_at": started_at,
    "finished_at": run_finished_at,
    "source_files_seen": len(file_summaries),
    "manifest_rows_inserted": 0,
    "exact_replays_skipped": total_replay_rows,
    "changed_payload_conflicts": 0,
    "accepted_rows": total_new_rows,
    "quarantined_rows": 0,
    "error_summary": json.dumps(hard_failures) if failed else None,
    "metrics_json": json.dumps(run_summary),
}
spark.createDataFrame([audit_row], schema=run_schema).write.format("delta").mode("append").saveAsTable(
    f"{catalog}.ops.pipeline_runs"
)

print(json.dumps(run_summary, indent=2))
if failed:
    raise RuntimeError(
        "Bronze CSV rows were loaded, but one or more hard data-quality rules failed. "
        f"Inspect {dq_table} for this run_id={run_id}: {json.dumps(hard_failures)}"
    )
dbutils.notebook.exit(json.dumps(run_summary))

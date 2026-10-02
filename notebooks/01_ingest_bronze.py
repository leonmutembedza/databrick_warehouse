# Databricks notebook source
# Batch file inventory for the BIDMC public release.
# Binary source files remain immutable in the Unity Catalog Volume. This task
# records their provenance and checksum in the Bronze Delta manifest.

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


dbutils.widgets.text(
    "raw_path",
    "/Volumes/<catalog>/<schema>/<volume>/landing/bidmc/1.0.0",
    "Raw BIDMC folder in Unity Catalog Volume",
)
dbutils.widgets.text("manifest_table", "<catalog>.<schema>.file_manifest", "Bronze Delta manifest table")
dbutils.widgets.text("run_id", "", "Workflow run ID; blank creates a manual run ID")
dbutils.widgets.text("file_limit", "5", "Maximum files for initial run; 0 processes all files")

raw_path = dbutils.widgets.get("raw_path").rstrip("/")
manifest_table = dbutils.widgets.get("manifest_table").strip()
run_id = dbutils.widgets.get("run_id").strip() or f"manual-{uuid.uuid4().hex}"
file_limit = int(dbutils.widgets.get("file_limit"))
pipeline_version = "0.1.0"
schema_version = "1.0.0"
dataset = "bidmc"
dataset_version = "1.0.0"
license_reference = "https://physionet.org/content/bidmc/1.0.0/"

if not raw_path.startswith("/Volumes/") or "<catalog>" in raw_path:
    raise ValueError("Set raw_path to the actual /Volumes/<catalog>/<schema>/<volume>/... location.")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", manifest_table):
    raise ValueError("manifest_table must be a three-part catalog.schema.table identifier.")
if file_limit < 0:
    raise ValueError("file_limit must be zero (all files) or a positive integer.")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def infer_modality(filename: str) -> str:
    lowered = filename.lower()
    if "breath" in lowered:
        return "respiration"
    # BIDMC signal files can contain PPG, ECG, and respiration channels.
    # Keep the multi-channel source file at family level until Silver splits it.
    return "physiological_signals"


root = Path(raw_path)
if not root.is_dir():
    raise FileNotFoundError(f"Volume landing folder does not exist: {raw_path}")

file_paths = sorted(
    path for path in root.rglob("*")
    if path.is_file() and path.suffix.lower() == ".csv"
)
if file_limit:
    file_paths = file_paths[:file_limit]
if not file_paths:
    raise RuntimeError(f"No files found under {raw_path}")

run_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
rows = []
for path in file_paths:
    relative_path = path.relative_to(root).as_posix()
    source_record_key = relative_path
    record_id = hashlib.sha256(
        f"{dataset}\x1f{dataset_version}\x1f{source_record_key}".encode("utf-8")
    ).hexdigest()[:32]
    rows.append(
        {
            "record_id": record_id,
            "subject_id_hash": None,
            "modality": infer_modality(path.name),
            "source_dataset": dataset,
            "source_version": dataset_version,
            "source_record_key": source_record_key,
            "event_timestamp": None,
            "time_precision": None,
            "raw_uri": str(path),
            "file_checksum": sha256_file(path),
            "file_format": path.suffix.lower().lstrip("."),
            "source_metadata": {
                "file_name": path.name,
                "relative_path": relative_path,
                "file_size_bytes": str(path.stat().st_size),
            },
            "ingestion_run_id": run_id,
            "ingested_at": run_timestamp,
            "schema_version": schema_version,
            "pipeline_version": pipeline_version,
            "license_reference": license_reference,
            "deidentification_status": "source_deidentified",
        }
    )

schema = StructType(
    [
        StructField("record_id", StringType(), False),
        StructField("subject_id_hash", StringType(), True),
        StructField("modality", StringType(), False),
        StructField("source_dataset", StringType(), False),
        StructField("source_version", StringType(), False),
        StructField("source_record_key", StringType(), False),
        StructField("event_timestamp", StringType(), True),
        StructField("time_precision", StringType(), True),
        StructField("raw_uri", StringType(), False),
        StructField("file_checksum", StringType(), False),
        StructField("file_format", StringType(), False),
        StructField("source_metadata", MapType(StringType(), StringType()), False),
        StructField("ingestion_run_id", StringType(), False),
        StructField("ingested_at", StringType(), False),
        StructField("schema_version", StringType(), False),
        StructField("pipeline_version", StringType(), False),
        StructField("license_reference", StringType(), True),
        StructField("deidentification_status", StringType(), False),
    ]
)
incoming = spark.createDataFrame(rows, schema=schema).dropDuplicates(
    ["source_dataset", "source_version", "source_record_key", "file_checksum"]
)

identity = ["source_dataset", "source_version", "source_record_key"]
exact_identity = identity + ["file_checksum"]
incoming_conflicts = (
    incoming.groupBy(*identity)
    .agg(F.countDistinct("file_checksum").alias("checksum_count"))
    .where(F.col("checksum_count") > 1)
)
if incoming_conflicts.limit(1).count():
    examples = [row.asDict() for row in incoming_conflicts.limit(10).collect()]
    raise RuntimeError(
        "The incoming batch contains the same source key with different checksums. "
        "Resolve the conflict before ingestion. Examples: " + json.dumps(examples)
    )

if not spark.catalog.tableExists(manifest_table):
    incoming.limit(0).write.format("delta").saveAsTable(manifest_table)

target = spark.table(manifest_table)
run_started_at = datetime.now(timezone.utc)

changed_payloads = (
    incoming.alias("new")
    .join(target.alias("old"), on=identity, how="inner")
    .where(F.lower(F.col("new.file_checksum")) != F.lower(F.col("old.file_checksum")))
)
changed_count = changed_payloads.count()
if changed_count:
    conflict_examples = [row.asDict() for row in changed_payloads.select(
        "new.source_record_key", "old.file_checksum", "new.file_checksum"
    ).limit(10).collect()]
    failed_summary = {
        "run_id": run_id,
        "dataset": dataset,
        "source_files_seen": len(rows),
        "manifest_rows_inserted": 0,
        "exact_replays_skipped": 0,
        "changed_payload_conflicts": changed_count,
        "manifest_table": manifest_table,
        "raw_path": raw_path,
    }
    audit_schema = StructType([
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
    failed_audit = {
        "run_id": run_id,
        "parent_job_run_id": None,
        "task_key": "ingest_bidmc_bronze",
        "source_dataset": dataset,
        "source_version": dataset_version,
        "run_date": run_started_at.date(),
        "pipeline_version": pipeline_version,
        "schema_version": schema_version,
        "rule_version": None,
        "run_status": "FAILED",
        "started_at": run_started_at,
        "finished_at": datetime.now(timezone.utc),
        "source_files_seen": len(rows),
        "manifest_rows_inserted": 0,
        "exact_replays_skipped": 0,
        "changed_payload_conflicts": changed_count,
        "accepted_rows": None,
        "quarantined_rows": None,
        "error_summary": "Changed payload detected for existing source key",
        "metrics_json": json.dumps(failed_summary),
    }
    spark.createDataFrame([failed_audit], schema=audit_schema).write.format("delta").mode("append").saveAsTable(
        ".".join(manifest_table.split(".")[:1] + ["ops", "pipeline_runs"])
    )
    raise RuntimeError(
        "BIDMC source keys already exist with different checksums. "
        "Resolve the changed source payload before merging. Examples: "
        + json.dumps(conflict_examples)
    )

exact_replays = incoming.alias("new").join(target.alias("old"), on=exact_identity, how="inner").count()
new_rows = incoming.alias("new").join(target.alias("old"), on=exact_identity, how="left_anti")
new_count = new_rows.count()
new_rows.createOrReplaceTempView("bidmc_manifest_incoming")

if new_count:
    spark.sql(
        f"""
        MERGE INTO {manifest_table} AS target
        USING bidmc_manifest_incoming AS source
        ON target.source_dataset = source.source_dataset
          AND target.source_version = source.source_version
          AND target.source_record_key = source.source_record_key
          AND target.file_checksum = source.file_checksum
        WHEN NOT MATCHED THEN INSERT *
        """
    )

summary = {
    "run_id": run_id,
    "dataset": dataset,
    "source_files_seen": len(rows),
    "manifest_rows_inserted": new_count,
    "exact_replays_skipped": exact_replays,
    "changed_payload_conflicts": changed_count,
    "manifest_table": manifest_table,
    "raw_path": raw_path,
}
audit_schema = StructType([
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
success_audit = {
    "run_id": run_id,
    "parent_job_run_id": None,
    "task_key": "ingest_bidmc_bronze",
    "source_dataset": dataset,
    "source_version": dataset_version,
    "run_date": run_started_at.date(),
    "pipeline_version": pipeline_version,
    "schema_version": schema_version,
    "rule_version": None,
    "run_status": "SUCCEEDED",
    "started_at": run_started_at,
    "finished_at": datetime.now(timezone.utc),
    "source_files_seen": len(rows),
    "manifest_rows_inserted": new_count,
    "exact_replays_skipped": exact_replays,
    "changed_payload_conflicts": 0,
    "accepted_rows": None,
    "quarantined_rows": None,
    "error_summary": None,
    "metrics_json": json.dumps(summary),
}
ops_table = ".".join(manifest_table.split(".")[:1] + ["ops", "pipeline_runs"])
spark.createDataFrame([success_audit], schema=audit_schema).write.format("delta").mode("append").saveAsTable(ops_table)
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
